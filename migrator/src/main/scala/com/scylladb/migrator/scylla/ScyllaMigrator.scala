package com.scylladb.migrator.scylla

import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.{
  MigratorConfig,
  SourceSettings,
  SparkSecretRedaction,
  TargetSettings
}
import com.scylladb.migrator.readers.{ ParquetSavepointsManager, TimestampColumns }
import com.scylladb.migrator.{ readers, writers }
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types.StructType

import scala.util.Using
import scala.util.control.NonFatal

/** @param cassandraExplodedWrite
  *   When set, [[migrate]] writes this `RDD[Row]` with [[writers.Scylla.writeRowRDD]] instead of
  *   [[writers.Scylla.writeDataframe]], so exploded rows can carry
  *   [[com.datastax.spark.connector.types.CassandraOption]] on regular columns (explicit CQL null
  *   vs unset). [[dataFrame]] stays the pre-explosion frame (e.g. wide Cassandra read) for
  *   partition metadata and logging.
  */
/** A per-element collection-timestamp write pass. Each carries the singleton/grouped
  * collection-append rows for one non-frozen collection column, written after the base row write
  * with `col = col + ?` and per-row TTL/WRITETIME so element-level timestamps are preserved.
  *
  * @param columnName
  *   the (source) collection column name; renames are applied by the writer.
  * @param rdd
  *   rows of `[primary key columns..., singleton/grouped collection value, ttl, writetime]`.
  * @param schema
  *   positional schema for `rdd`, ending in `ttl` (IntegerType) and `writetime` (LongType).
  */
case class CollectionAppendWrite(
  columnName: String,
  rdd: RDD[Row],
  schema: StructType
)

case class SourceDataFrame(
  dataFrame: DataFrame,
  timestampColumns: Option[TimestampColumns],
  savepointsSupported: Boolean,
  cassandraExplodedWrite: Option[(RDD[Row], StructType)] = None,
  collectionAppendWrites: Seq[CollectionAppendWrite] = Nil
)

trait ScyllaMigratorBase {
  protected val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  protected def externalSavepointsManager: Option[SavepointsManager] = None

  protected def createSavepointsManager(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Option[SavepointsManager]

  protected def shouldCloseManager(manager: SavepointsManager): Boolean

  def migrate(
    migratorConfig: MigratorConfig,
    target: TargetSettings.Scylla,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Unit = {

    log.info("Created source dataframe; resulting schema:")
    sourceDF.cassandraExplodedWrite match {
      case Some((_, writeSchema)) =>
        log.info("Exploded write path (cells may include CassandraOption); logical write schema:")
        writeSchema.printTreeString()
      case None =>
        sourceDF.dataFrame.printSchema()
    }

    val maybeSavepointsManager = externalSavepointsManager.orElse(
      createSavepointsManager(migratorConfig, sourceDF)
    )

    val partitionCount = sourceDF.cassandraExplodedWrite match {
      case Some((rdd, _)) => rdd.getNumPartitions
      case None           => sourceDF.dataFrame.rdd.getNumPartitions
    }
    log.info(s"We need to transfer: $partitionCount partitions in total")

    // Cassandra-specific token-range diff logging lives inside `CqlSavepointsManager` (invoked
    // from `savepointsManagerForSource`) so this central `migrate` method has zero
    // `isInstanceOf[SourceSettings.*]` checks. New source types do not need to be aware of, or
    // edit, this method.

    log.info("Starting write...")

    var caughtError: Option[Throwable] = None
    try {
      val tokenRangeAccumulator = maybeSavepointsManager.flatMap {
        case cqlManager: CqlSavepointsManager => Some(cqlManager.accumulator)
        case _                                => None
      }
      // Savepoint correctness for multi-pass collection preservation (Approach 2): when there are
      // collection-append passes, the base write and all but the FINAL append pass must not feed
      // the token-range accumulator. Passes run sequentially, so a range is only truly complete
      // after the final append pass finishes it — attaching the accumulator solely to that pass
      // makes a recorded "range done" mean "base + every append committed for that range". Ranges
      // reprocessed on resume are idempotent (INSERT/append with USING TIMESTAMP), so partial
      // ranges converge. With no append passes, the base write keeps the accumulator as before.
      //
      // M4 (accepted trade-off, not a correctness bug): the final append pass's RDD only contains
      // rows whose last collection column is non-null, so token ranges in which every row has a
      // null/empty last collection are NOT recorded even though they were fully written. On resume
      // those ranges are reprocessed. This only ever OVER-processes (idempotent), never skips, so
      // there is no data loss. The tempting alternative — recording ranges from the base write,
      // which visits every range — would mark ranges done BEFORE the append passes run and thus
      // reintroduce the C1/F1 data-loss-on-resume bug this design exists to prevent. Recording all
      // ranges accurately AND only after every pass commits would need a dedicated final sweep pass
      // over all partition keys; deferred as an efficiency-only improvement.
      val hasCollectionAppends = sourceDF.collectionAppendWrites.nonEmpty
      val baseAccumulator = if (hasCollectionAppends) None else tokenRangeAccumulator

      sourceDF.cassandraExplodedWrite match {
        case Some((explodedRdd, writeSchema)) =>
          writers.Scylla.writeRowRDD(
            target,
            migratorConfig.getRenamesOrNil,
            explodedRdd,
            writeSchema,
            sourceDF.timestampColumns,
            baseAccumulator,
            migratorConfig.source
          )
        case None =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.getRenamesOrNil,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            baseAccumulator,
            migratorConfig.source
          )
      }

      // Per-element collection timestamp preservation: after the base row write (scalars + frozen
      // collections), replay each non-frozen collection column with `col = col + ?` and per-row
      // TTL/WRITETIME. Only the last pass carries the token-range accumulator (see above).
      if (hasCollectionAppends) {
        log.info(
          s"Applying ${sourceDF.collectionAppendWrites.size} per-element collection-append " +
            s"pass(es): ${sourceDF.collectionAppendWrites.map(_.columnName).mkString(", ")}"
        )
        val lastIndex = sourceDF.collectionAppendWrites.size - 1
        sourceDF.collectionAppendWrites.zipWithIndex.foreach { case (caw, index) =>
          val accumulatorForPass =
            if (index == lastIndex) tokenRangeAccumulator else None
          writers.Scylla.writeCollectionAppendRDD(
            target,
            migratorConfig.getRenamesOrNil,
            caw.columnName,
            caw.rdd,
            caw.schema,
            accumulatorForPass,
            migratorConfig.source
          )
        }

        // C8/M4 visibility: the accumulator rides only the final append pass, which emits no rows
        // for ranges whose last collection column is null/empty. If it recorded nothing, resume
        // will reprocess the whole input (safe & idempotent, but with no savepoint speedup) — say
        // so rather than let operators be surprised by a full re-run.
        tokenRangeAccumulator.foreach { acc =>
          val recorded = acc.value.get.size
          val lastColumn = sourceDF.collectionAppendWrites.last.columnName
          if (recorded == 0)
            log.warn(
              "Collection-append savepoint pass recorded no token ranges (e.g. the last collection " +
                s"column '$lastColumn' was null/empty for every row). A resume will reprocess the " +
                "whole input: safe and idempotent, but without savepoint speedup."
            )
          else
            // Report the count, not just the all-empty case: ranges are recorded only where the
            // final append pass emitted rows, so a SPARSELY populated last collection column yields
            // proportionally few savepoints and a resume re-does most of the table. Operators need
            // the number to judge that, since it is safe-but-slow rather than incorrect.
            log.info(
              s"Collection-append savepoint pass recorded $recorded token range(s). Ranges are " +
                s"recorded only where the final collection column ('$lastColumn') was non-empty, so " +
                "a sparsely populated collection yields few savepoints and a resume will reprocess " +
                "most of the input (safe and idempotent, but slow)."
            )
        }
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e
        )
        caughtError = Some(e)
    } finally {
      // Release the source frame cache (see F5 in readers.Cassandra.readDataframe). All write
      // passes are eager actions that have run by now, so nothing still reads it. No-op when the
      // frame was never persisted (e.g. single-pass or non-Cassandra sources).
      try sourceDF.dataFrame.unpersist()
      catch { case NonFatal(_) => () }
      for (savePointsManger <- maybeSavepointsManager) {
        try
          savePointsManger.dumpMigrationState("final")
        catch {
          case NonFatal(finallyEx) =>
            caughtError.foreach(_.addSuppressed(finallyEx))
            if (caughtError.isEmpty) caughtError = Some(finallyEx)
        }
        if (shouldCloseManager(savePointsManger)) {
          try
            savePointsManger.close()
          catch {
            case NonFatal(closeEx) =>
              caughtError.foreach(_.addSuppressed(closeEx))
              if (caughtError.isEmpty) caughtError = Some(closeEx)
          }
        }
      }
    }
    caughtError.foreach(throw _)
  }
}

object ScyllaMigrator extends ScyllaMigratorBase {

  private[migrator] def savepointsManagerForSource(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Option[SavepointsManager] =
    if (!sourceDF.savepointsSupported) None
    else {
      val tokenRangeAccumulator = TokenRangeAccumulator.empty
      spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")
      val manager = new CqlSavepointsManager(
        migratorConfig,
        tokenRangeAccumulator,
        Some(spark.sparkContext.hadoopConfiguration),
        SparkSecretRedaction.redactionRegex(spark)
      )
      // Cassandra-only diagnostic: was previously emitted from `ScyllaMigratorBase.migrate`
      // gated by `isInstanceOf[SourceSettings.Cassandra]`. Moved here so the cast lives
      // alongside the CQL-specific manager that owns the partition shape. Non-Cassandra
      // DataFrame sources never construct this manager and never invoke this log call.
      manager.logTokenRangeCoverage(sourceDF, migratorConfig.skipTokenRanges)
      Some(manager)
    }

  protected override def createSavepointsManager(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Option[SavepointsManager] =
    savepointsManagerForSource(migratorConfig, sourceDF)

  protected override def shouldCloseManager(manager: SavepointsManager): Boolean = true

  def migrateToParquet(
    source: SourceSettings.Cassandra,
    target: TargetSettings.Parquet,
    migratorConfig: MigratorConfig
  )(implicit spark: SparkSession): Unit = {
    // Per-element collection TTL/WRITETIME are exported as array sidecars
    // (`__migrator_meta_<col>_ttl/_writetime`) and re-hydrated into collection-append passes on
    // the Parquet restore path (see `explodeRowsFromPerColumnMetaCollectionAware`).
    val sourceDF = readers.Cassandra.readDataframe(
      spark,
      source,
      source.preserveTimestamps,
      migratorConfig.getSkipTokenRangesOrEmptySet,
      skipExplosion = true
    )
    val dfForParquet =
      if (sourceDF.timestampColumns.isDefined)
        TimestampColumns.renameForParquet(sourceDF.dataFrame)
      else
        sourceDF.dataFrame
    Using.resource(
      CqlParquetSavepointsManager(
        migratorConfig,
        sourceDF,
        spark.sparkContext,
        SparkSecretRedaction.redactionRegex(spark)
      )
    ) { savepointsManager =>
      var caughtError: Option[Throwable] = None
      try
        writers.Parquet.writeDataframe(target, dfForParquet)
      catch {
        case NonFatal(e) =>
          log.error(
            "Caught error while writing Parquet. Will create a savepoint before exiting",
            e
          )
          caughtError = Some(e)
      } finally
        try savepointsManager.dumpMigrationState("final")
        catch {
          case NonFatal(finallyEx) =>
            caughtError.foreach(_.addSuppressed(finallyEx))
            if (caughtError.isEmpty) caughtError = Some(finallyEx)
        }
      // Re-throw so the process exits non-zero on a failed/partial Parquet export (including the
      // per-element collection array sidecars). Mirrors `ScyllaMigratorBase.migrate`; without this
      // a truncated export would be silently restored as complete on the Parquet import path.
      caughtError.foreach(throw _)
    }
  }
}

class ScyllaParquetMigrator(savepointsManager: ParquetSavepointsManager)
    extends ScyllaMigratorBase {

  protected override def externalSavepointsManager: Option[SavepointsManager] = {
    log.info("Using external Parquet savepoints manager")
    Some(savepointsManager)
  }

  protected override def createSavepointsManager(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Option[SavepointsManager] = None

  protected override def shouldCloseManager(manager: SavepointsManager): Boolean = false
}

object ScyllaParquetMigrator {
  def migrate(
    migratorConfig: MigratorConfig,
    target: TargetSettings.Scylla,
    sourceDF: SourceDataFrame,
    savepointsManager: ParquetSavepointsManager
  )(implicit spark: SparkSession): Unit =
    new ScyllaParquetMigrator(savepointsManager).migrate(migratorConfig, target, sourceDF)
}
