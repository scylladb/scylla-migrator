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

/** A per-element collection-timestamp write pass. Each carries the singleton/grouped
  * collection-append rows for one non-frozen collection column, written after the base row write
  * with `col = col + ?` and per-row TTL/WRITETIME so element-level timestamps are preserved.
  *
  * @param columnName
  *   the (source) collection column name; renames are applied by the writer.
  * @param rdd
  *   rows of `[primary key columns..., singleton/grouped collection value, ttl, writetime,
  *   overwrite]`.
  * @param schema
  *   positional schema for `rdd`, ending in `ttl` (IntegerType), `writetime` (LongType) and
  *   [[CollectionAppendWrite.OverwriteColumn]] (BooleanType).
  */
case class CollectionAppendWrite(
  columnName: String,
  rdd: RDD[Row],
  schema: StructType
)

object CollectionAppendWrite {

  /** Marks the one row per collection cell that is written with `col = ?` (overwrite) rather than
    * `col = col + ?`, so its collection tombstone clears target elements older than the source's.
    */
  val OverwriteColumn = "__migrator_overwrite"
}

/** @param cassandraExplodedWrite
  *   When set, [[migrate]] writes this `RDD[Row]` with [[writers.Scylla.writeRowRDD]] instead of
  *   [[writers.Scylla.writeDataframe]], so exploded rows can carry
  *   [[com.datastax.spark.connector.types.CassandraOption]] on regular columns (explicit CQL null
  *   vs unset). [[dataFrame]] stays the pre-explosion frame (e.g. wide Cassandra read) for
  *   partition metadata and logging.
  * @param batches
  *   When set, [[migrate]] writes these batches one after another instead of this frame's own
  *   passes, and [[dataFrame]] only describes the whole input (partition count, token-range
  *   coverage). Used for per-element collection preservation, whose multi-pass writes cache their
  *   input: batching bounds that cache to one batch.
  */
case class SourceDataFrame(
  dataFrame: DataFrame,
  timestampColumns: Option[TimestampColumns],
  savepointsSupported: Boolean,
  cassandraExplodedWrite: Option[(RDD[Row], StructType)] = None,
  collectionAppendWrites: Seq[CollectionAppendWrite] = Nil,
  batches: Option[SourceBatches] = None
)

/** One bounded part of a batched multi-pass migration.
  *
  * @param source
  *   the batch's own frame and write passes; its cache is released once they finish
  * @param description
  *   what the batch covers, for logging
  * @param recordProgress
  *   records the batch as done in the run's savepoints manager; called only after every one of its
  *   write passes has succeeded
  */
final case class SourceBatch(
  source: SourceDataFrame,
  description: String,
  recordProgress: SavepointsManager => Unit
)

/** The batches of a batched multi-pass migration, built lazily one at a time so that only the batch
  * being written is read and cached.
  */
final case class SourceBatches(count: Int, iterator: () => Iterator[SourceBatch])

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
      sourceDF.batches match {
        case None          => writePasses(migratorConfig, target, sourceDF, tokenRangeAccumulator)
        case Some(batches) =>
          // Each batch is read, cached, written by every pass and released before the next one,
          // so the cache holds one batch rather than the whole input. Its write tasks see pruned
          // partitions the connector cannot extract token ranges from, so no accumulator rides
          // the passes; the batch records its own progress once all of them have succeeded.
          log.info(s"Writing the source in ${batches.count} batch(es)")
          batches.iterator().zipWithIndex.foreach { case (batch, index) =>
            val label = s"batch ${index + 1}/${batches.count}"
            log.info(s"Writing $label: ${batch.description}")
            try writePasses(migratorConfig, target, batch.source, None)
            finally releaseCache(batch.source)
            maybeSavepointsManager.foreach { manager =>
              batch.recordProgress(manager)
              manager.dumpMigrationState(label)
            }
          }
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        val savepointMessage =
          if (maybeSavepointsManager.isDefined) "Will attempt to create a savepoint before exiting"
          else "No savepoint manager is available; exiting without a savepoint"
        log.error(
          s"Caught error while writing the DataFrame. $savepointMessage",
          e
        )
        caughtError = Some(e)
    } finally {
      // Release the source frame cache (see F5 in readers.Cassandra.readDataframe). All write
      // passes are eager actions that have run by now, so nothing still reads it. No-op when the
      // frame was never persisted (e.g. single-pass or non-Cassandra sources).
      releaseCache(sourceDF)
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

  /** Release a source frame's cache (see F5 in readers.Cassandra.readDataframe). Its write passes
    * are eager actions that have run by now, so nothing still reads it. No-op when the frame was
    * never persisted (e.g. single-pass or non-Cassandra sources). Failures are logged, not raised.
    */
  private def releaseCache(source: SourceDataFrame): Unit =
    try source.dataFrame.unpersist()
    catch {
      case NonFatal(e) =>
        log.warn("Failed to release the cached source DataFrame; continuing", e)
    }

  /** The base write and the per-element collection-append passes of one source frame. */
  private def writePasses(
    migratorConfig: MigratorConfig,
    target: TargetSettings.Scylla,
    sourceDF: SourceDataFrame,
    tokenRangeAccumulator: Option[TokenRangeAccumulator]
  )(implicit spark: SparkSession): Unit = {
    // Savepoint correctness for multi-pass collection preservation: a range is only complete once
    // the base write AND every append pass have written it, so only the final pass may feed the
    // token-range accumulator. Readers write per-element collections in batches (see
    // `SourceDataFrame.batches`), which pass no accumulator and record each batch only after all
    // of its passes have succeeded; recording from the base write instead would mark ranges done
    // BEFORE the append passes run, and a resume would then skip their collection elements.
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
    }
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
      val manager = CqlSavepointsManager(
        migratorConfig,
        tokenRangeAccumulator,
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
