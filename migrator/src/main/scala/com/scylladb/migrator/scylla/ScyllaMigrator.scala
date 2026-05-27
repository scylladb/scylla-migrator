package com.scylladb.migrator.scylla

import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
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
case class SourceDataFrame(
  dataFrame: DataFrame,
  timestampColumns: Option[TimestampColumns],
  savepointsSupported: Boolean,
  cassandraExplodedWrite: Option[(RDD[Row], StructType)] = None
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
      sourceDF.cassandraExplodedWrite match {
        case Some((explodedRdd, writeSchema)) =>
          writers.Scylla.writeRowRDD(
            target,
            migratorConfig.getRenamesOrNil,
            explodedRdd,
            writeSchema,
            sourceDF.timestampColumns,
            tokenRangeAccumulator,
            migratorConfig.source
          )
        case None =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.getRenamesOrNil,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            tokenRangeAccumulator,
            migratorConfig.source
          )
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e
        )
        caughtError = Some(e)
    } finally
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
      val manager = CqlSavepointsManager(migratorConfig, tokenRangeAccumulator)
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
      CqlParquetSavepointsManager(migratorConfig, sourceDF, spark.sparkContext)
    ) { savepointsManager =>
      try
        writers.Parquet.writeDataframe(target, dfForParquet)
      catch {
        case NonFatal(e) =>
          log.error(
            "Caught error while writing Parquet. Will create a savepoint before exiting",
            e
          )
      } finally
        savepointsManager.dumpMigrationState("final")
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
