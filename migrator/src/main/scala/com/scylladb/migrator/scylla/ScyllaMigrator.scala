package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers.{ ParquetSavepointsManager, TimestampColumns }
import com.scylladb.migrator.writers
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.util.control.NonFatal

case class SourceDataFrame(dataFrame: DataFrame,
                           timestampColumns: Option[TimestampColumns],
                           savepointsSupported: Boolean)

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
    sourceDF.dataFrame.printSchema()

    val maybeSavepointsManager = externalSavepointsManager.orElse(
      createSavepointsManager(migratorConfig, sourceDF)
    )

    log.info(
      "We need to transfer: " + sourceDF.dataFrame.rdd.getNumPartitions + " partitions in total")

    if (migratorConfig.source.isInstanceOf[SourceSettings.Cassandra]) {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => {
        p.asInstanceOf[CassandraPartition[_, _]]
      })
      val allTokenRangesBuilder = Set.newBuilder[(Token[_], Token[_])]
      cassandraPartitions.foreach(p => {
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach(tr => {
            val range: (Token[_], Token[_]) =
              (tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]])
            allTokenRangesBuilder += range
          })

      })
      val allTokenRanges = allTokenRangesBuilder.result()

      log.info("All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (migratorConfig.skipTokenRanges.isDefined) {
        log.info(
          "Savepoints array defined, size of the array: " + migratorConfig.skipTokenRanges.size)

        val diff = allTokenRanges.diff(migratorConfig.getSkipTokenRangesOrEmptySet)
        log.info("Diff ... total diff of full ranges to savepoints is: " + diff.size)
        log.debug("Dump of the missing tokens: ")
        log.debug(diff)
      }
    }

    log.info("Starting write...")

    try {
      val tokenRangeAccumulator = maybeSavepointsManager.flatMap {
        case cqlManager: CqlSavepointsManager => Some(cqlManager.accumulator)
        case _                                => None
      }
      writers.Scylla.writeDataframe(
        target,
        migratorConfig.getRenamesOrNil,
        sourceDF.dataFrame,
        sourceDF.timestampColumns,
        tokenRangeAccumulator)
    } catch {
      // Catch all non-fatal exceptions to ensure that a savepoint is created
      // even in the event of unexpected errors during the write process.
      case NonFatal(e) =>
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      for (savePointsManger <- maybeSavepointsManager) {
        savePointsManger.dumpMigrationState("final")
        if (shouldCloseManager(savePointsManger)) {
          savePointsManger.close()
        }
      }
    }
  }
}

object ScyllaMigrator extends ScyllaMigratorBase {

  protected override def createSavepointsManager(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame
  )(implicit spark: SparkSession): Option[SavepointsManager] =
    if (!sourceDF.savepointsSupported) None
    else {
      val tokenRangeAccumulator = TokenRangeAccumulator.empty
      spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")
      Some(CqlSavepointsManager(migratorConfig, tokenRangeAccumulator))
    }

  protected override def shouldCloseManager(manager: SavepointsManager): Boolean = true
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
