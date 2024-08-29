package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import com.scylladb.migrator.writers
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.util.control.NonFatal

case class SourceDataFrame(dataFrame: DataFrame,
                           timestampColumns: Option[TimestampColumns],
                           savepointsSupported: Boolean)

object ScyllaMigrator {
  val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  def migrate(migratorConfig: MigratorConfig,
              target: TargetSettings.Scylla,
              sourceDF: SourceDataFrame)(implicit spark: SparkSession): Unit = {

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    val maybeSavepointsManager =
      if (!sourceDF.savepointsSupported) None
      else {
        val tokenRangeAccumulator = TokenRangeAccumulator.empty
        spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

        Some(CqlSavepointsManager(migratorConfig, tokenRangeAccumulator))
      }

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
      writers.Scylla.writeDataframe(
        target,
        migratorConfig.getRenamesOrNil,
        sourceDF.dataFrame,
        sourceDF.timestampColumns,
        maybeSavepointsManager.map(_.accumulator))
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      for (savePointsManger <- maybeSavepointsManager) {
        savePointsManger.dumpMigrationState("final")
        savePointsManger.close()
      }
    }
  }

}
