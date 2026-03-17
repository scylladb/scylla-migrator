package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig
import org.apache.logging.log4j.LogManager
import org.apache.spark.scheduler.{ SparkListener, SparkListenerTaskEnd }
import org.apache.spark.{ SparkContext, Success => TaskEndSuccess }

/** Manage CQL-to-Parquet migrations by tracking the migrated token ranges.
  *
  * Since `df.write.parquet()` does not use the DataStax connector's `saveToCassandra()`, the
  * built-in `TokenRangeAccumulator` is never populated. Instead, we use a SparkListener to
  * intercept task completions and extract source partition metadata (token ranges from
  * `CassandraPartition`).
  */
class CqlParquetSavepointsManager(
  migratorConfig: MigratorConfig,
  cqlTokenRangeAccumulator: CqlTokenRangeAccumulator,
  sparkTaskEndListener: SparkListener,
  spark: SparkContext
) extends SavepointsManager(migratorConfig) {

  def describeMigrationState(): String =
    s"Token ranges accumulated: ${cqlTokenRangeAccumulator.value.size}"

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(skipTokenRanges = Some(cqlTokenRangeAccumulator.value))

  override def close(): Unit = {
    spark.removeSparkListener(sparkTaskEndListener)
    super.close()
  }

}

object CqlParquetSavepointsManager {

  private val log = LogManager.getLogger(classOf[CqlParquetSavepointsManager])

  /** Set up a savepoints manager that tracks the token ranges migrated from the source DataFrame.
    */
  def apply(
    migratorConfig: MigratorConfig,
    sourceDF: SourceDataFrame,
    spark: SparkContext
  ): CqlParquetSavepointsManager = {
    val cqlTokenRangeAccumulator =
      CqlTokenRangeAccumulator(migratorConfig.getSkipTokenRangesOrEmptySet)
    spark.register(cqlTokenRangeAccumulator, "CQL token ranges copied to Parquet")
    val sourcePartitions = sourceDF.dataFrame.rdd.partitions
    val sparkTaskEndListener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val partitionId = taskEnd.taskInfo.partitionId
        log.debug(s"Migration of partition ${partitionId} ended: ${taskEnd.reason}.")
        if (taskEnd.reason == TaskEndSuccess) {
          if (partitionId >= 0 && partitionId < sourcePartitions.length) {
            val partition = sourcePartitions(partitionId)
            try {
              val cassandraPartition = partition.asInstanceOf[CassandraPartition[_, _]]
              val tokenRanges = cassandraPartition.tokenRanges
                .asInstanceOf[Vector[CqlTokenRange[_, _]]]
                .map { tr =>
                  (tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]])
                }
                .toSet
              cqlTokenRangeAccumulator.add(tokenRanges)
              log.info(
                s"Marked ${tokenRanges.size} token ranges from partition ${partitionId} as migrated."
              )
            } catch {
              case e: ClassCastException =>
                log.error(
                  s"Unable to extract token ranges from partition ${partitionId}. The next savepoint will not include them.",
                  e
                )
            }
          } else {
            log.warn(
              s"Partition ID ${partitionId} is out of range [0, ${sourcePartitions.length}). Ignoring."
            )
          }
        }
      }
    }
    spark.addSparkListener(sparkTaskEndListener)
    new CqlParquetSavepointsManager(
      migratorConfig,
      cqlTokenRangeAccumulator,
      sparkTaskEndListener,
      spark
    )
  }

}
