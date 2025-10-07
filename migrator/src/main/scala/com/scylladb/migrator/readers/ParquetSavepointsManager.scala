package com.scylladb.migrator.readers

import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig
import org.apache.log4j.LogManager
import org.apache.spark.scheduler.{ SparkListener, SparkListenerTaskEnd }
import org.apache.spark.{ SparkContext, Success => TaskEndSuccess }
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * An accumulator that tracks processed files.
  */
class StringSetAccumulator(initialValue: Set[String] = Set.empty)
    extends AccumulatorV2[String, Set[String]] {
  private val _set: mutable.Set[String] = mutable.Set(initialValue.toSeq: _*)

  def isZero: Boolean = _set.isEmpty
  def copy(): StringSetAccumulator = new StringSetAccumulator(_set.toSet)
  def reset(): Unit = _set.clear()
  def add(v: String): Unit = _set += v
  def merge(other: AccumulatorV2[String, Set[String]]): Unit = other match {
    case o: StringSetAccumulator => _set ++= o._set
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }
  def value: Set[String] = _set.toSet
}

object StringSetAccumulator {
  def apply(initialValue: Set[String] = Set.empty): StringSetAccumulator =
    new StringSetAccumulator(initialValue)
}

/**
  * Manage Parquet-based migrations by tracking the processed files.
  *
  * This implementation provides savepoints functionality for Parquet migrations by tracking
  * which files have been processed. Unlike CqlSavepointsManager which uses InputSplit analysis,
  * this manager requires explicit calls to markAllFilesAsProcessed() upon successful completion
  * due to the complexity of mapping Spark partitions to specific Parquet files.
  *
  * Usage:
  * - Created before reading data (in Migrator.scala)
  * - Passed to ScyllaMigrator as external savepoints manager
  * - markAllFilesAsProcessed() called after successful migration
  * - Automatically closed via scala.util.Using.resource
  */
class ParquetSavepointsManager(migratorConfig: MigratorConfig,
                               filesAccumulator: StringSetAccumulator,
                               sparkTaskEndListener: SparkListener,
                               spark: SparkContext)
    extends SavepointsManager(migratorConfig) {

  def describeMigrationState(): String = {
    val processedCount = filesAccumulator.value.size
    s"Processed files: $processedCount (tracked via accumulator)"
  }

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(skipParquetFiles = Some(filesAccumulator.value))

  /**
    * Mark a file as processed. This should be called when a file processing is completed.
    */
  def markFileAsProcessed(filePath: String): Unit = {
    filesAccumulator.add(filePath)
    log.debug(s"Marked file as processed in savepoint: ${filePath}")
  }

  /**
    * Mark all remaining files as processed. This is useful when the migration completes successfully.
    */
  def markAllFilesAsProcessed(allFiles: Seq[String]): Unit = {
    val remainingFiles = allFiles.toSet -- filesAccumulator.value
    remainingFiles.foreach(filesAccumulator.add)
    if (remainingFiles.nonEmpty) {
      log.info(s"Marked ${remainingFiles.size} remaining files as processed")
    }
  }

  override def close(): Unit = {
    spark.removeSparkListener(sparkTaskEndListener)
    super.close()
  }
}

object ParquetSavepointsManager {

  val log = LogManager.getLogger(classOf[ParquetSavepointsManager])

  /**
    * Set up a savepoints manager that tracks Parquet files processed.
    *
    * Note: This implementation uses manual file marking (via markAllFilesAsProcessed)
    * rather than automatic InputSplit analysis due to complexities in mapping Spark
    * partitions to specific Parquet files. The manager must be used with
    * scala.util.Using.resource for proper cleanup.
    */
  def apply(migratorConfig: MigratorConfig, spark: SparkContext): ParquetSavepointsManager = {
    val filesAccumulator =
      StringSetAccumulator(migratorConfig.skipParquetFiles.getOrElse(Set.empty))

    // Register the accumulator
    spark.register(filesAccumulator, "processed-parquet-files")

    val sparkTaskEndListener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val partitionId = taskEnd.taskInfo.partitionId
        log.debug(s"Migration of partition ${partitionId} ended: ${taskEnd.reason}.")
        if (taskEnd.reason != TaskEndSuccess) {
          log.debug(
            s"Task ${partitionId} failed - files for this partition not marked as processed")
        }
        // Note: We don't automatically add files here due to complexity of mapping
        // partitions to specific Parquet files. Files should be marked manually via markFileAsProcessed.
      }
    }

    spark.addSparkListener(sparkTaskEndListener)
    new ParquetSavepointsManager(migratorConfig, filesAccumulator, sparkTaskEndListener, spark)
  }
}
