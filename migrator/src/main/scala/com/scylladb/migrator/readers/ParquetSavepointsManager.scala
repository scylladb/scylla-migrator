package com.scylladb.migrator.readers

import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig
import org.apache.spark.SparkContext
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
  * which files have been processed. Files are processed sequentially (per-file processing),
  * and each file is marked as processed immediately after successful migration.
  *
  * Usage:
  * - Created before processing files (in Migrator.scala)
  * - Passed to ScyllaParquetMigrator
  * - markFileAsProcessed() called after each file is successfully migrated
  * - Automatically closed via scala.util.Using.resource
  */
class ParquetSavepointsManager(migratorConfig: MigratorConfig,
                               filesAccumulator: StringSetAccumulator)
    extends SavepointsManager(migratorConfig) {

  def describeMigrationState(): String = {
    val processedCount = filesAccumulator.value.size
    s"Processed files: $processedCount"
  }

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(skipParquetFiles = Some(filesAccumulator.value))

  /**
    * Mark a file as processed. Called immediately after successful migration of the file.
    */
  def markFileAsProcessed(filePath: String): Unit = {
    filesAccumulator.add(filePath)
    log.debug(s"Marked file as processed: $filePath")
  }
}

object ParquetSavepointsManager {

  /**
    * Set up a savepoints manager that tracks Parquet files processed.
    *
    * Files are processed sequentially (per-file processing), and each file is marked
    * as processed via markFileAsProcessed() immediately after successful migration.
    * The manager must be used with scala.util.Using.resource for proper cleanup.
    */
  def apply(migratorConfig: MigratorConfig, spark: SparkContext): ParquetSavepointsManager = {
    val filesAccumulator =
      StringSetAccumulator(migratorConfig.skipParquetFiles.getOrElse(Set.empty))

    spark.register(filesAccumulator, "processed-parquet-files")

    new ParquetSavepointsManager(migratorConfig, filesAccumulator)
  }
}
