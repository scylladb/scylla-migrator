package com.scylladb.migrator.readers

import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig
import com.scylladb.migrator.alternator.StringSetAccumulator
import org.apache.spark.SparkContext

class ParquetSavepointsManager(
  migratorConfig: MigratorConfig,
  filesAccumulator: StringSetAccumulator
) extends SavepointsManager(migratorConfig) {

  def describeMigrationState(): String = {
    val processedCount = filesAccumulator.value.size
    s"Processed files: $processedCount"
  }

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(skipParquetFiles = Some(filesAccumulator.value))

  def markFileAsProcessed(filePath: String): Unit = {
    filesAccumulator.add(filePath)
    log.debug(s"Marked file as processed: $filePath")
  }
}

object ParquetSavepointsManager {

  def apply(migratorConfig: MigratorConfig, spark: SparkContext): ParquetSavepointsManager = {
    val filesAccumulator =
      StringSetAccumulator(migratorConfig.skipParquetFiles.getOrElse(Set.empty))

    spark.register(filesAccumulator, "processed-parquet-files")

    new ParquetSavepointsManager(migratorConfig, filesAccumulator)
  }
}
