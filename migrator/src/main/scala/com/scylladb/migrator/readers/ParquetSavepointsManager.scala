package com.scylladb.migrator.readers

import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.{ MigratorConfig, SparkSecretRedaction }
import com.scylladb.migrator.alternator.StringSetAccumulator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

class ParquetSavepointsManager(
  migratorConfig: MigratorConfig,
  filesAccumulator: StringSetAccumulator,
  hadoopConfiguration: Option[Configuration] = None,
  redactionRegex: Option[String] = None
) extends SavepointsManager(migratorConfig, hadoopConfiguration, redactionRegex) {

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

  def apply(
    migratorConfig: MigratorConfig,
    spark: SparkContext,
    redactionRegex: Option[String] = None
  ): ParquetSavepointsManager = {
    val filesAccumulator =
      StringSetAccumulator(migratorConfig.skipParquetFiles.getOrElse(Set.empty))

    spark.register(filesAccumulator, "processed-parquet-files")

    new ParquetSavepointsManager(
      migratorConfig,
      filesAccumulator,
      Some(spark.hadoopConfiguration),
      redactionRegex.orElse(SparkSecretRedaction.redactionRegex(spark))
    )
  }
}
