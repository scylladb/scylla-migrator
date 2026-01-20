package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.scylla.{ ScyllaParquetMigrator, SourceDataFrame }
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.Using

class SequentialParquetStrategy extends ParquetProcessingStrategy {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.SequentialParquetStrategy")

  override def migrate(config: MigratorConfig,
                       source: SourceSettings.Parquet,
                       target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit = {
    log.info("Using SEQUENTIAL processing mode (with savepoints, file-by-file)")

    val preparedReader = Parquet.prepareParquetReader(
      spark,
      source,
      config.getSkipParquetFilesOrEmptySet
    )

    Using.resource(
      ParquetSavepointsManager(
        config,
        spark.sparkContext
      )) { manager =>
      preparedReader.configureHadoop(spark)

      val filesToProcess = preparedReader.filesToProcess
      val totalFiles = filesToProcess.size

      if (totalFiles == 0) {
        log.info(
          "No Parquet files to process. Migration may be complete or all files already processed.")
      } else {
        log.info(s"Starting sequential Parquet migration: processing $totalFiles files")

        filesToProcess.zipWithIndex.foreach {
          case (filePath, index) =>
            val fileNum = index + 1
            log.info(s"Processing file $fileNum/$totalFiles: $filePath")

            val singleFileDF = spark.read.parquet(filePath)
            val sourceDF = SourceDataFrame(singleFileDF, None, savepointsSupported = false)

            ScyllaParquetMigrator.migrate(
              config,
              target,
              sourceDF,
              manager
            )

            manager.markFileAsProcessed(filePath)
            log.info(s"Successfully processed file $fileNum/$totalFiles: $filePath")
        }

        manager.dumpMigrationState("completed")

        log.info(
          s"Sequential Parquet migration completed successfully: $totalFiles files processed")
      }
    }
  }
}
