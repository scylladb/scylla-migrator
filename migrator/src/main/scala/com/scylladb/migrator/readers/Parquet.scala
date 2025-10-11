package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.scylla.SourceDataFrame
import com.scylladb.migrator.scylla
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import scala.util.Using

case class ParquetReaderWithSavepoints(source: SourceSettings.Parquet,
                                       allFiles: Seq[String],
                                       skipFiles: Set[String]) {

  val filesToProcess: Seq[String] = allFiles.filterNot(skipFiles.contains)

  def configureHadoop(spark: SparkSession): Unit =
    Parquet.configureHadoopCredentials(spark, source)
}

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  def migrateToScylla(config: MigratorConfig,
                      source: SourceSettings.Parquet,
                      target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit = {
    val preparedReader = prepareParquetReader(
      spark,
      source,
      config.getSkipParquetFilesOrEmptySet
    )

    val savepointsManager = ParquetSavepointsManager(
      config,
      spark.sparkContext
    )

    Using.resource(savepointsManager) { manager =>
      preparedReader.configureHadoop(spark)

      val filesToProcess = preparedReader.filesToProcess
      val totalFiles = filesToProcess.size

      if (totalFiles == 0) {
        log.info(
          "No Parquet files to process. Migration may be complete or all files already processed.")
      } else {
        log.info(s"Starting Parquet migration: processing $totalFiles files")

        filesToProcess.zipWithIndex.foreach {
          case (filePath, index) =>
            val fileNum = index + 1
            log.info(s"Processing file $fileNum/$totalFiles: $filePath")

            val singleFileDF = spark.read.parquet(filePath)
            val sourceDF = scylla.SourceDataFrame(singleFileDF, None, false)

            scylla.ScyllaParquetMigrator.migrate(
              config,
              target,
              sourceDF,
              manager
            )

            manager.markFileAsProcessed(filePath)
            log.info(s"Successfully processed file $fileNum/$totalFiles: $filePath")
        }

        manager.dumpMigrationState("completed")

        log.info(s"Parquet migration completed successfully: $totalFiles files processed")
      }
    }
  }

  def prepareParquetReader(spark: SparkSession,
                           source: SourceSettings.Parquet,
                           skipFiles: Set[String] = Set.empty): ParquetReaderWithSavepoints = {

    configureHadoopCredentials(spark, source)

    val allFiles = listParquetFiles(spark, source.path)
    log.info(s"Found ${allFiles.size} Parquet files in ${source.path}")

    if (skipFiles.nonEmpty) {
      log.info(s"Skipping ${skipFiles.size} already processed files")
    }

    ParquetReaderWithSavepoints(source, allFiles, skipFiles)
  }

  @deprecated(
    "Use prepareParquetReader and process files individually for savepoints support. See migrateToScylla for file-by-file processing pattern.")
  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame =
    readDataFrame(spark, source, Set.empty)

  @deprecated(
    "Use prepareParquetReader and process files individually for savepoints support. See migrateToScylla function for file-by-file processing pattern.")
  def readDataFrame(spark: SparkSession,
                    source: SourceSettings.Parquet,
                    skipFiles: Set[String]): SourceDataFrame = {
    val preparedReader = prepareParquetReader(spark, source, skipFiles)
    preparedReader.configureHadoop(spark)

    val filesToRead = preparedReader.filesToProcess

    if (filesToRead.isEmpty) {
      log.warn("No files to process after filtering. Migration may be complete.")
      val samplePath =
        if (preparedReader.allFiles.nonEmpty) preparedReader.allFiles.head else source.path
      val emptyDf = spark.read.parquet(samplePath).limit(0)
      return SourceDataFrame(emptyDf, None, false)
    }

    log.info(s"Reading ${filesToRead.size} Parquet files")

    val df = if (filesToRead.size == 1) {
      spark.read.parquet(filesToRead.head)
    } else {
      val dataFrames = filesToRead.map(spark.read.parquet)
      dataFrames.reduce(_.union(_))
    }

    SourceDataFrame(df, None, false)
  }

  def listParquetFiles(spark: SparkSession, path: String): Seq[String] = {
    log.info(s"Discovering Parquet files in $path")

    try {
      val dataFrame = spark.read
        .option("recursiveFileLookup", "true")
        .parquet(path)

      val files = dataFrame.inputFiles.toSeq.distinct.sorted

      if (files.isEmpty) {
        throw new IllegalArgumentException(s"No Parquet files found in $path")
      }

      log.info(s"Found ${files.size} Parquet file(s)")
      files
    } catch {
      case e: AnalysisException =>
        val message = s"Failed to list Parquet files from $path: ${e.getMessage}"
        log.error(message)
        throw new IllegalArgumentException(message, e)
    }
  }

  private[readers] def configureHadoopCredentials(spark: SparkSession,
                                                  source: SourceSettings.Parquet): Unit =
    source.finalCredentials.foreach { credentials =>
      log.info("Loaded AWS credentials from config file")
      source.region.foreach { region =>
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", region)
      }
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.secretKey)
      credentials.maybeSessionToken.foreach { sessionToken =>
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.session.token",
          sessionToken
        )
      }
    }
}
