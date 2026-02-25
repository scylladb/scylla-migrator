package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.scylla.{ ScyllaMigrator, ScyllaParquetMigrator, SourceDataFrame }
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import scala.util.Using

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  def migrateToScylla(config: MigratorConfig,
                      source: SourceSettings.Parquet,
                      target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit = {

    val useFileTracking = config.savepoints.enableParquetFileTracking

    if (useFileTracking) {
      log.info("Starting Parquet migration with file-level savepoint tracking")
      migrateWithSavepoints(config, source, target)
    } else {
      log.info("Starting Parquet migration without savepoint tracking")
      migrateWithoutSavepoints(config, source, target)
    }
  }

  /**
    * Parquet migration with file-level savepoint tracking.
    *
    * This mode tracks completion of individual Parquet files, enabling resume capability
    * if the migration is interrupted. Uses SparkListener to detect when all partitions
    * of a file have been processed.
    */
  private def migrateWithSavepoints(
    config: MigratorConfig,
    source: SourceSettings.Parquet,
    target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit = {
    configureHadoopCredentials(spark, source)

    val allFiles = listParquetFiles(spark, source.path)
    val skipFiles = config.getSkipParquetFilesOrEmptySet
    val filesToProcess = allFiles.filterNot(skipFiles.contains)

    if (filesToProcess.isEmpty) {
      log.info("No Parquet files to process. Migration is complete.")
      return
    }

    log.info(s"Processing ${filesToProcess.size} Parquet files")

    val df = if (skipFiles.isEmpty) {
      spark.read.parquet(source.path)
    } else {
      spark.read.parquet(filesToProcess: _*)
    }

    log.info("Reading partition metadata for file tracking...")
    val metadata = PartitionMetadataReader.readMetadataFromDataFrame(df)

    val partitionToFiles = PartitionMetadataReader.buildPartitionToFileMap(metadata)
    val fileToPartitions = PartitionMetadataReader.buildFileToPartitionsMap(metadata)

    log.info(
      s"Discovered ${fileToPartitions.size} files with ${metadata.size} total partitions to process")

    Using.resource(ParquetSavepointsManager(config, spark.sparkContext)) { savepointsManager =>
      val listener = new FileCompletionListener(
        partitionToFiles,
        fileToPartitions,
        savepointsManager
      )
      spark.sparkContext.addSparkListener(listener)

      try {
        val sourceDF = SourceDataFrame(df, None, savepointsSupported = false)

        log.info("Created DataFrame from Parquet source")

        ScyllaParquetMigrator.migrate(config, target, sourceDF, savepointsManager)

        savepointsManager.dumpMigrationState("completed")

        log.info(
          s"Parquet migration completed successfully: " +
            s"${listener.getCompletedFilesCount}/${listener.getTotalFilesCount} files processed")

      } finally {
        spark.sparkContext.removeSparkListener(listener)
        log.info(s"Final progress: ${listener.getProgressReport}")
      }
    }
  }

  /**
    * Parquet migration without savepoint tracking.
    *
    * This mode reads all Parquet files using Spark's native parallelism but does not
    * track individual file completion. If migration is interrupted, it will restart
    * from the beginning.
    */
  private def migrateWithoutSavepoints(
    config: MigratorConfig,
    source: SourceSettings.Parquet,
    target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit = {
    val sourceDF = ParquetWithoutSavepoints.readDataFrame(spark, source)
    ScyllaMigrator.migrate(config, target, sourceDF)
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

  /**
    * Configures Hadoop S3A credentials for reading from AWS S3.
    *
    * This method sets the necessary Hadoop configuration properties for AWS access key, secret key,
    * and optionally a session token. When a session token is present, it sets the credentials provider
    * to TemporaryAWSCredentialsProvider as required by Hadoop.
    *
    * If a region is specified in the source configuration, this method also sets the S3A endpoint region
    * via the `fs.s3a.endpoint.region` property.
    *
    * For more details, see the official Hadoop AWS documentation:
    * https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication
    */
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
