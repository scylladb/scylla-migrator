package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{
  MigratorConfig,
  ParquetProcessingMode,
  SourceSettings,
  TargetSettings
}
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
    val processingMode = config.savepoints.getParquetProcessingMode

    val strategy: ParquetProcessingStrategy = processingMode match {
      case ParquetProcessingMode.Parallel =>
        log.info("Selected PARALLEL processing mode (default)")
        new ParallelParquetStrategy()
      case ParquetProcessingMode.Sequential =>
        log.info("Selected SEQUENTIAL processing mode (with savepoints)")
        new SequentialParquetStrategy()
    }

    strategy.migrate(config, source, target)
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
