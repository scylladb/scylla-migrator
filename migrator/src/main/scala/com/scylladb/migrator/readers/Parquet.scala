package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Prepared reader for Parquet files with savepoints support.
  * This class separates file discovery from file processing.
  * Files are processed one-by-one in Migrator.scala to enable granular savepoints.
  */
case class ParquetReaderWithSavepoints(source: SourceSettings.Parquet,
                                       allFiles: Seq[String],
                                       skipFiles: Set[String]) {

  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  /**
    * Files that should be processed (filtered by skipFiles)
    */
  val filesToProcess: Seq[String] = allFiles.filterNot(skipFiles.contains)

  /**
    * Configure Hadoop configuration for the source (called once before processing files)
    */
  def configureHadoop(spark: SparkSession): Unit =
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

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  /**
    * Prepare a Parquet reader for savepoints-enabled migration.
    * This method discovers all files and prepares the reader, but doesn't read data yet.
    * Use this method when you need to create savepoints manager before reading data.
    */
  def prepareParquetReader(spark: SparkSession,
                           source: SourceSettings.Parquet,
                           skipFiles: Set[String] = Set.empty): ParquetReaderWithSavepoints = {
    // Get all Parquet files from the source path
    val allFiles = listParquetFiles(spark, source.path)
    log.info(s"Found ${allFiles.size} Parquet files in ${source.path}")

    if (skipFiles.nonEmpty) {
      log.info(s"Skipping ${skipFiles.size} already processed files")
    }

    ParquetReaderWithSavepoints(source, allFiles, skipFiles)
  }

  /**
    * Legacy method for backward compatibility.
    * Reads all files at once (non-savepoints mode).
    * For savepoints-enabled migrations, use prepareParquetReader and process files individually.
    */
  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame =
    readDataFrame(spark, source, Set.empty)

  /**
    * Legacy method for backward compatibility.
    * Reads all files at once (non-savepoints mode).
    * For savepoints-enabled migrations, use prepareParquetReader and process files individually.
    */
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

  /**
    * List all Parquet files by leveraging Spark's built-in file discovery.
    *
    * Uses DataFrame.inputFiles which returns the actual files Spark will read.
    * This is simpler and more reliable than manual filesystem traversal.
    */
  def listParquetFiles(spark: SparkSession, path: String): Seq[String] = {
    log.info(s"Discovering Parquet files in $path")

    // Let Spark discover files (handles _SUCCESS, _metadata, etc. automatically)
    // Read only schema, not data
    val files = spark.read.parquet(path).limit(0).inputFiles.toSeq.sorted

    if (files.isEmpty) {
      throw new IllegalArgumentException(s"No Parquet files found in $path")
    }

    log.info(s"Found ${files.size} Parquet file(s)")
    files
  }

}
