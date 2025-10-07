package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.apache.hadoop.mapreduce.lib.input.{ CombineFileSplit, FileInputFormat }
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Success, Try }

/**
  * Prepared reader for Parquet files with savepoints support.
  * This class separates file discovery from actual DataFrame reading to enable
  * InputSplit analysis in savepoints manager.
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
    * Configure Hadoop configuration for the source
    */
  def configureHadoop(spark: SparkSession): Unit =
    source.finalCredentials.foreach { credentials =>
      log.info("Loaded AWS credentials from config file")
      source.region.foreach { region =>
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", region)
      }
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.secretKey)
      // See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Using_Session_Credentials_with_TemporaryAWSCredentialsProvider
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

  /**
    * Read DataFrame from the prepared files
    */
  def readDataFrame(spark: SparkSession): SourceDataFrame = {
    configureHadoop(spark)

    log.info(s"Processing ${filesToProcess.size} Parquet files")

    if (filesToProcess.isEmpty) {
      log.warn("No files to process after filtering. Migration may be complete.")
      // Return empty DataFrame with proper schema by reading one file if available
      val samplePath = if (allFiles.nonEmpty) allFiles.head else source.path
      val emptyDf = spark.read.parquet(samplePath).limit(0)
      return SourceDataFrame(emptyDf, None, false)
    }

    // Process files using the parallelize approach similar to DynamoDB S3 Export
    val df = if (filesToProcess.size == 1) {
      // Single file optimization
      spark.read.parquet(filesToProcess.head)
    } else {
      // Multiple files - use union approach
      val dataFrames = filesToProcess.map(spark.read.parquet)
      dataFrames.reduce(_.union(_))
    }

    SourceDataFrame(df, None, false)
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
    * Use prepareParquetReader + readDataFrame for savepoints-enabled migrations.
    */
  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame =
    readDataFrame(spark, source, Set.empty)

  /**
    * Legacy method for backward compatibility.
    * Use prepareParquetReader + readDataFrame for savepoints-enabled migrations.
    */
  def readDataFrame(spark: SparkSession,
                    source: SourceSettings.Parquet,
                    skipFiles: Set[String]): SourceDataFrame = {
    val preparedReader = prepareParquetReader(spark, source, skipFiles)
    preparedReader.readDataFrame(spark)
  }

  /**
    * List all Parquet files in the given path (supports S3, HDFS, local filesystem)
    */
  def listParquetFiles(spark: SparkSession, path: String): Seq[String] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    Try {
      val pathObj = new Path(path)
      val fs = FileSystem.get(pathObj.toUri, hadoopConf)

      // Check if it's a single file or directory
      val fileStatus = fs.getFileStatus(pathObj)
      if (fileStatus.isFile) {
        // Single file case
        if (path.endsWith(".parquet")) {
          Seq(path)
        } else {
          log.warn(s"Single file $path doesn't have .parquet extension")
          Seq(path)
        }
      } else {
        // Directory case - recursively find all .parquet files
        val files = ListBuffer[String]()

        def collectParquetFiles(dir: Path): Unit = {
          val dirStatus = fs.listStatus(
            dir,
            new PathFilter {
              override def accept(path: Path): Boolean = {
                val name = path.getName
                !name.startsWith("_") && !name.startsWith(".") // Skip hidden/metadata files
              }
            }
          )

          dirStatus.foreach { status =>
            if (status.isDirectory) {
              collectParquetFiles(status.getPath)
            } else if (status.getPath.getName.endsWith(".parquet")) {
              files += status.getPath.toString
            }
          }
        }

        collectParquetFiles(pathObj)
        files.toSeq.sorted
      }
    } match {
      case Success(files) =>
        log.info(s"Listed ${files.size} Parquet files from $path")
        files
      case Failure(ex) =>
        log.error(s"Failed to list files from $path: ${ex.getMessage}")
        // Fallback to simple path approach
        Seq(path)
    }
  }

}
