package com.scylladb.migrator.readers

import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.execution.datasources.PartitionMetadataExtractor

case class PartitionMetadata(
  partitionId: Int,
  filename: String
)

/** This reader uses Spark's internal partition information to build mappings between partition IDs
  * and file paths. This allows us to track when all partitions of a file have been processed,
  * enabling file-level savepoints.
  */
object PartitionMetadataReader {
  private val logger = LogManager.getLogger("com.scylladb.migrator.readers.PartitionMetadataReader")

  def readMetadata(spark: SparkSession, filePaths: Seq[String]): Seq[PartitionMetadata] = {
    logger.info(s"Reading partition metadata from ${filePaths.size} file(s)")
    val df = spark.read.parquet(filePaths: _*)
    readMetadataFromDataFrame(df)
  }

  def readMetadataFromDataFrame(df: DataFrame): Seq[PartitionMetadata] =
    try {
      logger.info("Extracting partition metadata from execution plan")

      val fileMap: Map[Int, Seq[String]] = PartitionMetadataExtractor.getPartitionFiles(df)

      val metadata = fileMap.flatMap { case (partId, files) =>
        files.map(f => PartitionMetadata(partId, f))
      }.toSeq

      logger.info(s"Discovered ${metadata.size} partition-to-file mappings")

      if (logger.isDebugEnabled) {
        val fileStats = metadata.groupBy(_.filename).view.mapValues(_.size)
        logger.debug(s"Files distribution: ${fileStats.size} unique files")
        fileStats.foreach { case (file, partCount) =>
          logger.debug(s"  File: $file -> $partCount partition(s)")
        }
      }

      metadata

    } catch {
      case e: Exception =>
        logger.error(s"Failed to read partition metadata", e)
        throw new RuntimeException(s"Could not read partition metadata: ${e.getMessage}", e)
    }

  def buildFileToPartitionsMap(metadata: Seq[PartitionMetadata]): Map[String, Set[Int]] = {
    val result = metadata
      .groupBy(_.filename)
      .view
      .mapValues(_.map(_.partitionId).toSet)
      .toMap

    logger.debug(s"Built file-to-partitions map with ${result.size} files")
    result
  }

  def buildPartitionToFileMap(metadata: Seq[PartitionMetadata]): Map[Int, Set[String]] = {
    val result = metadata
      .groupBy(_.partitionId)
      .view
      .mapValues(_.map(_.filename).toSet)
      .toMap

    logger.debug(s"Built partition-to-file map with ${result.size} partitions")
    result
  }
}
