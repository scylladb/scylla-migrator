// IMPORTANT: Must be in this package to access Spark internal API
package org.apache.spark.sql.execution.datasources

import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec

/**
  * Extracts partition-to-file mappings from the Spark execution plan.
  *
  * This uses Spark API to access the FileScanRDD, which already contains
  * the partition-to-file mapping computed during query planning.
  */
object PartitionMetadataExtractor {
  private val logger =
    LogManager.getLogger("org.apache.spark.sql.execution.datasources.PartitionMetadataExtractor")

  def getPartitionFiles(df: DataFrame): Map[Int, Seq[String]] = {
    logger.debug("Extracting partition-to-file mapping from execution plan")

    val plan = df.queryExecution.executedPlan

    val scanExecList = plan.collect {
      case exec: FileSourceScanExec => exec
    }

    val scanExec = scanExecList match {
      case list if list.size == 1 =>
        list.head
      case list if list.size > 1 =>
        val message = "Several FileSourceScanExec were found in plan"
        logger.error(s"$message. Plan: ${plan.treeString}")
        throw new IllegalArgumentException(message)
      case list if list.isEmpty =>
        val message = "DataFrame is not based on file source (FileSourceScanExec not found in plan)"
        logger.error(s"$message. Plan: ${plan.treeString}")
        throw new IllegalArgumentException(message)
    }

    val rdd = scanExec.inputRDD

    val partitionFiles = rdd.partitions.map {
      case p: FilePartition =>
        val filePaths = p.files.map(_.filePath.toString).toSeq
        (p.index, filePaths)
    }.toMap

    logger.debug(
      s"Extracted ${partitionFiles.size} partition mappings covering ${partitionFiles.values.flatten.toSet.size} unique files")

    partitionFiles
  }
}
