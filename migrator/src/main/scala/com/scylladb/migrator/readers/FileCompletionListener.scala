package com.scylladb.migrator.readers

import org.apache.log4j.LogManager
import org.apache.spark.scheduler.{ SparkListener, SparkListenerTaskEnd }
import org.apache.spark.Success

import scala.collection.concurrent.TrieMap

/**
  * SparkListener that tracks partition completion and aggregates it to file-level completion.
  *
  * This listener monitors Spark task completion events and maintains mappings between
  * partitions and files. When all partitions belonging to a file have been successfully
  * completed, it marks the file as processed via the ParquetSavepointsManager.
  *
  * @param partitionToFile Mapping from Spark partition ID to source file paths
  * @param fileToPartitions Mapping from file path to the set of partition IDs reading from it
  * @param savepointsManager Manager to notify when files are completed
  */
class FileCompletionListener(
  partitionToFiles: Map[Int, Set[String]],
  fileToPartitions: Map[String, Set[Int]],
  savepointsManager: ParquetSavepointsManager
) extends SparkListener {

  private val log = LogManager.getLogger("com.scylladb.migrator.readers.FileCompletionListener")

  private val completedPartitions = TrieMap.empty[Int, Boolean]

  private val completedFiles = TrieMap.empty[String, Boolean]

  log.info(
    s"FileCompletionListener initialized: tracking ${fileToPartitions.size} files " +
      s"across ${partitionToFiles.size} partitions")

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
    if (taskEnd.reason == Success) {
      val partitionId = taskEnd.taskInfo.partitionId

      partitionToFiles.get(partitionId) match {
        case Some(filenames) =>
          if (completedPartitions.putIfAbsent(partitionId, true).isEmpty) {
            filenames.foreach { filename =>
              log.debug(s"Partition $partitionId completed (file: $filename)")
              checkFileCompletion(filename)
            }
          }

        case None =>
          log.trace(s"Task completed for untracked partition $partitionId")
      }
    } else {
      log.debug(
        s"Task for partition ${taskEnd.taskInfo.partitionId} did not complete successfully: ${taskEnd.reason}")
    }

  private def checkFileCompletion(filename: String): Unit = {
    if (completedFiles.contains(filename)) {
      return
    }

    fileToPartitions.get(filename) match {
      case Some(allPartitions) =>
        val allComplete = allPartitions.forall(completedPartitions.contains)

        if (allComplete) {
          if (completedFiles.putIfAbsent(filename, true).isEmpty) {
            savepointsManager.markFileAsProcessed(filename)

            val progress = s"${completedFiles.size}/${fileToPartitions.size}"
            log.info(s"File completed: $filename (progress: $progress)")
          }
        } else {
          val completedCount = allPartitions.count(completedPartitions.contains)
          log.trace(s"File $filename: $completedCount/${allPartitions.size} partitions complete")
        }

      case None =>
        log.warn(s"File $filename not found in fileToPartitions map (this shouldn't happen)")
    }
  }

  def getCompletedFilesCount: Int = completedFiles.size

  def getTotalFilesCount: Int = fileToPartitions.size

  def getProgressReport: String = {
    val filesCompleted = getCompletedFilesCount
    val totalFiles = getTotalFilesCount

    s"Progress: $filesCompleted/$totalFiles files"
  }
}
