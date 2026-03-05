package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings }
import org.apache.spark.scheduler.{ SparkListenerTaskEnd, TaskInfo }
import org.apache.spark.{ Success, TaskEndReason }
import org.apache.spark.sql.SparkSession
import java.nio.file.Files

class FileCompletionListenerTest extends munit.FunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("FileCompletionListenerTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  def createMockTaskEnd(
    partitionId: Int,
    stageId: Int = 0,
    success: Boolean = true
  ): SparkListenerTaskEnd = {
    val taskInfo = new TaskInfo(
      taskId        = partitionId.toLong,
      index         = partitionId,
      attemptNumber = 0,
      partitionId   = partitionId,
      launchTime    = System.currentTimeMillis(),
      executorId    = "executor-1",
      host          = "localhost",
      taskLocality  = org.apache.spark.scheduler.TaskLocality.PROCESS_LOCAL,
      speculative   = false
    )

    val reason: TaskEndReason = if (success) Success else org.apache.spark.TaskKilled("test")

    new SparkListenerTaskEnd(
      stageId             = stageId,
      stageAttemptId      = 0,
      taskType            = "ResultTask",
      reason              = reason,
      taskInfo            = taskInfo,
      taskExecutorMetrics = null,
      taskMetrics         = null
    )
  }

  test("FileCompletionListener tracks single-partition files") {
    val tempDir = Files.createTempDirectory("savepoints-listener-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        // Setup: 3 files, each with 1 partition
        val partitionToFile = Map(
          0 -> Set("file1.parquet"),
          1 -> Set("file2.parquet"),
          2 -> Set("file3.parquet")
        )

        val fileToPartitions = Map(
          "file1.parquet" -> Set(0),
          "file2.parquet" -> Set(1),
          "file3.parquet" -> Set(2)
        )

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        assertEquals(listener.getCompletedFilesCount, 0)
        assertEquals(listener.getTotalFilesCount, 3)

        listener.onTaskEnd(createMockTaskEnd(0))

        assertEquals(listener.getCompletedFilesCount, 1)

        listener.onTaskEnd(createMockTaskEnd(1))

        assertEquals(listener.getCompletedFilesCount, 2)

        listener.onTaskEnd(createMockTaskEnd(2))

        assertEquals(listener.getCompletedFilesCount, 3)

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("FileCompletionListener tracks multi-partition files") {
    val tempDir = Files.createTempDirectory("savepoints-listener-multipart-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val partitionToFile = Map(
          0 -> Set("file1.parquet"),
          1 -> Set("file1.parquet"),
          2 -> Set("file1.parquet"),
          3 -> Set("file2.parquet"),
          4 -> Set("file2.parquet")
        )

        val fileToPartitions = Map(
          "file1.parquet" -> Set(0, 1, 2),
          "file2.parquet" -> Set(3, 4)
        )

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        assertEquals(listener.getCompletedFilesCount, 0)

        listener.onTaskEnd(createMockTaskEnd(0))
        assertEquals(listener.getCompletedFilesCount, 0, "File1 should not be complete yet")

        listener.onTaskEnd(createMockTaskEnd(1))
        assertEquals(listener.getCompletedFilesCount, 0, "File1 should still not be complete")

        listener.onTaskEnd(createMockTaskEnd(2))
        assertEquals(listener.getCompletedFilesCount, 1, "File1 should be complete now")

        listener.onTaskEnd(createMockTaskEnd(3))
        assertEquals(listener.getCompletedFilesCount, 1, "File2 should not be complete yet")

        listener.onTaskEnd(createMockTaskEnd(4))
        assertEquals(listener.getCompletedFilesCount, 2, "Both files should be complete")

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("FileCompletionListener handles failed tasks correctly") {
    val tempDir = Files.createTempDirectory("savepoints-listener-failure-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val partitionToFile = Map(
          0 -> Set("file1.parquet"),
          1 -> Set("file2.parquet")
        )

        val fileToPartitions = Map(
          "file1.parquet" -> Set(0),
          "file2.parquet" -> Set(1)
        )

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        listener.onTaskEnd(createMockTaskEnd(0, success = false))

        assertEquals(listener.getCompletedFilesCount, 0)

        listener.onTaskEnd(createMockTaskEnd(0, success = true))

        assertEquals(listener.getCompletedFilesCount, 1)

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("FileCompletionListener is idempotent for duplicate task completions") {
    val tempDir = Files.createTempDirectory("savepoints-listener-idempotent-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val partitionToFile = Map(0 -> Set("file1.parquet"))
        val fileToPartitions = Map("file1.parquet" -> Set(0))

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        listener.onTaskEnd(createMockTaskEnd(0))
        listener.onTaskEnd(createMockTaskEnd(0))
        listener.onTaskEnd(createMockTaskEnd(0))

        assertEquals(listener.getCompletedFilesCount, 1)

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("FileCompletionListener provides accurate progress report") {
    val tempDir = Files.createTempDirectory("savepoints-listener-report-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val partitionToFile = Map(
          0 -> Set("file1.parquet"),
          1 -> Set("file1.parquet"),
          2 -> Set("file2.parquet")
        )

        val fileToPartitions = Map(
          "file1.parquet" -> Set(0, 1),
          "file2.parquet" -> Set(2)
        )

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        val initialReport = listener.getProgressReport
        assert(initialReport.contains("0/2 files"))

        listener.onTaskEnd(createMockTaskEnd(0))
        val midReport = listener.getProgressReport
        assert(midReport.contains("0/2 files"), "File not complete until all partitions done")

        listener.onTaskEnd(createMockTaskEnd(1))
        val file1CompleteReport = listener.getProgressReport
        assert(file1CompleteReport.contains("1/2 files"))

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("FileCompletionListener handles multiple files per partition") {
    val tempDir = Files.createTempDirectory("savepoints-listener-multi-file-partition-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val partitionToFile = Map(0 -> Set("file1.parquet", "file2.parquet"))
        val fileToPartitions = Map(
          "file1.parquet" -> Set(0),
          "file2.parquet" -> Set(0)
        )

        val listener = new FileCompletionListener(
          partitionToFile,
          fileToPartitions,
          manager
        )

        listener.onTaskEnd(createMockTaskEnd(0))

        assertEquals(listener.getCompletedFilesCount, 2)
      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }
}
