package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings }
import com.scylladb.migrator.readers.{ Parquet, ParquetSavepointsManager }
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

class ParquetSavepointsTest extends munit.FunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ParquetSavepointsTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("Parquet file listing with single file") {
    val tempDir = Files.createTempDirectory("parquet-test")
    val tempFile = tempDir.resolve("test.parquet")

    try {
      import spark.implicits._
      val testData = Seq(("1", "test")).toDF("id", "name")
      testData.write.parquet(tempFile.toString)

      val files = Parquet.listParquetFiles(spark, tempFile.toString)
      assert(files.size >= 1)
      assert(files.head.contains("part-") && files.head.endsWith(".parquet"))

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("Parquet file listing with directory") {
    val tempDir = Files.createTempDirectory("parquet-dir-test")

    try {
      import spark.implicits._
      val testData1 = Seq(("1", "test1")).toDF("id", "name")
      val testData2 = Seq(("2", "test2")).toDF("id", "name")

      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")

      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)

      val files = Parquet.listParquetFiles(spark, tempDir.toString)
      assert(files.size >= 2)
      files.foreach(file => assert(file.contains("part-") && file.endsWith(".parquet")))

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("ParquetSavepointsManager initialization and state") {
    val tempDir = Files.createTempDirectory("savepoints-test")

    try {
      val config = MigratorConfig(
        source           = SourceSettings.Parquet("dummy", None, None, None),
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = Some(Set("file1.parquet")),
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        val initialState = manager.describeMigrationState()
        assert(initialState.contains("Processed files: 1"))

        val updatedConfig = manager.updateConfigWithMigrationState()
        assertEquals(updatedConfig.skipParquetFiles.get, Set("file1.parquet"))

      } finally
        manager.close()

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("Parquet file filtering with skipFiles") {
    val tempDir = Files.createTempDirectory("parquet-skip-test")

    try {
      import spark.implicits._

      val testData1 = Seq(("1", "data1")).toDF("id", "name")
      val testData2 = Seq(("2", "data2")).toDF("id", "name")

      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")

      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)

      // List all files
      val allFiles = Parquet.listParquetFiles(spark, tempDir.toString)
      assert(allFiles.length >= 2)

      // Test filtering with skipFiles
      val fileToSkip = allFiles.head
      val skipFiles = Set(fileToSkip)
      val filesToProcess = allFiles.filterNot(skipFiles.contains)

      assertEquals(filesToProcess.size, allFiles.size - 1)
      assert(!filesToProcess.contains(fileToSkip))

    } finally
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
  }

  test("ParquetSavepointsManager tracks files during per-file processing") {
    val tempDir = Files.createTempDirectory("savepoints-tracking-test")
    val savepointsDir = Files.createTempDirectory("savepoints-output")

    try {
      import spark.implicits._

      val testData1 = Seq((1, "data1")).toDF("id", "name")
      val testData2 = Seq((2, "data2")).toDF("id", "name")
      val testData3 = Seq((3, "data3")).toDF("id", "name")

      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")
      val file3Path = tempDir.resolve("file3.parquet")

      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)
      testData3.write.parquet(file3Path.toString)

      val parquetSource = SourceSettings.Parquet(tempDir.toString, None, None, None)

      val allFiles = Parquet.listParquetFiles(spark, tempDir.toString)

      val config = MigratorConfig(
        source           = parquetSource,
        target           = null,
        renames          = None,
        savepoints       = com.scylladb.migrator.config.Savepoints(300, savepointsDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        assertEquals(
          manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty).size,
          0
        )

        allFiles.zipWithIndex.foreach { case (filePath, index) =>
          val singleFileDF = spark.read.parquet(filePath)
          val count = singleFileDF.count()
          assert(count >= 1)

          manager.markFileAsProcessed(filePath)
          val processedSoFar =
            manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty)
          assertEquals(processedSoFar.size, index + 1, s"After processing file ${index + 1}")
        }

        val finalProcessed =
          manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty)
        assertEquals(finalProcessed.size, allFiles.size)
        assert(finalProcessed == allFiles.toSet)

      } finally
        manager.close()

    } finally {
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
      Files
        .walk(savepointsDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }
}
