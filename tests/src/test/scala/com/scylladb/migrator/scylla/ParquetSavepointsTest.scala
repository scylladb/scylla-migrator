package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.{MigratorConfig, SourceSettings}
import com.scylladb.migrator.readers.{Parquet, ParquetSavepointsManager, StringSetAccumulator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.{Files, Path, Paths}

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

  test("StringSetAccumulator basic functionality") {
    val accumulator = StringSetAccumulator()
    
    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Set.empty[String])
    
    accumulator.add("file1.parquet")
    accumulator.add("file2.parquet")
    
    assertEquals(accumulator.value, Set("file1.parquet", "file2.parquet"))
    assertEquals(accumulator.isZero, false)
    
    val copy = accumulator.copy()
    assertEquals(copy.value, accumulator.value)
    
    accumulator.reset()
    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Set.empty[String])
  }

  test("StringSetAccumulator merge functionality") {
    val accumulator1 = StringSetAccumulator(Set("file1.parquet"))
    val accumulator2 = StringSetAccumulator(Set("file2.parquet", "file3.parquet"))
    
    accumulator1.merge(accumulator2)
    assertEquals(accumulator1.value, Set("file1.parquet", "file2.parquet", "file3.parquet"))
  }

  test("Parquet file listing with single file") {
    // Note: Spark always writes to a directory, not a single file
    // When writing to a path ending with .parquet, Spark still creates a directory
    val tempDir = Files.createTempDirectory("parquet-test")
    val tempFile = tempDir.resolve("test.parquet")
    
    try {
      // Create a minimal Parquet file
      import spark.implicits._
      val testData = Seq(("1", "test")).toDF("id", "name")
      testData.write.parquet(tempFile.toString)
      
      val files = Parquet.listParquetFiles(spark, tempFile.toString)
      // Spark creates actual parquet files in subdirectories
      assert(files.size >= 1) // At least one parquet file
      assert(files.head.contains("part-") && files.head.endsWith(".parquet"))
      
    } finally {
      // Clean up
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("Parquet file listing with directory") {
    // Create temporary directory structure
    val tempDir = Files.createTempDirectory("parquet-dir-test")
    
    try {
      // Create multiple parquet files
      import spark.implicits._
      val testData1 = Seq(("1", "test1")).toDF("id", "name")
      val testData2 = Seq(("2", "test2")).toDF("id", "name")
      
      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")
      
      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)
      
      val files = Parquet.listParquetFiles(spark, tempDir.toString)
      // Should find parquet files inside the subdirectories created by Spark
      assert(files.size >= 2) // At least 2 parquet files
      files.foreach(file => {
        assert(file.contains("part-") && file.endsWith(".parquet"))
      })
      
    } finally {
      // Clean up
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("ParquetSavepointsManager initialization and state") {
    val tempDir = Files.createTempDirectory("savepoints-test")

    try {
      val config = MigratorConfig(
        source = SourceSettings.Parquet("dummy", None, None, None),
        target = null, // Not needed for this test
        renames = None,
        savepoints = com.scylladb.migrator.config.Savepoints(300, tempDir.toString),
        skipTokenRanges = None,
        skipSegments = None,
        skipParquetFiles = Some(Set("file1.parquet")),
        validation = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        // Check initial state - should show files from accumulator, not from hardcoded list
        val initialState = manager.describeMigrationState()
        // Initially should show 1 processed file (file1.parquet from skipParquetFiles)
        assert(initialState.contains("Processed files: 1"))

        // Note: We can't easily test the SparkListener functionality in unit tests
        // because it requires actual task execution. This tests the basic structure.

        // Test config update
        val updatedConfig = manager.updateConfigWithMigrationState()
        assertEquals(updatedConfig.skipParquetFiles.get, Set("file1.parquet"))

      } finally {
        manager.close()
      }

    } finally {
      // Clean up
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("Parquet reader with skipFiles functionality") {
    val tempDir = Files.createTempDirectory("parquet-skip-test")
    
    try {
      import spark.implicits._
      
      // Create multiple parquet files with different data
      val testData1 = Seq(("1", "data1")).toDF("id", "name")
      val testData2 = Seq(("2", "data2")).toDF("id", "name")
      
      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")
      
      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)
      
      val parquetSource = SourceSettings.Parquet(tempDir.toString, None, None, None)
      
      // Test: Get all files first, then filter
      val preparedReaderAll = Parquet.prepareParquetReader(spark, parquetSource, Set.empty)
      val allFiles = preparedReaderAll.allFiles
      assert(allFiles.length >= 2) // Should have at least 2 parquet files
      
      // Test reading all files using legacy method
      val allDataDF = Parquet.readDataFrame(spark, parquetSource)
      assert(allDataDF.dataFrame.count() >= 2L)
      
      // Test reading with skip files - skip the first found file
      val fileToSkip = allFiles.head
      val preparedReaderFiltered = Parquet.prepareParquetReader(spark, parquetSource, Set(fileToSkip))
      
      if (preparedReaderFiltered.filesToProcess.nonEmpty) {
        // Use legacy method with skipFiles
        val filteredDataDF = Parquet.readDataFrame(spark, parquetSource, Set(fileToSkip))
        // Should have less data after filtering
        val filteredCount = filteredDataDF.dataFrame.count()
        assert(filteredCount < allDataDF.dataFrame.count())
      }
      
    } finally {
      // Clean up
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("ParquetSavepointsManager tracks files during per-file processing") {
    val tempDir = Files.createTempDirectory("savepoints-tracking-test")
    val savepointsDir = Files.createTempDirectory("savepoints-output")

    try {
      import spark.implicits._

      // Create multiple parquet files
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

      // Prepare reader
      val preparedReader = Parquet.prepareParquetReader(spark, parquetSource, Set.empty)
      val allFiles = preparedReader.filesToProcess

      // Create config and savepoints manager
      val config = MigratorConfig(
        source = parquetSource,
        target = null,
        renames = None,
        savepoints = com.scylladb.migrator.config.Savepoints(300, savepointsDir.toString),
        skipTokenRanges = None,
        skipSegments = None,
        skipParquetFiles = None,
        validation = None
      )

      val manager = ParquetSavepointsManager(config, spark.sparkContext)

      try {
        // Initially no files processed
        assertEquals(manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty).size, 0)

        // Simulate per-file processing (as done in Migrator.scala)
        allFiles.zipWithIndex.foreach { case (filePath, index) =>
          // Read and process single file
          val singleFileDF = spark.read.parquet(filePath)
          val count = singleFileDF.count()
          assert(count >= 1)

          // Mark file as processed immediately after successful processing
          manager.markFileAsProcessed(filePath)

          // Verify accumulator is updated
          val processedSoFar = manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty)
          assertEquals(processedSoFar.size, index + 1, s"After processing file ${index + 1}")
        }

        // All files should be marked as processed now
        val finalProcessed = manager.updateConfigWithMigrationState().skipParquetFiles.getOrElse(Set.empty)
        assertEquals(finalProcessed.size, allFiles.size)
        assert(finalProcessed == allFiles.toSet)

      } finally {
        manager.close()
      }

    } finally {
      // Clean up
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
      Files.walk(savepointsDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }
}