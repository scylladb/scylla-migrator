package com.scylladb.migrator.readers

import org.apache.spark.sql.SparkSession
import java.nio.file.Files

class PartitionMetadataReaderTest extends munit.FunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("PartitionMetadataReaderTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("readMetadata returns partition-to-file mappings") {
    val tempDir = Files.createTempDirectory("partition-metadata-test")

    try {
      import spark.implicits._

      // Create multiple parquet files
      val testData1 = (1 to 10).map(i => (i, s"data$i")).toDF("id", "name")
      val testData2 = (11 to 20).map(i => (i, s"data$i")).toDF("id", "name")

      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")

      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)

      val files = Parquet.listParquetFiles(spark, tempDir.toString)
      val metadata = PartitionMetadataReader.readMetadata(spark, files)

      assert(metadata.nonEmpty, "Metadata should not be empty")

      metadata.foreach { pm =>
        assert(pm.partitionId >= 0, s"Partition ID should be non-negative: ${pm.partitionId}")
        assert(pm.filename.nonEmpty, s"Filename should not be empty")
      }

      val uniqueFiles = metadata.map(_.filename).toSet
      assert(uniqueFiles.size >= 2, s"Should have at least 2 unique files, got ${uniqueFiles.size}")

    } finally {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("buildFileToPartitionsMap creates correct mapping") {
    val metadata = Seq(
      PartitionMetadata(0, "file1.parquet"),
      PartitionMetadata(1, "file1.parquet"),
      PartitionMetadata(2, "file2.parquet"),
      PartitionMetadata(3, "file3.parquet")
    )

    val fileToPartitions = PartitionMetadataReader.buildFileToPartitionsMap(metadata)

    assertEquals(fileToPartitions.size, 3)
    assertEquals(fileToPartitions("file1.parquet"), Set(0, 1))
    assertEquals(fileToPartitions("file2.parquet"), Set(2))
    assertEquals(fileToPartitions("file3.parquet"), Set(3))
  }

  test("buildPartitionToFileMap creates correct mapping") {
    val metadata = Seq(
      PartitionMetadata(0, "file1.parquet"),
      PartitionMetadata(1, "file1.parquet"),
      PartitionMetadata(2, "file2.parquet")
    )

    val partitionToFile = PartitionMetadataReader.buildPartitionToFileMap(metadata)

    assertEquals(partitionToFile.size, 3)
    assertEquals(partitionToFile(0), Set("file1.parquet"))
    assertEquals(partitionToFile(1), Set("file1.parquet"))
    assertEquals(partitionToFile(2), Set("file2.parquet"))
  }

  test("buildPartitionToFileMap groups multiple files per partition") {
    val metadata = Seq(
      PartitionMetadata(0, "file1.parquet"),
      PartitionMetadata(0, "file2.parquet"),
      PartitionMetadata(1, "file3.parquet")
    )

    val partitionToFile = PartitionMetadataReader.buildPartitionToFileMap(metadata)

    assertEquals(partitionToFile(0), Set("file1.parquet", "file2.parquet"))
    assertEquals(partitionToFile(1), Set("file3.parquet"))
  }

  test("file filtering logic works correctly") {
    val allFiles = Seq("file1.parquet", "file2.parquet", "file3.parquet")
    val processedFiles = Set("file1.parquet", "file3.parquet")

    val filesToProcess = allFiles.filterNot(processedFiles.contains)

    assertEquals(filesToProcess.size, 1)
    assertEquals(filesToProcess.head, "file2.parquet")
  }

  test("file filtering returns all files when skipFiles is empty") {
    val allFiles = Seq("file1.parquet", "file2.parquet")
    val skipFiles = Set.empty[String]

    val filesToProcess = allFiles.filterNot(skipFiles.contains)

    assertEquals(filesToProcess.size, allFiles.size)
    assertEquals(filesToProcess, allFiles)
  }

  test("reading metadata with file filtering") {
    val tempDir = Files.createTempDirectory("partition-metadata-filtering-test")

    try {
      import spark.implicits._

      val testData1 = (1 to 5).map(i => (i, s"data$i")).toDF("id", "name")
      val testData2 = (6 to 10).map(i => (i, s"data$i")).toDF("id", "name")

      val file1Path = tempDir.resolve("file1.parquet")
      val file2Path = tempDir.resolve("file2.parquet")

      testData1.write.parquet(file1Path.toString)
      testData2.write.parquet(file2Path.toString)

      val allFiles = Parquet.listParquetFiles(spark, tempDir.toString)
      val allMetadata = PartitionMetadataReader.readMetadata(spark, allFiles)

      val fileToSkip = allFiles.head
      val filesToProcess = allFiles.filterNot(_ == fileToSkip)

      val filteredMetadata = PartitionMetadataReader.readMetadata(spark, filesToProcess)

      assert(filteredMetadata.size < allMetadata.size)

      filteredMetadata.foreach { pm =>
        assert(pm.filename != fileToSkip, s"Skipped file ${fileToSkip} should not appear in filtered metadata")
      }

    } finally {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  test("readMetadata handles single partition per file") {
    val tempDir = Files.createTempDirectory("single-partition-test")

    try {
      import spark.implicits._

      val testData = Seq((1, "data")).toDF("id", "name")
      val filePath = tempDir.resolve("small.parquet")

      testData.write.parquet(filePath.toString)

      val files = Parquet.listParquetFiles(spark, tempDir.toString)
      val metadata = PartitionMetadataReader.readMetadata(spark, files)

      assert(metadata.nonEmpty, "Metadata should contain at least one entry")

      val fileToPartitions = PartitionMetadataReader.buildFileToPartitionsMap(metadata)

      assertEquals(fileToPartitions.size, 1)

    } finally {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }
}
