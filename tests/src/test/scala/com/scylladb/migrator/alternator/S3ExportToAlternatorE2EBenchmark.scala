package com.scylladb.migrator.alternator

import com.scylladb.migrator.{
  DynamoDBBenchmarkDataGenerator,
  SparkUtils,
  ThroughputReporter
}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

/** End-to-end throughput benchmark for S3 Export -> Alternator.
  *
  * Source: S3 Export files uploaded to LocalStack S3 Target: ScyllaDB Alternator (port 8000)
  *
  * Requires running `test-benchmark-e2e-dynamodb-s3export` first to generate S3 Export files.
  *
  * Row count is configurable via `-De2e.ddb.rows=N` (default: 500000).
  */
class S3ExportToAlternatorE2EBenchmark extends DynamoDBE2EBenchmarkSuite {

  private val bucketName = "bench-bucket"
  private val s3Prefix = "bench_s3export"

  private val s3Client: S3Client = S3TestUtils.localStackS3Client()

  override def afterAll(): Unit = {
    s3Client.close()
    super.afterAll()
  }

  test(s"S3Export->Alternator ${rowCount} rows") {
    val targetTable = "bench_e2e_ddb_s3_restore"

    assert(
      DynamoDBS3ExportE2EBenchmarkUtils.hasExportFiles,
      "No S3 export files found. Run test-benchmark-e2e-dynamodb-s3export first."
    )

    // Clean up target and S3 bucket
    deleteTableIfExists(targetAlternator(), targetTable)
    S3TestUtils.deleteBucket(s3Client, bucketName)
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    // Upload export files to LocalStack S3
    S3TestUtils.uploadDirToS3(
      s3Client,
      bucketName,
      DynamoDBS3ExportE2EBenchmarkUtils.s3ExportHostDir,
      s3Prefix
    )

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration("bench-e2e-s3export-to-alternator.yaml")
      val durationMs = System.currentTimeMillis() - startTime

      val targetCount = DynamoDBBenchmarkDataGenerator.countItems(targetAlternator(), targetTable)
      assertEquals(targetCount, rowCount.toLong, "Row count mismatch for S3Export->Alternator")

      // Spot-check a few rows to catch data corruption
      DynamoDBBenchmarkDataGenerator.spotCheckRows(targetAlternator(), targetTable, rowCount)

      ThroughputReporter.report(s"s3export-to-alternator-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      deleteTableIfExists(targetAlternator(), targetTable)
      S3TestUtils.deleteBucket(s3Client, bucketName)
      DynamoDBS3ExportE2EBenchmarkUtils.deleteS3ExportDir()
    }
  }
}
