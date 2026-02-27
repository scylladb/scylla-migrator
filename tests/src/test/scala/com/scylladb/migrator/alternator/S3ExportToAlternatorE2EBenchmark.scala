package com.scylladb.migrator.alternator

import com.scylladb.migrator.{
  DynamoDBBenchmarkDataGenerator,
  E2E,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import org.junit.experimental.categories.Category
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  DeleteBucketRequest,
  DeleteObjectRequest,
  HeadBucketRequest,
  ListObjectsV2Request,
  NoSuchBucketException,
  PutObjectRequest
}

import java.io.File
import java.net.URI
import java.nio.file.Files
import scala.concurrent.duration._

/** End-to-end throughput benchmark for S3 Export -> Alternator.
  *
  * Source: S3 Export files uploaded to LocalStack S3 Target: ScyllaDB Alternator (port 8000)
  *
  * Requires running `test-benchmark-e2e-dynamodb-s3export` first to generate S3 Export files.
  *
  * Row count is configurable via `-De2e.ddb.rows=N` (default: 500000).
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class S3ExportToAlternatorE2EBenchmark extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount = sys.props.getOrElse("e2e.ddb.rows", "500000").toInt
  private val bucketName = "bench-bucket"
  private val s3Prefix = "bench_s3export"

  private val s3Client: S3Client =
    S3Client
      .builder()
      .endpointOverride(new URI("http://127.0.0.1:4566"))
      .region(Region.EU_WEST_1)
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
      )
      .forcePathStyle(true)
      .build()

  test(s"S3Export->Alternator ${rowCount} rows") {
    val targetTable = "bench_e2e_ddb_s3_restore"

    assert(
      S3ExportE2EBenchmarkUtils.hasExportFiles,
      "No S3 export files found. Run test-benchmark-e2e-dynamodb-s3export first."
    )

    // Clean up target and S3 bucket
    deleteTableIfExists(targetAlternator(), targetTable)
    deleteBucket()
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    // Upload export files to LocalStack S3
    uploadDirToS3(S3ExportE2EBenchmarkUtils.s3ExportHostDir, s3Prefix)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-s3export-to-alternator.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    val targetCount = DynamoDBBenchmarkDataGenerator.countItems(targetAlternator(), targetTable)
    assertEquals(targetCount, rowCount.toLong, "Row count mismatch for S3Export->Alternator")

    ThroughputReporter.report(s"s3export-to-alternator-${rowCount}", rowCount.toLong, durationMs)

    // Clean up
    deleteBucket()
    S3ExportE2EBenchmarkUtils.deleteS3ExportDir()
  }

  private def uploadDirToS3(localDir: File, prefix: String): Unit =
    if (localDir.isDirectory) {
      val files = localDir.listFiles()
      if (files != null)
        files.foreach { f =>
          val key = s"${prefix}/${f.getName}"
          if (f.isDirectory) uploadDirToS3(f, key)
          else
            s3Client.putObject(
              PutObjectRequest.builder().bucket(bucketName).key(key).build(),
              f.toPath
            )
        }
    }

  private def deleteBucket(): Unit =
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
      s3Client
        .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).build())
        .stream()
        .forEach { response =>
          response.contents().stream().forEach { s3Object =>
            s3Client.deleteObject(
              DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build()
            )
          }
        }
      s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    } catch {
      case _: NoSuchBucketException => ()
    }
}
