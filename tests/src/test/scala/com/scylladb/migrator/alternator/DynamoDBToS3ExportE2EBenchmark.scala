package com.scylladb.migrator.alternator

import com.scylladb.migrator.{
  DynamoDBBenchmarkDataGenerator,
  SparkUtils,
  ThroughputReporter
}
import io.circe.parser.{ decode => jsonDecode }

import java.io.File
import scala.io.Source
import scala.util.Using

/** End-to-end throughput benchmark for DynamoDB -> S3 Export.
  *
  * Source: DynamoDB Local (port 8001) Target: S3 Export files at /app/parquet/bench_s3export
  *
  * Row count is configurable via `-De2e.ddb.rows=N` (default: 500000).
  */
class DynamoDBToS3ExportE2EBenchmark extends DynamoDBE2EBenchmarkSuite {

  test(s"DynamoDB->S3Export ${rowCount} rows") {
    val tableName = "bench_e2e_ddb_s3"

    DynamoDBS3ExportE2EBenchmarkUtils.deleteS3ExportDir()
    DynamoDBBenchmarkDataGenerator.createSimpleTable(sourceDDb(), tableName)
    DynamoDBBenchmarkDataGenerator.insertSimpleRows(sourceDDb(), tableName, rowCount)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration("bench-e2e-dynamodb-to-s3export.yaml")
      val durationMs = System.currentTimeMillis() - startTime

      assert(
        DynamoDBS3ExportE2EBenchmarkUtils.hasExportFiles,
        s"Expected S3 export files in ${DynamoDBS3ExportE2EBenchmarkUtils.s3ExportHostDir}, found none"
      )

      // Verify item count from manifest
      val summaryFile = new File(DynamoDBS3ExportE2EBenchmarkUtils.s3ExportHostDir, "manifest-summary.json")
      val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
      val summary = jsonDecode[DynamoDBS3ExportE2EBenchmarkUtils.ManifestSummary](summaryJson).fold(throw _, identity)
      assertEquals(summary.itemCount, rowCount.toLong, "Manifest item count mismatch")

      ThroughputReporter.report(s"dynamodb-to-s3export-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      deleteTableIfExists(sourceDDb(), tableName)
      // S3 export files are intentionally NOT deleted here because
      // S3ExportToAlternatorE2EBenchmark depends on them. Cleanup happens there.
    }
  }

}
