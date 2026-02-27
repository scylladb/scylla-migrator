package com.scylladb.migrator.alternator

import com.scylladb.migrator.{
  DynamoDBBenchmarkDataGenerator,
  E2E,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** End-to-end throughput benchmark for DynamoDB -> S3 Export.
  *
  * Source: DynamoDB Local (port 8001) Target: S3 Export files at /app/parquet/bench_s3export
  *
  * Row count is configurable via `-De2e.ddb.rows=N` (default: 500000).
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class DynamoDBToS3ExportE2EBenchmark extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount = sys.props.getOrElse("e2e.ddb.rows", "500000").toInt

  test(s"DynamoDB->S3Export ${rowCount} rows") {
    val tableName = "bench_e2e_ddb_s3"

    S3ExportE2EBenchmarkUtils.deleteS3ExportDir()
    DynamoDBBenchmarkDataGenerator.createSimpleTable(sourceDDb(), tableName)
    DynamoDBBenchmarkDataGenerator.insertSimpleRows(sourceDDb(), tableName, rowCount)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-dynamodb-to-s3export.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    assert(
      S3ExportE2EBenchmarkUtils.hasExportFiles,
      s"Expected S3 export files in ${S3ExportE2EBenchmarkUtils.s3ExportHostDir}, found none"
    )

    ThroughputReporter.report(s"dynamodb-to-s3export-${rowCount}", rowCount.toLong, durationMs)
  }
}
