package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  BenchmarkDataGenerator,
  E2E,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** End-to-end throughput benchmark for Scylla -> Parquet export.
  *
  * Source: ScyllaDB (port 9042) Target: Parquet at /app/parquet/bench_e2e
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class ScyllaToParquetE2EBenchmark extends MigratorSuite(sourcePort = 9042) {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount = sys.props.getOrElse("e2e.cql.rows", "5000000").toInt

  test(s"Scylla->Parquet ${rowCount} rows") {
    val sourceTable = "bench_e2e_parquet"

    ParquetE2EBenchmarkUtils.deleteParquetDir()
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, sourceTable)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, sourceTable, rowCount)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-scylla-to-parquet.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    val parquetFiles = ParquetE2EBenchmarkUtils.countParquetFiles()
    assert(
      parquetFiles > 0,
      s"Expected parquet files in ${ParquetE2EBenchmarkUtils.parquetHostDir}, found none"
    )

    ThroughputReporter.report(s"scylla-to-parquet-${rowCount}", rowCount.toLong, durationMs)
  }
}
