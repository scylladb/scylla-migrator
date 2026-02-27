package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  BenchmarkDataGenerator,
  E2E,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** End-to-end throughput benchmark for Parquet -> Scylla import.
  *
  * Source: Parquet at /app/parquet/bench_e2e Target: ScyllaDB (port 9042)
  *
  * Requires running `test-benchmark-e2e-scylla-parquet` first to generate Parquet files.
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class ParquetToScyllaE2EBenchmark extends MigratorSuite(sourcePort = 9042) {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount = sys.props.getOrElse("e2e.cql.rows", "5000000").toInt

  test(s"Parquet->Scylla ${rowCount} rows") {
    val targetTable = "bench_e2e_parquet_restore"

    assert(
      ParquetE2EBenchmarkUtils.countParquetFiles() > 0,
      "No parquet files found. Run test-benchmark-e2e-scylla-parquet first."
    )

    BenchmarkDataGenerator.createSimpleTable(targetScylla(), keyspace, targetTable)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-parquet-to-scylla.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    val countResult = targetScylla()
      .execute(
        QueryBuilder
          .selectFrom(keyspace, targetTable)
          .countAll()
          .build()
      )
    val targetRowCount = countResult.one().getLong(0)
    assertEquals(targetRowCount, rowCount.toLong, "Row count mismatch for Parquet->Scylla")

    ThroughputReporter.report(s"parquet-to-scylla-${rowCount}", rowCount.toLong, durationMs)

    ParquetE2EBenchmarkUtils.deleteParquetDir()
  }
}
