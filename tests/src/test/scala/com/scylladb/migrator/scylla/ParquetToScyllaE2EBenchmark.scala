package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  BenchmarkDataGenerator,
  SparkUtils,
  ThroughputReporter
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder

/** End-to-end throughput benchmark for Parquet -> Scylla import.
  *
  * Source: Parquet at /app/parquet/bench_e2e Target: ScyllaDB (port 9042)
  *
  * Requires running `test-benchmark-e2e-scylla-parquet` first to generate Parquet files.
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
class ParquetToScyllaE2EBenchmark extends E2EBenchmarkSuite(sourcePort = 9042) {

  test(s"Parquet->Scylla ${rowCount} rows") {
    val targetTable = "bench_e2e_parquet_restore"

    assert(
      ParquetE2EBenchmarkUtils.countParquetFiles() > 0,
      "No parquet files found. Run test-benchmark-e2e-scylla-parquet first."
    )

    val parquetRows = ParquetE2EBenchmarkUtils.countParquetRows()
    assertEquals(
      parquetRows,
      rowCount.toLong,
      s"Parquet files contain $parquetRows rows but expected $rowCount. " +
        "Stale files from a previous run with different -De2e.cql.rows? " +
        "Re-run test-benchmark-e2e-scylla-parquet first."
    )

    BenchmarkDataGenerator.createSimpleTable(targetScylla(), keyspace, targetTable)

    try {
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

      // Spot-check a few rows to catch data corruption
      spotCheckRows(targetTable, rowCount)

      ThroughputReporter.report(s"parquet-to-scylla-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      ParquetE2EBenchmarkUtils.deleteParquetDir()
    }
  }
}
