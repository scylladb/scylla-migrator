package com.scylladb.migrator.scylla

import com.scylladb.migrator.{ BenchmarkDataGenerator, SparkUtils, ThroughputReporter }
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

/** End-to-end throughput benchmark for Scylla -> Parquet export.
  *
  * Source: ScyllaDB (port 9042) Target: Parquet at /app/parquet/bench_e2e
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
class ScyllaToParquetE2EBenchmark extends E2EBenchmarkSuite(sourcePort = 9042) {

  test(s"Scylla->Parquet ${rowCount} rows") {
    val sourceTable = "bench_e2e_parquet"

    ParquetE2EBenchmarkUtils.deleteParquetDir()
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, sourceTable)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, sourceTable, rowCount)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration("bench-e2e-scylla-to-parquet.yaml")
      val durationMs = System.currentTimeMillis() - startTime

      val parquetFiles = ParquetE2EBenchmarkUtils.countParquetFiles()
      assert(
        parquetFiles > 0,
        s"Expected parquet files in ${ParquetE2EBenchmarkUtils.parquetHostDir}, found none"
      )

      val parquetRows = ParquetE2EBenchmarkUtils.countParquetRows()
      assertEquals(parquetRows, rowCount.toLong, "Parquet row count mismatch")

      // Spot-check a few rows to catch data corruption
      val rows = ParquetE2EBenchmarkUtils.readFirstRows(ParquetE2EBenchmarkUtils.parquetHostDir, 3)
      assert(rows.nonEmpty, "Spot-check: should be able to read at least one row")
      for (row <- rows) {
        assert(row.contains("id"), s"Spot-check: row missing 'id' column: $row")
        assert(row.contains("col1"), s"Spot-check: row missing 'col1' column: $row")
        assert(row("id").nonEmpty, s"Spot-check: 'id' should not be empty: $row")
      }

      ThroughputReporter.report(s"scylla-to-parquet-${rowCount}", rowCount.toLong, durationMs)
    } finally
      // Parquet files are intentionally NOT deleted here because
      // ParquetToScyllaE2EBenchmark depends on them. Cleanup happens there.
      try
        sourceCassandra().execute(
          SchemaBuilder.dropTable(keyspace, sourceTable).ifExists().build()
        )
      catch {
        case e: Exception =>
          System.err.println(s"Cleanup: failed to drop source table: ${e.getMessage}")
      }
  }
}
