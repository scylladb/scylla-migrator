package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  BenchmarkDataGenerator,
  SparkUtils,
  TestFileUtils,
  ThroughputReporter
}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

import java.io.File

/** End-to-end throughput benchmark for Cassandra -> Parquet export.
  *
  * Source: Cassandra (port 9043) Target: Parquet at /app/parquet/bench_e2e_cass
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
class CassandraToParquetE2EBenchmark extends E2EBenchmarkSuite(sourcePort = 9043) {

  // Host path maps to /app/parquet/bench_e2e_cass inside the Spark container
  private val parquetHostDir = new File("docker/parquet/bench_e2e_cass")

  test(s"Cassandra->Parquet ${rowCount} rows") {
    val sourceTable = "bench_e2e_cass_parquet"

    if (parquetHostDir.exists()) TestFileUtils.deleteRecursive(parquetHostDir)
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, sourceTable)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, sourceTable, rowCount)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration("bench-e2e-cassandra-to-parquet.yaml")
      val durationMs = System.currentTimeMillis() - startTime

      val parquetFiles = ParquetE2EBenchmarkUtils.countParquetFilesIn(parquetHostDir)
      assert(
        parquetFiles > 0,
        s"Expected parquet files in ${parquetHostDir}, found none"
      )

      val parquetRows = ParquetE2EBenchmarkUtils.countParquetRowsIn(parquetHostDir)
      assertEquals(parquetRows, rowCount.toLong, "Parquet row count mismatch")

      // Spot-check a few rows to catch data corruption
      val rows = ParquetE2EBenchmarkUtils.readFirstRows(parquetHostDir, 3)
      assert(rows.nonEmpty, "Spot-check: should be able to read at least one row")
      for (row <- rows) {
        assert(row.contains("id"), s"Spot-check: row missing 'id' column: $row")
        assert(row.contains("col1"), s"Spot-check: row missing 'col1' column: $row")
        assert(row("id").nonEmpty, s"Spot-check: 'id' should not be empty: $row")
      }

      ThroughputReporter.report(s"cassandra-to-parquet-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      if (parquetHostDir.exists()) TestFileUtils.deleteRecursive(parquetHostDir)
      try sourceCassandra().execute(
        SchemaBuilder.dropTable(keyspace, sourceTable).ifExists().build()
      )
      catch { case e: Exception => System.err.println(s"Cleanup: failed to drop source table: ${e.getMessage}") }
    }
  }
}
