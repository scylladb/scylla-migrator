package com.scylladb.migrator.scylla

/** End-to-end throughput benchmarks for Scylla-to-Scylla migration.
  *
  * Source: ScyllaDB (port 9044, scylla-source) Target: ScyllaDB (port 9042, scylla)
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
class ScyllaToScyllaE2EBenchmark extends E2EBenchmarkSuite(sourcePort = 9044) {

  test(s"Scylla->Scylla ${rowCount} rows") {
    runThroughputBenchmark(
      scenario   = s"scylla-to-scylla-${rowCount}",
      tableName  = "bench_e2e_s2s",
      configFile = "bench-e2e-scylla-to-scylla.yaml",
      rowCount   = rowCount
    )
  }
}
