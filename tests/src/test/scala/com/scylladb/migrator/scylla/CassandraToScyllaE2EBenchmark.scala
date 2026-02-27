package com.scylladb.migrator.scylla

/** End-to-end throughput benchmarks for Cassandra-to-Scylla migration.
  *
  * Source: Cassandra (port 9043) Target: ScyllaDB (port 9042)
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
class CassandraToScyllaE2EBenchmark extends E2EBenchmarkSuite(sourcePort = 9043) {

  test(s"Cassandra->Scylla ${rowCount} rows without timestamps") {
    runThroughputBenchmark(
      scenario   = s"cassandra-to-scylla-nots-${rowCount}",
      tableName  = "bench_e2e_nots",
      configFile = "bench-e2e-cassandra-to-scylla-nots.yaml",
      rowCount   = rowCount
    )
  }

  test(s"Cassandra->Scylla ${rowCount} rows with timestamps") {
    runThroughputBenchmark(
      scenario   = s"cassandra-to-scylla-ts-${rowCount}",
      tableName  = "bench_e2e_ts",
      configFile = "bench-e2e-cassandra-to-scylla-ts.yaml",
      rowCount   = rowCount
    )
  }
}
