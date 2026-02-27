package com.scylladb.migrator.scylla

/** Throughput benchmarks for Cassandra-to-Scylla migration paths.
  *
  * Source: Cassandra (port 9043) Target: ScyllaDB (port 9042)
  */
class CassandraToScyllaBenchmark extends BenchmarkSuite(sourcePort = 9043) {

  test("Cassandra->Scylla simple 100K rows without timestamps") {
    runThroughputBenchmark(
      scenario   = "cassandra-to-scylla-simple-nots-100k",
      tableName  = "bench_simple_nots",
      configFile = "bench-cassandra-to-scylla-simple-nots.yaml",
      rowCount   = 100000
    )
  }

  test("Cassandra->Scylla simple 500K rows without timestamps") {
    runThroughputBenchmark(
      scenario   = "cassandra-to-scylla-simple-nots-500k",
      tableName  = "bench_simple_nots",
      configFile = "bench-cassandra-to-scylla-simple-nots.yaml",
      rowCount   = 500000
    )
  }

  test("Cassandra->Scylla simple 100K rows with timestamps") {
    runThroughputBenchmark(
      scenario   = "cassandra-to-scylla-simple-ts-100k",
      tableName  = "bench_simple_ts",
      configFile = "bench-cassandra-to-scylla-simple-ts.yaml",
      rowCount   = 100000
    )
  }

  test("Cassandra->Scylla simple 500K rows with timestamps") {
    runThroughputBenchmark(
      scenario   = "cassandra-to-scylla-simple-ts-500k",
      tableName  = "bench_simple_ts",
      configFile = "bench-cassandra-to-scylla-simple-ts.yaml",
      rowCount   = 500000
    )
  }
}
