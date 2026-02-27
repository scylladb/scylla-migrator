package com.scylladb.migrator.scylla

/** Throughput benchmarks for Scylla-to-Scylla migration paths.
  *
  * Source: ScyllaDB (port 9044, scylla-source) Target: ScyllaDB (port 9042, scylla)
  */
class ScyllaToScyllaBenchmark extends BenchmarkSuite(sourcePort = 9044) {

  test("Scylla->Scylla simple 100K rows without timestamps") {
    runThroughputBenchmark(
      scenario   = "scylla-to-scylla-simple-100k",
      tableName  = "bench_s2s_simple",
      configFile = "bench-scylla-to-scylla-simple.yaml",
      rowCount   = 100000
    )
  }

  test("Scylla->Scylla simple 500K rows without timestamps") {
    runThroughputBenchmark(
      scenario   = "scylla-to-scylla-simple-500k",
      tableName  = "bench_s2s_simple",
      configFile = "bench-scylla-to-scylla-simple.yaml",
      rowCount   = 500000
    )
  }
}
