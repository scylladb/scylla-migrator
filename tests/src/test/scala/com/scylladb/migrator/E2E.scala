package com.scylladb.migrator

/** munit tag for end-to-end throughput benchmarks (1M+ rows).
  *
  * Separated from [[Benchmark]] so that shorter benchmarks can run independently.
  */
class E2E extends munit.Tag("E2E")
