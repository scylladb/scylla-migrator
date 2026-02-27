package com.scylladb.migrator

/** Structured stdout reporter for integration throughput benchmarks.
  *
  * Output format is grep-friendly: {{{ BENCHMARK_RESULT | scenario=X rows=N durationMs=T
  * rowsPerSec=R }}}
  */
object ThroughputReporter {

  def report(scenario: String, rows: Long, durationMs: Long): Unit = {
    val rowsPerSec = if (durationMs > 0) rows * 1000.0 / durationMs else 0.0
    println(
      f"BENCHMARK_RESULT | scenario=$scenario rows=$rows durationMs=$durationMs rowsPerSec=$rowsPerSec%.1f"
    )
  }
}
