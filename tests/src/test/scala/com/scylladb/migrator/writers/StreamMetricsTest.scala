package com.scylladb.migrator.writers

class StreamMetricsTest extends munit.FunSuite {

  test("publishToCloudWatch does not throw when CloudWatch is disabled") {
    val metrics = new StreamMetrics("test-table", Some("us-east-1"), enableCloudWatch = false)
    try {
      // Should be a no-op when CloudWatch is disabled
      metrics.publishToCloudWatch()
    } finally
      metrics.close()
  }

  test("metric counters are correctly initialized to zero") {
    val metrics = new StreamMetrics("test-table", None, enableCloudWatch = false)
    try {
      assertEquals(metrics.recordsProcessed.get(), 0L)
      assertEquals(metrics.pollCycles.get(), 0L)
      assertEquals(metrics.activeShards.get(), 0L)
      assertEquals(metrics.maxIteratorAgeMs.get(), 0L)
      assertEquals(metrics.lastPollDurationMs.get(), 0L)
    } finally
      metrics.close()
  }

  test("metric counters can be updated and read back") {
    val metrics = new StreamMetrics("test-table", None, enableCloudWatch = false)
    try {
      metrics.recordsProcessed.set(100L)
      metrics.pollCycles.set(10L)
      metrics.activeShards.set(3L)
      metrics.maxIteratorAgeMs.set(5000L)
      metrics.lastPollDurationMs.set(250L)

      assertEquals(metrics.recordsProcessed.get(), 100L)
      assertEquals(metrics.pollCycles.get(), 10L)
      assertEquals(metrics.activeShards.get(), 3L)
      assertEquals(metrics.maxIteratorAgeMs.get(), 5000L)
      assertEquals(metrics.lastPollDurationMs.get(), 250L)
    } finally
      metrics.close()
  }

  test("metric counters support atomic increment") {
    val metrics = new StreamMetrics("test-table", None, enableCloudWatch = false)
    try {
      metrics.recordsProcessed.addAndGet(50L)
      metrics.recordsProcessed.addAndGet(30L)
      assertEquals(metrics.recordsProcessed.get(), 80L)
    } finally
      metrics.close()
  }

  test("close does not throw when CloudWatch is disabled") {
    val metrics = new StreamMetrics("test-table", None, enableCloudWatch = false)
    // Should not throw
    metrics.close()
    // Calling close again should also not throw
    metrics.close()
  }

  test("publishToCloudWatch is idempotent when disabled") {
    val metrics = new StreamMetrics("test-table", Some("eu-west-1"), enableCloudWatch = false)
    try {
      // Multiple calls should all be no-ops
      metrics.publishToCloudWatch()
      metrics.publishToCloudWatch()
      metrics.publishToCloudWatch()
    } finally
      metrics.close()
  }

  test("publishToCloudWatch handles error gracefully when CloudWatch is enabled but unreachable") {
    // Create metrics with CloudWatch enabled — it will fail to connect to real CloudWatch
    // since we're running locally with dummy credentials, but should not throw
    val metrics = new StreamMetrics("test-table", Some("us-east-1"), enableCloudWatch = true)
    try {
      metrics.recordsProcessed.set(42L)
      metrics.activeShards.set(2L)
      // publishToCloudWatch should catch the exception internally and not propagate it
      metrics.publishToCloudWatch()
    } finally
      metrics.close()
  }

  test("close does not throw when CloudWatch is enabled but unreachable") {
    val metrics = new StreamMetrics("test-table", Some("us-east-1"), enableCloudWatch = true)
    // Force lazy initialization by publishing (it will fail silently)
    metrics.publishToCloudWatch()
    // close should handle cleanup gracefully
    metrics.close()
  }
}
