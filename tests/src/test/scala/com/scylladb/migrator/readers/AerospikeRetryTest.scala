package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.query.PartitionTracker

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicReference }

class AerospikeRetryTest extends munit.FunSuite {
  private val connectionConfig = AerospikeConnectionConfig(
    hosts            = List("localhost"),
    port             = 3000,
    connectTimeoutMs = None,
    socketTimeoutMs  = Some(5000),
    totalTimeoutMs   = Some(15000),
    tlsName          = None,
    maxConnsPerNode  = None,
    connPoolsPerNode = None
  )

  test("data scans leave retries to the RDD while preserving configured timeouts") {
    val policy = AerospikeRDD.buildScanPolicy(connectionConfig, includeBinData = true)
    assertEquals(policy.maxRetries, 0, "SDK retries must not multiply maxScanRetries")
    assertEquals(policy.socketTimeout, 5000)
    assertEquals(policy.totalTimeout, 15000)
    assert(policy.includeBinData)
  }

  test("SDK unavailable partitions use the bounded RDD retry path") {
    val policy = AerospikeRDD.buildScanPolicy(connectionConfig, includeBinData = true)
    val tracker = new PartitionTracker(policy, 1)
    val nodePartitions = new PartitionTracker.NodePartitions(null, 1)
    tracker.partitionUnavailable(nodePartitions, 0)
    val error = intercept[AerospikeException] {
      tracker.isComplete(false, policy, java.util.Collections.singletonList(nodePartitions))
    }
    assertEquals(error.getResultCode, ResultCode.PARTITION_UNAVAILABLE)
    assert(AerospikeRDD.shouldRetryScan(error, retryCount = 0, maxRetries = 2, cancelled = false))
    assert(!AerospikeRDD.shouldRetryScan(error, retryCount = 2, maxRetries = 2, cancelled = false))
    assert(!AerospikeRDD.shouldRetryScan(error, retryCount = 0, maxRetries = 0, cancelled = false))
    assert(!AerospikeRDD.shouldRetryScan(error, retryCount = 0, maxRetries = 2, cancelled = true))
  }

  private def blockedPoll(
    retryInProgress: AtomicBoolean,
    generation: AtomicInteger
  )(duringPoll: => Unit): Boolean = {
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val result = new AtomicReference[(ScanQueueEntry, Boolean)]()
    val failure = new AtomicReference[Throwable]()
    val queue = new CancellableScanQueue(1, new AtomicBoolean(false)) {
      override def poll(timeout: Long, unit: TimeUnit): ScanQueueEntry = {
        entered.countDown()
        if (!finish.await(5, TimeUnit.SECONDS)) throw new AssertionError("Poll was not released")
        super.poll()
      }
    }
    val worker = new Thread(() =>
      try
        result.set(AerospikeRDD.pollWithRetryAllowance(retryInProgress, generation) {
          queue.poll(2, TimeUnit.SECONDS)
        })
      catch { case e: Throwable => failure.set(e) }
    )
    worker.setDaemon(true)
    worker.start()
    try {
      assert(entered.await(5, TimeUnit.SECONDS))
      duringPoll
      finish.countDown()
      worker.join(5000)
      assert(!worker.isAlive)
      assertEquals(failure.get(), null)
      assertEquals(result.get()._1, null)
      result.get()._2
    } finally {
      finish.countDown()
      worker.interrupt()
      worker.join(5000)
    }
  }

  test("backoff ending during a blocked poll does not consume the idle-timeout budget") {
    val retryInProgress = new AtomicBoolean(true)
    val generation = new AtomicInteger(1)
    assert(blockedPoll(retryInProgress, generation) {
      retryInProgress.set(false)
    })
  }

  test("a retry starting and ending during a blocked poll grants only that poll an allowance") {
    val retryInProgress = new AtomicBoolean(false)
    val generation = new AtomicInteger(0)
    assert(blockedPoll(retryInProgress, generation) {
      retryInProgress.set(true)
      generation.incrementAndGet()
      retryInProgress.set(false)
    })
    val (_, retryOverlapped) =
      AerospikeRDD.pollWithRetryAllowance(retryInProgress, generation)(null)
    assert(!retryOverlapped, "The next idle poll must count toward maxPollRetries")
  }

  test("an idle poll without a retry counts toward the finite timeout budget") {
    assert(!blockedPoll(new AtomicBoolean(false), new AtomicInteger(0))(()))
  }
}
