package com.scylladb.migrator.readers

import com.aerospike.client.{
  AerospikeClient,
  AerospikeException,
  Host,
  Key,
  Record,
  ResultCode,
  ScanCallback
}
import com.aerospike.client.policy.{ ClientPolicy, ScanPolicy }
import com.aerospike.client.query.PartitionFilter
import com.scylladb.migrator.SparkUtils
import com.scylladb.migrator.config.{ AerospikeInvalidRecordHandling, AerospikeKeyMode }
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.{ LogEvent, Logger }
import org.apache.logging.log4j.core.appender.{ AbstractAppender, AppenderLoggingException }
import org.apache.logging.log4j.core.config.Property
import org.apache.spark.{ Partition, SparkContext, SparkException, TaskContext }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import com.aerospike.client.query.PartitionTracker

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicReference }

/** State stays in the local executor JVM, rather than serializing live threads or SDK clients. */
private object ControlledAerospikeScan {
  final class State(val retry: Boolean, val scanFailure: Option[Throwable]) {
    val attempts = new AtomicInteger(0)
    val taskThread = new AtomicReference[Thread]()
    val scanThread = new AtomicReference[Thread]()
    val callbackExited = new CountDownLatch(1)
    val recordConsumed = new CountDownLatch(1)
    val uncaughtFailure = new AtomicReference[Throwable]()

    def awaitQueueWait(thread: AtomicReference[Thread], operation: String): Unit = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      def blocked: Boolean = Option(thread.get()).exists(
        _.getStackTrace.exists(frame =>
          frame.getClassName == "java.util.concurrent.LinkedBlockingQueue" && frame.getMethodName == operation
        )
      )
      var observed = blocked
      while (!observed && System.nanoTime() < deadline) {
        Thread.`yield`()
        observed = blocked
      }
      require(observed, s"Scan fixture did not reach blocking queue $operation")
    }
  }
  @volatile var state: State = _
}

private class ControlledAerospikeRDD(sc: SparkContext, config: AerospikeConnectionConfig)
    extends AerospikeRDD(
      sc,
      config,
      AerospikeReadConfig(
        AerospikeKeyMode.StringKey,
        "ns",
        "set",
        1,
        1,
        10,
        1,
        1,
        "id",
        None,
        None,
        AerospikeInvalidRecordHandling.Skip
      ),
      sc.broadcast[Option[(String, String)]](None),
      sc.broadcast(IndexedSeq.empty[String]),
      sc.broadcast(StructType(Seq(StructField("id", StringType, nullable = false)))),
      sc.longAccumulator,
      sc.longAccumulator,
      sc.longAccumulator
    ) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    ControlledAerospikeScan.state.taskThread.set(Thread.currentThread())
    super.compute(split, context)
  }

  override protected def scanPartitions(
    client: AerospikeClient,
    policy: ScanPolicy,
    filter: PartitionFilter,
    callback: ScanCallback,
    bins: Option[Seq[String]]
  ): Unit = {
    val state = ControlledAerospikeScan.state
    state.scanThread.set(Thread.currentThread())
    if (state.scanFailure.nonEmpty)
      Thread
        .currentThread()
        .setUncaughtExceptionHandler((_, error) => state.uncaughtFailure.set(error))
    val attempt = state.attempts.incrementAndGet()
    if (state.retry && attempt == 1) {
      // The iterator has captured generation zero before this error advances it.
      state.awaitQueueWait(state.taskThread, "poll")
      throw new AerospikeException(ResultCode.TIMEOUT)
    }
    try
      if (state.scanFailure.nonEmpty) {
        callback.scanCallback(new Key("ns", "set", "before-failure"), new Record(null, 0, 0))
        if (!state.recordConsumed.await(5, TimeUnit.SECONDS))
          throw new AssertionError("The task did not consume the record before scan failure")
        throw state.scanFailure.get
      } else if (state.retry)
        Seq("retried-one", "retried-two")
          .foreach(id => callback.scanCallback(new Key("ns", "set", id), new Record(null, 0, 0)))
      else {
        var record = 0
        while (true) {
          callback.scanCallback(new Key("ns", "set", s"record-$record"), new Record(null, 0, 0))
          record += 1
        }
      }
    finally state.callbackExited.countDown()
  }
}

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

  private class DisconnectedClient extends AerospikeClient(
        { val policy = new ClientPolicy(); policy.failIfNotConnected = false; policy },
        Array.empty[Host]: _*
      ) {
    val closeCount = new AtomicInteger(0)
    override def close(): Unit = { closeCount.incrementAndGet(); super.close() }
  }

  private def withStartupClient(
    action: (AerospikeConnectionConfig, AerospikeConnectionKey, DisconnectedClient) => Unit
  ): Unit = {
    val config =
      connectionConfig.copy(hosts = List(s"startup-fixture-${java.util.UUID.randomUUID()}.invalid"))
    val key = AerospikeConnectionKey.fromConfig(config, None)
    val client = new DisconnectedClient
    AerospikeClientHolder.getOrCreate(key)(client)
    try action(config, key, client)
    finally
      try AerospikeClientHolder.release(config, None)
      finally if (client.closeCount.get() == 0) client.close()
  }

  private def assertStartupFailure(
    failure: Throwable
  )(createThread: AerospikeClient => Thread): Unit =
    withStartupClient { (config, key, client) =>
      // A second owner makes a duplicate startup release observable as premature replacement.
      assert(AerospikeClientHolder.acquire(key, config, None) eq client)
      val thrown =
        try {
          AerospikeRDD.startScanThread(key, config, None) { acquired =>
            assert(acquired eq client)
            createThread(acquired)
          }
          null
        } catch { case error: Throwable => error }
      assert(thrown eq failure, "Startup must preserve the original Throwable")
      assert(AerospikeClientHolder.getOrCreate(key) {
        throw new AssertionError("Startup released another owner's reference")
      } eq client)
      AerospikeClientHolder.releaseOne(key)
      assertStartupClientReleased(key, client)
    }

  private def assertStartupClientReleased(
    key: AerospikeConnectionKey,
    client: DisconnectedClient
  ): Unit = {
    var replacement: DisconnectedClient = null
    val cached = AerospikeClientHolder.getOrCreate(key) {
      replacement = new DisconnectedClient
      replacement
    }
    assert(replacement != null, "Startup/producer cleanup must leave no held reference")
    assert(cached eq replacement)
    assertEquals(client.closeCount.get(), 1)
  }

  test("scan thread factory failure releases exactly its acquired client reference") {
    val failure = new LinkageError("controlled thread construction failure")
    assertStartupFailure(failure)(_ => throw failure)
  }

  test("scan thread start failure releases exactly its acquired client reference") {
    val failure = new OutOfMemoryError("controlled thread start failure")
    assertStartupFailure(failure) { _ =>
      new Thread() {
        override def start(): Unit = throw failure
      }
    }
  }

  test("a started scan thread retains ownership until its producer releases the reference") {
    withStartupClient { (config, key, client) =>
      assert(AerospikeClientHolder.acquire(key, config, None) eq client)
      val finish = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val thread = AerospikeRDD.startScanThread(key, config, None) { acquired =>
        assert(acquired eq client)
        val producer = new Thread(() =>
          try
            if (!finish.await(5, TimeUnit.SECONDS))
              throw new AssertionError("Producer was not released")
          catch { case error: Throwable => failure.set(error) }
          finally AerospikeClientHolder.releaseOne(key)
        )
        producer.setDaemon(true)
        producer
      }
      try {
        finish.countDown()
        thread.join(5000)
        assert(!thread.isAlive, "Producer must exit before checking its released reference")
        assertEquals(failure.get(), null)
        assert(AerospikeClientHolder.getOrCreate(key) {
          throw new AssertionError("Successful startup released the producer's reference twice")
        } eq client)
        AerospikeClientHolder.releaseOne(key)
        assertStartupClientReleased(key, client)
      } finally {
        finish.countDown()
        thread.interrupt()
        thread.join(5000)
      }
    }
  }

  private def withControlledScan(
    retry: Boolean,
    scanFailure: Option[Throwable] = None
  )(action: ControlledAerospikeRDD => Unit): Unit =
    SparkUtils.withSparkSession() { spark =>
      val config =
        connectionConfig.copy(hosts = List(s"scan-fixture-${java.util.UUID.randomUUID()}.invalid"))
      val credentials = Aerospike.resolveCredentialsFromEnvOr(None)
      val key = AerospikeConnectionKey.fromConfig(config, credentials)
      val client = new DisconnectedClient
      val state = new ControlledAerospikeScan.State(retry, scanFailure)
      ControlledAerospikeScan.state = state
      AerospikeClientHolder.getOrCreate(key)(client)
      try {
        action(new ControlledAerospikeRDD(spark.sparkContext, config))
        assert(state.callbackExited.await(5, TimeUnit.SECONDS))
        state.scanThread.get().join(5000)
        assert(
          !state.scanThread.get().isAlive,
          "Scan thread must exit before its client can be replaced"
        )
        var replacement: DisconnectedClient = null
        val cached = AerospikeClientHolder.getOrCreate(key) {
          replacement = new DisconnectedClient
          replacement
        }
        assert(replacement != null, "Task/scan cleanup must release the holder reference")
        assert(cached eq replacement)
        assertEquals(client.closeCount.get(), 1)
      } finally
        try
          Option(state.scanThread.get()).foreach { thread =>
            thread.interrupt()
            thread.join(5000)
            assert(!thread.isAlive, "Fixture cleanup left a scan thread running")
          }
        finally
          try AerospikeClientHolder.release(config, credentials)
          finally
            try if (client.closeCount.get() == 0) client.close()
            finally ControlledAerospikeScan.state = null
    }

  test("actual compute adopts the retry generation and delivers every successful scan record") {
    withControlledScan(retry = true) { rdd =>
      assertEquals(rdd.collect().map(_.getString(0)).toSeq, Seq("retried-one", "retried-two"))
      assertEquals(ControlledAerospikeScan.state.attempts.get(), 2)
    }
  }

  test("successful take releases a blocked callback and its acquired client reference") {
    withControlledScan(retry = false) { rdd =>
      val first = rdd
        .mapPartitions { rows =>
          ControlledAerospikeScan.state.awaitQueueWait(
            ControlledAerospikeScan.state.scanThread,
            "offer"
          )
          rows
        }
        .take(1)
      assertEquals(first.map(_.getString(0)).toSeq, Seq("record-0"))
      assertEquals(ControlledAerospikeScan.state.attempts.get(), 1)
    }
  }

  test("a throwing error appender cannot hide or replace a fatal scan failure") {
    val scanFailure = new LinkageError("controlled fatal scan failure")
    val loggedFailure = new AtomicReference[Throwable]()
    val logAttempts = new AtomicInteger(0)
    withControlledScan(retry = false, scanFailure = Some(scanFailure)) { rdd =>
      val state = ControlledAerospikeScan.state
      val logger = Aerospike.log.asInstanceOf[Logger]
      val appender =
        new AbstractAppender(
          "aerospike-scan-error-test",
          null,
          null,
          false,
          Array.empty[Property]
        ) {
          override def append(event: LogEvent): Unit =
            if (event.getLevel == Level.ERROR) {
              loggedFailure.set(event.getThrown)
              logAttempts.incrementAndGet()
              throw new AppenderLoggingException("controlled scan diagnostic failure")
            }
        }
      val previousLevel = logger.getLevel
      appender.start()
      logger.addAppender(appender)
      logger.setLevel(Level.INFO)
      try {
        val thrown = intercept[SparkException] {
          rdd
            .map { row =>
              ControlledAerospikeScan.state.recordConsumed.countDown()
              row
            }
            .collect()
        }
        val causes = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null).toList
        assert(
          causes.exists(error =>
            error.isInstanceOf[LinkageError] && error.getMessage == scanFailure.getMessage
          ),
          "collect must fail with the original scan cause"
        )
        state.scanThread.get().join(5000)
        assert(!state.scanThread.get().isAlive, "Fatal producer must finish its cleanup")
        assert(state.uncaughtFailure.get() eq scanFailure)
        assert(loggedFailure.get() eq scanFailure)
        assertEquals(logAttempts.get(), 1)
        assertEquals(state.attempts.get(), 1)
      } finally {
        logger.removeAppender(appender)
        logger.setLevel(previousLevel)
        appender.stop()
      }
    }
  }

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
