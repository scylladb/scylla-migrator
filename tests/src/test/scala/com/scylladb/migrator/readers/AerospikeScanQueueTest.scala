package com.scylladb.migrator.readers

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }

class AerospikeScanQueueTest extends munit.FunSuite {
  private def awaitBlockedOffer(worker: Thread): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    def blocked: Boolean =
      worker.getState == Thread.State.TIMED_WAITING &&
        worker.getStackTrace.exists(frame =>
          frame.getClassName == "java.util.concurrent.LinkedBlockingQueue" &&
            frame.getMethodName == "offer"
        )
    var observedWait = blocked
    while (!observedWait && worker.isAlive && System.nanoTime() < deadline) {
      Thread.`yield`()
      observedWait = blocked
    }
    assert(observedWait, "Producer must block in LinkedBlockingQueue.offer before it is released")
  }

  test("producer shutdown waits for active callbacks even if coordinator is interrupted") {
    val enteredOffer = new CountDownLatch(1)
    val allowExit = new CountDownLatch(1)
    val shutdownComplete = new CountDownLatch(1)
    val cancelled = new AtomicBoolean(false)
    val queue = new CancellableScanQueue(1, cancelled) {
      override def offer(entry: ScanQueueEntry, timeout: Long, unit: TimeUnit): Boolean = {
        enteredOffer.countDown()
        allowExit.await()
        false
      }
    }
    val worker = new Thread(() =>
      try queue.enqueue(ScanRetry(0))
      catch { case _: ScanCancelledException => }
    )
    val coordinator = new Thread(() => {
      Thread.currentThread().interrupt()
      queue.stopProducers()
      shutdownComplete.countDown()
    })
    worker.setDaemon(true)
    coordinator.setDaemon(true)
    worker.start()
    try {
      assert(enteredOffer.await(5, TimeUnit.SECONDS))
      coordinator.start()
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      def waitingForProducers: Boolean = {
        val state = coordinator.getState
        (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) &&
        coordinator.getStackTrace.exists(frame =>
          frame.getClassName == "java.util.concurrent.Phaser" &&
            frame.getMethodName == "arriveAndAwaitAdvance"
        )
      }
      var observedWait = waitingForProducers
      while (!observedWait && coordinator.isAlive && System.nanoTime() < deadline) {
        Thread.`yield`()
        observedWait = waitingForProducers
      }
      assert(observedWait, "Coordinator must wait in Phaser before the active callback is released")
      assertEquals(shutdownComplete.getCount, 1L)
      allowExit.countDown()
      assert(shutdownComplete.await(5, TimeUnit.SECONDS))
    } finally {
      cancelled.set(true)
      allowExit.countDown()
      worker.join(5000)
      coordinator.join(5000)
    }
  }

  test("cancellation releases every blocked producer without interrupting workers") {
    val cancelled = new AtomicBoolean(false)
    val queue = new CancellableScanQueue(1, cancelled)
    queue.enqueue(ScanRetry(0))
    val stopped = new CountDownLatch(3)
    val error = new AtomicReference[Throwable]()
    val workers = (1 to 3).map { generation =>
      val worker = new Thread(() =>
        try {
          queue.enqueue(ScanRetry(generation))
          error.compareAndSet(null, new AssertionError("Full queue accepted a record"))
        } catch {
          case _: ScanCancelledException =>
          case e: Throwable              => error.compareAndSet(null, e)
        } finally stopped.countDown()
      )
      worker.setDaemon(true)
      worker.start()
      worker
    }
    try {
      workers.foreach(awaitBlockedOffer)
      assertEquals(stopped.getCount, 3L)
      cancelled.set(true)
      assert(stopped.await(5, TimeUnit.SECONDS))
      queue.stopProducers()
      assertEquals(error.get(), null)
      intercept[ScanCancelledException](queue.enqueue(ScanComplete))
    } finally {
      cancelled.set(true)
      workers.foreach(_.interrupt())
      workers.foreach(_.join(5000))
    }
  }

  test("queue preserves backpressure until a consumer makes room") {
    val queue = new CancellableScanQueue(1, new AtomicBoolean(false))
    queue.enqueue(ScanRetry(0))
    val stopped = new CountDownLatch(1)
    val worker = new Thread(() =>
      try queue.enqueue(ScanComplete)
      finally stopped.countDown()
    )
    worker.setDaemon(true)
    worker.start()
    try {
      awaitBlockedOffer(worker)
      assertEquals(stopped.getCount, 1L)
      assertEquals(queue.poll(5, TimeUnit.SECONDS), ScanRetry(0))
      assert(stopped.await(5, TimeUnit.SECONDS))
      assertEquals(queue.poll(5, TimeUnit.SECONDS), ScanComplete)
    } finally {
      worker.interrupt()
      worker.join(5000)
      queue.stopProducers()
    }
  }
}
