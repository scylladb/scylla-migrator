package com.scylladb.migrator.readers

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }

class AerospikeScanQueueTest extends munit.FunSuite {
  test("producer shutdown waits for active callbacks even if coordinator is interrupted") {
    val enteredOffer = new CountDownLatch(1)
    val allowExit = new CountDownLatch(1)
    val shutdownComplete = new CountDownLatch(1)
    val queue = new CancellableScanQueue(1, new AtomicBoolean(false)) {
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
      assert(!shutdownComplete.await(200, TimeUnit.MILLISECONDS))
      allowExit.countDown()
      assert(shutdownComplete.await(5, TimeUnit.SECONDS))
    } finally {
      allowExit.countDown()
      worker.join(5000)
      coordinator.join(5000)
    }
  }

  test("cancellation releases every blocked producer without interrupting workers") {
    val cancelled = new AtomicBoolean(false)
    val queue = new CancellableScanQueue(1, cancelled)
    queue.enqueue(ScanRetry(0))
    val started = new CountDownLatch(3)
    val stopped = new CountDownLatch(3)
    val error = new AtomicReference[Throwable]()
    val workers = (1 to 3).map { generation =>
      val worker = new Thread(() => {
        started.countDown()
        try {
          queue.enqueue(ScanRetry(generation))
          error.compareAndSet(null, new AssertionError("Full queue accepted a record"))
        } catch {
          case _: ScanCancelledException =>
          case e: Throwable              => error.compareAndSet(null, e)
        } finally stopped.countDown()
      })
      worker.setDaemon(true)
      worker.start()
      worker
    }
    try {
      assert(started.await(5, TimeUnit.SECONDS))
      assert(!stopped.await(200, TimeUnit.MILLISECONDS))
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
      assert(!stopped.await(200, TimeUnit.MILLISECONDS))
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
