package com.scylladb.migrator.aerospike

import com.aerospike.client.{ AerospikeClient, Bin, Key }
import com.aerospike.client.policy.WritePolicy

import org.apache.logging.log4j.LogManager

import java.util.concurrent.{ LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit }
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

/** Generates test data in Aerospike for throughput benchmarks. */
object AerospikeBenchmarkDataGenerator {

  private val log = LogManager.getLogger(getClass)

  /** Sample indices for spot-checking benchmark rows: first, 25%, 50%, last. */
  def sampleIndices(rowCount: Int): Seq[Int] =
    if (rowCount <= 0) Seq.empty
    else Seq(0, rowCount / 4, rowCount / 2, rowCount - 1).distinct.filter(_ >= 0)

  /** Insert rows concurrently using a thread pool.
    *
    * Each record has: key=id-N, col1=value-N (String), col2=N (Long), col3=N*1000 (Long)
    *
    * For single-node test setups, records are immediately available after this method returns. For
    * multi-node clusters, add a post-insert verification step (e.g., `client.exists()` on the last
    * key) to ensure replication has completed.
    *
    * @param client
    *   Aerospike client (thread-safe)
    * @param namespace
    *   Aerospike namespace
    * @param set
    *   Aerospike set name
    * @param rowCount
    *   number of rows to insert
    * @param maxConcurrent
    *   max in-flight insert operations
    */
  def insertSimpleRows(
    client: AerospikeClient,
    namespace: String,
    set: String,
    rowCount: Int,
    maxConcurrent: Int = 64
  ): Unit = {
    val policy = new WritePolicy()
    policy.sendKey = true

    // Bounded queue with CallerRunsPolicy provides backpressure: when the queue is full,
    // the submitting thread runs the task itself, throttling submission rate.
    val executor = new ThreadPoolExecutor(
      maxConcurrent,
      maxConcurrent,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](maxConcurrent * 2),
      new ThreadPoolExecutor.CallerRunsPolicy()
    )
    val firstError = new AtomicReference[Throwable](null)
    val completed = new AtomicLong(0)
    val startTime = System.currentTimeMillis()

    try
      for (i <- 0 until rowCount) {
        firstError.get() match {
          case null => // no error yet
          case ex   => throw new RuntimeException("Insert failed", ex)
        }
        executor.execute(() =>
          try {
            val key = new Key(namespace, set, s"id-$i")
            client.put(
              policy,
              key,
              new Bin("col1", s"value-$i"),
              new Bin("col2", i.toLong),
              new Bin("col3", i.toLong * 1000L)
            )
            val done = completed.incrementAndGet()
            if (done % 100000 == 0) {
              val elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0
              val rate = done / elapsedSec
              log.info(f"Inserted $done%,d / $rowCount%,d rows ($rate%.0f rows/sec)")
            }
          } catch {
            case e: Throwable =>
              if (!firstError.compareAndSet(null, e)) {
                // Another error was already recorded — add this one as suppressed
                val existing = firstError.get()
                if (existing != null) existing.addSuppressed(e)
              }
          }
        )
      }
    finally {
      executor.shutdown()
      try
        if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
          log.warn("AerospikeBenchmarkDataGenerator executor did not terminate within 5 minutes")
          executor.shutdownNow()
        }
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          executor.shutdownNow()
      }
    }
    firstError.get() match {
      case null => // success
        assert(
          completed.get() == rowCount.toLong,
          s"Expected $rowCount rows, inserted ${completed.get()}"
        )
      case ex => throw new RuntimeException("Insert failed", ex)
    }
  }
}
