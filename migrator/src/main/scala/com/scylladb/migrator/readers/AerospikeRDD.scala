package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeException, Key, Record, ScanCallback }
import com.aerospike.client.policy.ScanPolicy
import com.aerospike.client.query.PartitionFilter
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicReference }

/** Thrown when a Spark task cancellation interrupts an Aerospike scan callback. This is expected
  * during limit()/take() operations and should not be logged as an error.
  */
private[migrator] class ScanCancelledException extends RuntimeException("Task cancelled")

/** Queue entry types for the scan-to-iterator pipeline in AerospikeRDD. */
private[migrator] sealed trait ScanQueueEntry

/** A data record from the scan, tagged with a generation counter for retry filtering. */
private[migrator] case class ScanData(generation: Int, key: Key, record: Record)
    extends ScanQueueEntry

/** Sentinel posted after a retry to unblock the iterator; carries the stale generation so the
  * iterator discards it.
  */
private[migrator] case class ScanRetry(staleGeneration: Int) extends ScanQueueEntry

/** Sentinel signaling that the scan has completed (successfully or with an error). */
private[migrator] case object ScanComplete extends ScanQueueEntry

private[migrator] object AerospikeRDD {

  /** Compute Aerospike partition ranges for the given split count */
  def computePartitions(splitCount: Int): Array[AerospikePartition] = {
    val totalPartitions = Aerospike.TotalAerospikePartitions
    val partitionsPerSplit = totalPartitions / splitCount
    val remainder = totalPartitions % splitCount

    (0 until splitCount).map { i =>
      val begin = i * partitionsPerSplit + math.min(i, remainder)
      val count = partitionsPerSplit + (if (i < remainder) 1 else 0)
      AerospikePartition(i, begin, count)
    }.toArray
  }
}

/** Aerospike RDD with at-least-once semantics: if a scan fails partway through and is retried,
  * records already consumed before the failure may be re-scanned. The generation counter filters
  * most duplicates from the queue, but records already yielded to Spark before the retry are not
  * tracked. Downstream consumers should be idempotent or deduplicate by key.
  */
private[migrator] class AerospikeRDD(
  sc: SparkContext,
  connConfig: AerospikeConnectionConfig,
  readConfig: AerospikeReadConfig,
  broadcastCredentials: Broadcast[Option[(String, String)]],
  broadcastBinNames: Broadcast[IndexedSeq[String]],
  broadcastSchema: Broadcast[StructType],
  recordsReadAccumulator: LongAccumulator
) extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] =
    AerospikeRDD.computePartitions(readConfig.splitCount).asInstanceOf[Array[Partition]]

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val asPart = split.asInstanceOf[AerospikePartition]
    val schema = broadcastSchema.value
    val binNames = broadcastBinNames.value
    // Prefer environment variables on executors to avoid relying on broadcast credentials
    val credentials = Aerospike.resolveCredentialsFromEnvOr(broadcastCredentials.value)
    val connectionKey = AerospikeConnectionKey.fromConfig(connConfig, credentials)
    val client = AerospikeClientHolder.get(connectionKey, connConfig, credentials)
    val scanPolicy = new ScanPolicy()
    connConfig.socketTimeoutMs.foreach(t => scanPolicy.socketTimeout = t)
    connConfig.totalTimeoutMs.foreach(t => scanPolicy.totalTimeout = t)

    // Use a blocking queue to stream records from the scan callback to the iterator.
    // The scan runs on a background thread and the iterator drains the queue.
    // ScanComplete signals scan completion; ScanData carries data; ScanRetry unblocks the iterator.
    // The generation counter allows the iterator to discard stale records from a
    // previous scan attempt after a retry — preventing duplicate rows.
    val queue = new LinkedBlockingQueue[ScanQueueEntry](readConfig.queueSize)
    val scanError = new AtomicReference[Throwable](null)
    val scanGeneration = new AtomicInteger(0)
    // Cooperative cancellation flag: set by TaskCompletionListener so the scan thread
    // stops enqueuing records when Spark's limit()/take() terminates the task early.
    val cancelled = new AtomicBoolean(false)

    val scanThread = new Thread(() =>
      try {
        val binOpt: Option[Seq[String]] = if (binNames.nonEmpty) Some(binNames) else None
        var retryCount = 0
        var scanDone = false
        while (!scanDone) {
          val gen = scanGeneration.get()
          var loggedQueueFull = false
          val callback = new ScanCallback {
            override def scanCallback(key: Key, record: Record): Unit = {
              if (cancelled.get() || Thread.currentThread().isInterrupted)
                throw new ScanCancelledException
              if (queue.remainingCapacity() == 0) {
                if (!loggedQueueFull) {
                  loggedQueueFull = true
                  Aerospike.log.debug(
                    s"Partition ${asPart.index}: scan queue full (size=${readConfig.queueSize}), " +
                      "scan thread will block. Consider increasing 'queueSize' if this happens frequently."
                  )
                }
              } else {
                loggedQueueFull = false
              }
              queue.put(ScanData(gen, key, record))
            }
          }
          // Create a fresh PartitionFilter for each attempt. PartitionFilter is stateful —
          // it tracks which partitions have been scanned. Reusing it after a failed scan
          // may skip partitions or re-scan them inconsistently.
          val partitionFilter = PartitionFilter.range(asPart.partBegin, asPart.partCount)
          try {
            Aerospike.runScanPartitions(
              client,
              scanPolicy,
              partitionFilter,
              readConfig.namespace,
              readConfig.set,
              callback,
              binOpt
            )
            scanDone = true
          } catch {
            case e: AerospikeException
                if Aerospike.isRetryable(e) && retryCount < readConfig.maxScanRetries =>
              retryCount += 1
              Aerospike.log.warn(
                s"Partition ${asPart.index}: retryable scan error (code ${e.getResultCode}), " +
                  s"attempt $retryCount/${readConfig.maxScanRetries}",
                e
              )
              // Advance generation so the iterator discards records from the failed scan,
              // then clear the queue to make room for the retry's records.
              val staleGen = scanGeneration.get()
              scanGeneration.incrementAndGet()
              queue.clear()
              // Unblock the iterator thread if it's stuck in poll() — the stale-generation
              // entry will be discarded by the generation filter.
              queue.put(ScanRetry(staleGen))
              // Exponential backoff: 2s, 4s, 8s with default maxScanRetries=3.
              // Cap at 30s for users who configure higher retry counts.
              val backoffMs = math.min(1000L * (1L << retryCount), 30000L)
              Thread.sleep(backoffMs)
          }
        }
      } catch {
        case _: ScanCancelledException =>
          // Expected on task cancellation (limit()/take()), don't log as error
          Aerospike.log.debug(s"Partition ${asPart.index}: scan cancelled")
        case _: InterruptedException if cancelled.get() =>
          // Backoff sleep interrupted due to task cancellation — treat as normal shutdown
          Aerospike.log.debug(s"Partition ${asPart.index}: scan interrupted during cancellation")
        case e: Throwable =>
          Aerospike.log.error(s"Partition ${asPart.index}: scan thread failed", e)
          scanError.set(e)
          e match {
            case _: VirtualMachineError | _: ThreadDeath | _: LinkageError =>
              // Fatal JVM error — re-throw so the uncaught exception handler can act.
              // The error is also recorded in scanError for the iterator thread.
              throw e
            case _ => // non-fatal, recorded in scanError for the iterator to surface
          }
      } finally {
        // Drain the queue on error so the sentinel can always be posted, preventing
        // the scan thread from hanging when the iterator is not consuming records.
        if (scanError.get() != null) queue.clear()
        queue.put(ScanComplete)
      }
    )
    scanThread.setName(s"aerospike-scan-p${asPart.index}")
    scanThread.setDaemon(true)
    scanThread.start()

    context.addTaskCompletionListener[Unit] { _ =>
      cancelled.set(true)
      scanThread.interrupt()
    }

    val keyType =
      schema.fields.find(_.name == Aerospike.KeyColumnName).map(_.dataType).getOrElse(StringType)
    val fieldCount = schema.fields.length

    new Iterator[Row] {
      private var recordCount = 0L
      private var localAccumulatorCount = 0L
      // Fixed 100K interval; keeps log noise manageable for large datasets
      private val LogInterval = 100000L
      private val AccumulatorFlushInterval = 1000L
      private var completionLogged = false
      // Pre-allocate to avoid repeated Array construction; clone() on each row
      // is still needed since Spark may retain references to returned Rows.
      private val values = new Array[Any](fieldCount)

      private val maxConsecutiveTimeouts = readConfig.maxPollRetries

      private def pollQueue(): Option[(Key, Record)] = {
        val currentGen = scanGeneration.get()
        // Two-phase polling: try a short initial poll, then the remainder of the timeout.
        // The early phase (capped at 10s) allows logging a "still waiting" message before
        // the full timeout elapses. Only splits the timeout when > 20s to avoid degenerate cases.
        val totalPollSeconds = readConfig.pollTimeoutSeconds.toLong
        val earlyPollSeconds = if (totalPollSeconds > 20) 10L else totalPollSeconds
        var consecutiveTimeouts = 0
        while (true) {
          val earlyResult = queue.poll(earlyPollSeconds, TimeUnit.SECONDS)
          val raw = if (earlyResult == null && totalPollSeconds > earlyPollSeconds) {
            Aerospike.log.info(
              s"Partition ${asPart.index}: still waiting for scan records " +
                s"(${totalPollSeconds}s timeout)"
            )
            queue.poll(totalPollSeconds - earlyPollSeconds, TimeUnit.SECONDS)
          } else earlyResult
          raw match {
            case null =>
              consecutiveTimeouts += 1
              if (
                consecutiveTimeouts >= maxConsecutiveTimeouts / 2 && consecutiveTimeouts < maxConsecutiveTimeouts
              )
                Aerospike.log.warn(
                  s"Partition ${asPart.index}: $consecutiveTimeouts/$maxConsecutiveTimeouts consecutive poll timeouts " +
                    s"(${consecutiveTimeouts * readConfig.pollTimeoutSeconds}s total). " +
                    "Consider increasing 'pollTimeoutSeconds' or 'maxPollRetries' if the Aerospike scan is slow."
                )
              else
                Aerospike.log.debug(
                  s"Partition ${asPart.index}: poll timeout after ${readConfig.pollTimeoutSeconds}s, checking for errors " +
                    s"(consecutive timeouts: $consecutiveTimeouts/$maxConsecutiveTimeouts)"
                )
              if (consecutiveTimeouts >= maxConsecutiveTimeouts)
                throw new RuntimeException(
                  s"Aerospike scan timed out: no records after ${consecutiveTimeouts * readConfig.pollTimeoutSeconds}s"
                )
              val ex = scanError.get()
              if (ex != null)
                throw new RuntimeException(
                  s"Aerospike scan failed (${ex.getClass.getSimpleName}): ${ex.getMessage}",
                  ex
                )
              if (!scanThread.isAlive) {
                // Thread finished — try one final poll in case sentinel was just added
                queue.poll() match {
                  case null | ScanComplete =>
                    throw new RuntimeException(
                      "Aerospike scan thread died without signaling completion"
                    )
                  case ScanData(gen, key, record) if gen == currentGen =>
                    return Some((key, record))
                  case _ => // stale generation or retry sentinel, continue polling
                }
              }
            case ScanComplete => return None // sentinel: scan complete
            case ScanData(gen, key, record) if gen == currentGen =>
              consecutiveTimeouts = 0
              return Some((key, record))
            case _ => // stale generation or retry sentinel, discard
          }
        }
        None // unreachable — satisfies compiler
      }

      private var current: Option[(Key, Record)] = pollQueue()

      override def hasNext: Boolean = {
        if (current.isDefined) return true
        val ex = scanError.get()
        if (ex != null)
          throw new RuntimeException(
            s"Aerospike scan failed (${ex.getClass.getSimpleName}): ${ex.getMessage}",
            ex
          )
        if (recordCount > 0 && !completionLogged) {
          if (localAccumulatorCount > 0) {
            recordsReadAccumulator.add(localAccumulatorCount)
            localAccumulatorCount = 0
          }
          Aerospike.log.info(
            s"Partition ${asPart.index}: completed, processed $recordCount records"
          )
          completionLogged = true
        }
        false
      }

      override def next(): Row = {
        val (key, record) = current.get

        var i = 0
        while (i < fieldCount) {
          val field = schema.fields(i)
          values(i) =
            if (field.name == Aerospike.KeyColumnName)
              AerospikeTypes.extractKey(key, keyType)
            else if (field.name == Aerospike.TtlColumnName) {
              // Normalize: getTimeToLive() returns 0 for records without expiration in
              // some Aerospike client versions. Use -1 consistently for "no expiration".
              val ttl = record.getTimeToLive()
              (if (ttl <= 0) -1 else ttl).asInstanceOf[Any]
            } else if (field.name == Aerospike.GenerationColumnName)
              record.generation.asInstanceOf[Any]
            else
              AerospikeTypes.convertValue(record.bins.get(field.name), field.dataType)
          i += 1
        }

        recordCount += 1
        localAccumulatorCount += 1
        if (localAccumulatorCount >= AccumulatorFlushInterval) {
          recordsReadAccumulator.add(localAccumulatorCount)
          localAccumulatorCount = 0
        }
        if (recordCount % LogInterval == 0)
          Aerospike.log.info(
            s"Partition ${asPart.index}: processed $recordCount records"
          )

        current = pollQueue()
        new GenericRow(values.clone())
      }
    }
  }
}
