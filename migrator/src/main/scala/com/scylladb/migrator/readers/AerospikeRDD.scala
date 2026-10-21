package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeClient, AerospikeException, Key, Record, ScanCallback }
import com.aerospike.client.policy.ScanPolicy
import com.scylladb.migrator.config.AerospikeInvalidRecordHandling
import com.aerospike.client.query.PartitionFilter
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

import java.util.concurrent.{ LinkedBlockingQueue, Phaser, TimeUnit }
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

/** Bounded scan queue whose producers can stop even when Spark no longer drains records. */
private[migrator] class CancellableScanQueue(capacity: Int, cancelled: AtomicBoolean)
    extends LinkedBlockingQueue[ScanQueueEntry](capacity) {
  private val producers = new Phaser(1)

  def enqueue(entry: ScanQueueEntry): Unit = {
    producers.register()
    try {
      var offered = false
      while (!offered) {
        if (cancelled.get() || Thread.currentThread().isInterrupted)
          throw new ScanCancelledException
        offered = offer(entry, 100, TimeUnit.MILLISECONDS)
      }
    } finally producers.arriveAndDeregister()
  }

  def stopProducers(): Unit = {
    cancelled.set(true)
    // The client may return after its first worker fails, before other callbacks have exited.
    // Wait without reacting to the coordinator's cancellation interrupt.
    producers.arriveAndAwaitAdvance()
  }
}

private[migrator] object AerospikeRDD {

  /** Transfer the acquired reference to the producer only after its thread starts successfully. */
  private[migrator] def startScanThread(
    connectionKey: AerospikeConnectionKey,
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  )(createThread: AerospikeClient => Thread): Thread = {
    val client = AerospikeClientHolder.acquire(connectionKey, connConfig, credentials)
    try {
      val thread = createThread(client)
      thread.start()
      thread
    } catch {
      case error: Throwable =>
        try AerospikeClientHolder.releaseOne(connectionKey)
        catch { case _: Throwable => () }
        throw error
    }
  }

  private[migrator] def shouldRetryScan(
    error: AerospikeException,
    retryCount: Int,
    maxRetries: Int,
    cancelled: Boolean
  ): Boolean = !cancelled && Aerospike.isRetryable(error) && retryCount < maxRetries

  private[migrator] def buildScanPolicy(
    connConfig: AerospikeConnectionConfig,
    includeBinData: Boolean
  ): ScanPolicy = {
    val policy = new ScanPolicy()
    // The RDD owns the configured retry budget; SDK retries would multiply that budget.
    policy.maxRetries = 0
    connConfig.socketTimeoutMs.foreach(t => policy.socketTimeout = t)
    connConfig.totalTimeoutMs.foreach(t => policy.totalTimeout = t)
    policy.includeBinData = includeBinData
    policy
  }

  /** Keep a poll that overlaps retry backoff out of the consecutive idle-timeout budget. */
  private[migrator] def pollWithRetryAllowance(
    retryInProgress: AtomicBoolean,
    scanGeneration: AtomicInteger
  )(poll: => ScanQueueEntry): (ScanQueueEntry, Boolean) = {
    val generationAtStart = scanGeneration.get()
    val retryingAtStart = retryInProgress.get()
    val entry = poll
    val retryOverlapped = retryingAtStart || retryInProgress.get() ||
      generationAtStart != scanGeneration.get()
    (entry, retryOverlapped)
  }

  /** Validate source identity and each declared bin before submitting a row to Spark. */
  private[migrator] def recordToRow(
    key: Key,
    record: Record,
    schema: StructType,
    readConfig: AerospikeReadConfig,
    nowMillis: Long,
    onInvalid: String => Unit = _ => (),
    onMissing: String => Unit = _ => (),
    onInvalidRecord: () => Unit = () => ()
  ): Option[Row] = {
    var invalidRecord = false
    def invalid(reason: String): Unit = {
      if (!invalidRecord) {
        invalidRecord = true
        onInvalidRecord()
      }
      onInvalid(reason)
    }
    val identity =
      try AerospikeTypes.extractKey(key, readConfig.keyMode)
      catch {
        case error: AerospikeTypes.InvalidValue =>
          invalid(error.getMessage)
          return None
      }
    val values = new Array[Any](schema.length)
    var index = 0
    while (index < schema.length) {
      val field = schema(index)
      if (field.name == readConfig.keyColumnName)
        values(index) = identity
      else if (readConfig.ttlColumnName.contains(field.name))
        values(index) = remainingTtl(record.expiration, nowMillis)
      else if (readConfig.generationColumnName.contains(field.name))
        values(index) = record.generation
      else {
        val raw = if (record.bins != null) record.bins.get(field.name) else null
        if (raw == null && (record.bins == null || !record.bins.containsKey(field.name)))
          onMissing(field.name)
        try values(index) = AerospikeTypes.convertValue(raw, field.dataType)
        catch {
          case error: AerospikeTypes.InvalidValue =>
            invalid(s"Invalid Aerospike bin '${field.name}': ${error.getMessage}")
            if (readConfig.invalidRecordHandling == AerospikeInvalidRecordHandling.Skip)
              return None
            values(index) = null
        }
      }
      index += 1
    }
    Some(new GenericRow(values))
  }

  /** Bounded observations for one Spark partition attempt, including any scan retries. */
  private[migrator] class MissingBins(partitionIndex: Int) {
    private var count = 0L
    private val examples = scala.collection.mutable.LinkedHashSet.empty[String]

    def observe(name: String): Unit = {
      count += 1
      if (examples.size < 5) examples += name.take(80)
      if (count == 1)
        Aerospike.log.warn(
          s"Partition $partitionIndex: declared Aerospike bin '${name.take(80)}' is absent; " +
            "projecting null. Sparse records are valid; check schema bin spelling and case if unexpected. " +
            "Further missing-bin observations are summarized at partition completion."
        )
    }

    def summary: String =
      s"$count missing declared bin observations" +
        (if (examples.isEmpty) "" else s" (example names: ${examples.mkString(", ")})")
  }

  /** Count each consumed record before conversion and cache one valid row. Returning a row never
    * polls for the following record.
    */
  private[migrator] def validRows[A](
    poll: () => Option[A],
    convert: A => Option[Row],
    onComplete: () => Unit = () => (),
    onRead: () => Unit = () => ()
  ): Iterator[Row] = new Iterator[Row] {
    private var cached: Option[Row] = None
    private var completed = false

    override def hasNext: Boolean = {
      while (cached.isEmpty && !completed)
        poll() match {
          case Some(record) =>
            onRead()
            cached = convert(record)
          case None =>
            onComplete()
            completed = true
        }
      cached.isDefined
    }

    override def next(): Row = {
      if (!hasNext) throw new NoSuchElementException("Aerospike scan exhausted")
      val row = cached.get
      cached = None
      row
    }
  }

  /** Aerospike expiration is unsigned seconds since 2010-01-01, with zero meaning no expiry. Clamp
    * remaining seconds to the INT metadata column's range instead of overflowing.
    */
  private[migrator] def remainingTtl(expiration: Int, nowMillis: Long): Int =
    if (expiration == 0) -1
    else {
      val nowSeconds = nowMillis / 1000L - 1262304000L
      val remaining = java.lang.Integer.toUnsignedLong(expiration) - nowSeconds
      math.min(Int.MaxValue.toLong, math.max(0L, remaining)).toInt
    }

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
  recordsReadAccumulator: LongAccumulator,
  invalidRecordsAccumulator: LongAccumulator,
  skippedRecordsAccumulator: LongAccumulator
) extends RDD[Row](sc, Nil) {

  protected def scanPartitions(
    client: AerospikeClient,
    policy: ScanPolicy,
    filter: PartitionFilter,
    callback: ScanCallback,
    bins: Option[Seq[String]]
  ): Unit =
    Aerospike.runScanPartitions(
      client,
      policy,
      filter,
      readConfig.namespace,
      readConfig.set,
      callback,
      bins
    )

  override def getPartitions: Array[Partition] =
    AerospikeRDD.computePartitions(readConfig.splitCount).asInstanceOf[Array[Partition]]

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val asPart = split.asInstanceOf[AerospikePartition]
    val schema = broadcastSchema.value
    val binNames = broadcastBinNames.value
    // Prefer environment variables on executors to avoid relying on broadcast credentials
    val credentials = Aerospike.resolveCredentialsFromEnvOr(broadcastCredentials.value)
    val connectionKey = AerospikeConnectionKey.fromConfig(connConfig, credentials)
    val scanPolicy = AerospikeRDD.buildScanPolicy(
      connConfig,
      binNames.nonEmpty
    )

    // Use a blocking queue to stream records from the scan callback to the iterator.
    // The scan runs on a background thread and the iterator drains the queue.
    // ScanComplete signals scan completion; ScanData carries data; ScanRetry unblocks the iterator.
    // The generation counter allows the iterator to discard stale records from a
    // failed attempt that are still queued. Rows already returned to Spark can replay after a retry.
    // Set on task completion so callbacks stop even on separate Aerospike worker threads.
    val cancelled = new AtomicBoolean(false)
    val queue = new CancellableScanQueue(readConfig.queueSize, cancelled)
    val scanError = new AtomicReference[Throwable](null)
    val scanGeneration = new AtomicInteger(0)
    // True while the scan thread is backing off before a retry, so the iterator does not
    // mistake the (intentional) gap in records for a fatal poll timeout.
    val retryInProgress = new AtomicBoolean(false)

    val scanThread = AerospikeRDD.startScanThread(connectionKey, connConfig, credentials) {
      client =>
        val thread = new Thread(() =>
          try {
            val binOpt: Option[Seq[String]] =
              if (binNames.isEmpty) None else Some(binNames)
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
                      if (Aerospike.log.isDebugEnabled)
                        Aerospike.log.debug(
                          s"Partition ${asPart.index}: scan queue full (size=${readConfig.queueSize}), " +
                            "scan thread will block. Consider increasing 'queueSize' if this happens frequently."
                        )
                    }
                  } else {
                    loggedQueueFull = false
                  }
                  queue.enqueue(ScanData(gen, key, record))
                }
              }
              // Create a fresh PartitionFilter for each attempt. PartitionFilter is stateful —
              // it tracks which partitions have been scanned. Reusing it after a failed scan
              // may skip partitions or re-scan them inconsistently.
              val partitionFilter = PartitionFilter.range(asPart.partBegin, asPart.partCount)
              try {
                scanPartitions(client, scanPolicy, partitionFilter, callback, binOpt)
                scanDone = true
              } catch {
                case e: AerospikeException
                    if AerospikeRDD.shouldRetryScan(
                      e,
                      retryCount,
                      readConfig.maxScanRetries,
                      cancelled.get()
                    ) =>
                  retryCount += 1
                  Aerospike.log.warn(
                    s"Partition ${asPart.index}: retryable scan error (code ${e.getResultCode}), " +
                      s"attempt $retryCount/${readConfig.maxScanRetries}",
                    e
                  )
                  // Advance generation so the iterator discards records from the failed scan,
                  // then clear the queue to make room for the retry's records.
                  // Mark backoff before waking the iterator, so its next poll observes the gap.
                  retryInProgress.set(true)
                  val staleGen = scanGeneration.get()
                  scanGeneration.incrementAndGet()
                  queue.clear()
                  // Unblock the iterator thread if it's stuck in poll() — the stale-generation
                  // entry will be discarded by the generation filter.
                  queue.enqueue(ScanRetry(staleGen))
                  // Exponential backoff: 2s, 4s, 8s with default maxScanRetries=3.
                  // Cap at 30s for users who configure higher retry counts.
                  val backoffMs = math.min(1000L * (1L << math.min(retryCount, 20)), 30000L)
                  try Thread.sleep(backoffMs)
                  finally retryInProgress.set(false)
              }
            }
          } catch {
            case _: ScanCancelledException =>
              // Expected on task cancellation (limit()/take()), don't log as error
              if (Aerospike.log.isDebugEnabled)
                Aerospike.log.debug(s"Partition ${asPart.index}: scan cancelled")
            case _: InterruptedException if cancelled.get() =>
              // Backoff sleep interrupted due to task cancellation — treat as normal shutdown
              if (Aerospike.log.isDebugEnabled)
                Aerospike.log.debug(
                  s"Partition ${asPart.index}: scan interrupted during cancellation"
                )
            case _: AerospikeException if cancelled.get() =>
              // Multi-node scans wrap callback cancellation in AerospikeException.
              if (Aerospike.log.isDebugEnabled)
                Aerospike.log.debug(s"Partition ${asPart.index}: scan cancelled")
            case e: Throwable =>
              scanError.set(e)
              // Diagnostics must not hide the scan failure from the iterator or uncaught handler.
              try Aerospike.log.error(s"Partition ${asPart.index}: scan thread failed", e)
              catch { case _: Throwable => }
              e match {
                case _: VirtualMachineError | _: ThreadDeath | _: LinkageError =>
                  // Fatal JVM error — re-throw so the uncaught exception handler can act.
                  // The error is also recorded in scanError for the iterator thread.
                  throw e
                case _ => // non-fatal, recorded in scanError for the iterator to surface
              }
          } finally
            try {
              if (scanError.get() != null) queue.clear()
              try queue.enqueue(ScanComplete)
              catch {
                case _: ScanCancelledException                  =>
                case _: InterruptedException if cancelled.get() =>
              }
            } finally {
              queue.stopProducers()
              AerospikeClientHolder.releaseOne(connectionKey)
            }
        )
        thread.setName(s"aerospike-scan-p${asPart.index}")
        thread.setDaemon(true)
        thread
    }

    context.addTaskCompletionListener[Unit] { _ =>
      cancelled.set(true)
      scanThread.interrupt()
    }

    val rows = {
      var recordCount = 0L
      var localAccumulatorCount = 0L
      def flushReadCount(): Unit = {
        if (localAccumulatorCount > 0) recordsReadAccumulator.add(localAccumulatorCount)
        localAccumulatorCount = 0L
      }
      context.addTaskCompletionListener[Unit] { task =>
        if (!task.isFailed() && !task.isInterrupted()) flushReadCount()
      }
      // Fixed 100K interval; keeps log noise manageable for large datasets
      val LogInterval = 100000L
      val AccumulatorFlushInterval = 1000L
      var invalidCount = 0L
      var skippedCount = 0L
      val missingBins = new AerospikeRDD.MissingBins(asPart.index)
      val maxConsecutiveTimeouts = readConfig.maxPollRetries

      def pollQueue(): Option[(Key, Record)] = {
        var currentGen = scanGeneration.get()
        // Two-phase polling: try a short initial poll, then the remainder of the timeout.
        // The early phase (capped at 10s) allows logging a "still waiting" message before
        // the full timeout elapses. Only splits the timeout when > 20s to avoid degenerate cases.
        val totalPollSeconds = readConfig.pollTimeoutSeconds.toLong
        val earlyPollSeconds = if (totalPollSeconds > 20) 10L else totalPollSeconds
        var consecutiveTimeouts = 0
        while (true) {
          val (raw, retrying) =
            AerospikeRDD.pollWithRetryAllowance(retryInProgress, scanGeneration) {
              val earlyResult = queue.poll(earlyPollSeconds, TimeUnit.SECONDS)
              if (earlyResult == null && totalPollSeconds > earlyPollSeconds) {
                Aerospike.log.info(
                  s"Partition ${asPart.index}: still waiting for scan records " +
                    s"(${totalPollSeconds}s timeout)"
                )
                queue.poll(totalPollSeconds - earlyPollSeconds, TimeUnit.SECONDS)
              } else earlyResult
            }
          raw match {
            case null =>
              if (retrying) consecutiveTimeouts = 0 else consecutiveTimeouts += 1
              if (
                consecutiveTimeouts >= maxConsecutiveTimeouts / 2 && consecutiveTimeouts < maxConsecutiveTimeouts
              )
                Aerospike.log.warn(
                  s"Partition ${asPart.index}: $consecutiveTimeouts/$maxConsecutiveTimeouts consecutive poll timeouts " +
                    s"(${consecutiveTimeouts * readConfig.pollTimeoutSeconds}s total). " +
                    "Consider increasing 'pollTimeoutSeconds' or 'maxPollRetries' if the Aerospike scan is slow."
                )
              else if (Aerospike.log.isDebugEnabled)
                Aerospike.log.debug(
                  s"Partition ${asPart.index}: poll timeout after ${readConfig.pollTimeoutSeconds}s, checking for errors " +
                    s"(consecutive timeouts: $consecutiveTimeouts/$maxConsecutiveTimeouts)"
                )
              if (!retrying && consecutiveTimeouts >= maxConsecutiveTimeouts)
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
                  case ScanComplete => return None // normal completion sentinel
                  case null =>
                    throw new RuntimeException(
                      "Aerospike scan thread died without signaling completion"
                    )
                  case ScanData(gen, key, record) if gen == currentGen =>
                    return Some((key, record))
                  case ScanRetry(_) =>
                    currentGen = scanGeneration.get()
                  case _ => // stale generation, continue polling
                }
              }
            case ScanComplete => return None // sentinel: scan complete
            case ScanData(gen, key, record) if gen == currentGen =>
              consecutiveTimeouts = 0
              return Some((key, record))
            case ScanRetry(_) =>
              // A retry advanced the generation; adopt it so the new attempt's records are kept,
              // and reset the timeout counter since a retry is legitimate progress, not a stall.
              currentGen          = scanGeneration.get()
              consecutiveTimeouts = 0
            case _ => // stale generation, discard
          }
        }
        None // unreachable — satisfies compiler
      }

      AerospikeRDD.validRows[(Key, Record)](
        () => pollQueue(),
        { case (key, record) =>
          val row = AerospikeRDD.recordToRow(
            key,
            record,
            schema,
            readConfig,
            System.currentTimeMillis(),
            reason => {
              invalidCount += 1
              if (invalidCount == 1 || invalidCount % 10000 == 0)
                Aerospike.log.warn(
                  s"Partition ${asPart.index}: $invalidCount invalid source values; $reason"
                )
            },
            missingBins.observe,
            () => invalidRecordsAccumulator.add(1L)
          )
          if (row.isEmpty) {
            skippedCount += 1
            skippedRecordsAccumulator.add(1L)
          }
          row
        },
        () => {
          val ex = scanError.get()
          if (ex != null)
            throw new RuntimeException(
              s"Aerospike scan failed (${ex.getClass.getSimpleName}): ${ex.getMessage}",
              ex
            )
          flushReadCount()
          Aerospike.log.info(
            s"Partition ${asPart.index}: completed, read $recordCount records, $invalidCount invalid source values, $skippedCount skipped records; ${missingBins.summary}"
          )
        },
        () => {
          recordCount += 1
          localAccumulatorCount += 1
          if (localAccumulatorCount >= AccumulatorFlushInterval) {
            flushReadCount()
          }
          if (recordCount % LogInterval == 0)
            Aerospike.log.info(s"Partition ${asPart.index}: read $recordCount records")
        }
      )
    }
    rows
  }
}
