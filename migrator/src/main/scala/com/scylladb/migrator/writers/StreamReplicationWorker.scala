package com.scylladb.migrator.writers

import com.scylladb.migrator.StreamPollerOps
import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  DynamoDbException,
  Record,
  ShardIteratorType,
  TableDescription
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Encapsulates the mutable state and polling logic for stream replication.
  *
  * ==Thread-safety model==
  *
  * This class participates in three threads:
  *
  *   1. '''Scheduler thread''' (`dynamo-stream-poller`): Owns all mutable `var` fields and drives
  *      the [[pollAndProcess]] loop. All field reads and writes to `shardSequenceNumbers`,
  *      `cachedShards`, `closedShardAbsentCycles`, `consecutiveErrors`, rate-limit state, and
  *      observability counters happen exclusively on this thread.
  *   2. '''Lease-renewal thread''' (`dynamo-lease-renewer`): Periodically iterates
  *      `shardIterators.keySet()` to renew leases. It only reads the key set; it never mutates
  *      iterators, sequence numbers, or any `var` fields.
  *   3. '''Polling pool threads''' (`pollingEc`): Execute individual shard polls via `Future`. They
  *      access only immutable snapshot data captured in closures and do not touch any `var` fields.
  *
  * `shardIterators` is a [[java.util.concurrent.ConcurrentHashMap]] specifically to allow
  * cross-thread reads by the lease-renewal thread and the `ownedShards` closure used during
  * shutdown, while writes happen on the scheduler thread.
  *
  * Checkpoint and lease operations are delegated to the injected [[CheckpointManager]], decoupling
  * this class from the concrete DynamoDB checkpoint implementation and enabling isolated testing.
  */
private[writers] class StreamReplicationWorker(
  streamsClient: DynamoDbStreamsClient,
  targetClient: DynamoDbClient,
  checkpointClient: DynamoDbClient,
  target: TargetSettings.DynamoDB,
  targetTableDesc: TableDescription,
  renamesMap: Map[String, String],
  poller: StreamPollerOps,
  checkpointMgr: CheckpointManager,
  streamArn: String,
  workerId: String,
  checkpointTableName: String,
  batchIntervalSeconds: Int,
  maxConsecutiveErrors: Int,
  leaseDurationMs: Long,
  maxRecordsPerPoll: Option[Int],
  maxRecordsPerSecond: Option[Int],
  pollFutureTimeoutSeconds: Int,
  metrics: StreamMetrics,
  terminationLatch: CountDownLatch
)(implicit pollingEc: ExecutionContext) {

  private val log = LogManager.getLogger("com.scylladb.migrator.writers.StreamReplicationWorker")

  // ---------------------------------------------------------------------------
  // Mutable state — all vars below are confined to the single scheduler thread,
  // except `shardIterators` which is a ConcurrentHashMap for cross-thread reads
  // by the lease-renewal thread and the `ownedShards` closure.
  // ---------------------------------------------------------------------------

  // -- Shard tracking --
  private val shardIterators = new ConcurrentHashMap[String, String]()

  /** Returns the set of shard IDs currently owned by this worker. Thread-safe: reads from the
    * ConcurrentHashMap.
    */
  private[writers] def ownedShardIds: Set[String] =
    shardIterators.keySet().asScala.toSet
  private var shardSequenceNumbers = Map.empty[String, String]
  private var cachedShards = Seq.empty[software.amazon.awssdk.services.dynamodb.model.Shard]
  private var shardListRefreshCounter = 0
  private var lastShardListRefreshMs = 0L
  private var closedShardAbsentCycles = Map.empty[String, Int]

  // -- Error tracking --
  private var consecutiveErrors = 0
  @volatile private var terminated = false

  // -- Rate limiter (token bucket) --
  private var rateLimitTokens = maxRecordsPerSecond.getOrElse(Int.MaxValue).toLong
  private var lastRateLimitRefill = System.currentTimeMillis()

  // -- Observability counters --
  private var totalRecordsProcessed = 0L
  private var pollCycleCount = 0L

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  private val shardCleanupThresholdCycles = 10
  private val shardListRefreshInterval = 6
  private val minShardRefreshIntervalMs = 5000L
  private val metricsPublishIntervalCycles = 60

  /** The main polling loop, called periodically by the scheduler.
    *
    * Each invocation performs the following steps:
    *   1. Refill rate-limit tokens based on elapsed time.
    *   2. Poll all owned shards in parallel via the polling thread pool.
    *   3. Process poll results: collect items, update shard iterators, identify closed shards.
    *   4. Discover new/unclaimed shards and attempt to claim expired leases.
    *   5. Clean up checkpoint rows for shards absent from the stream for several cycles.
    *   6. Write items to the target table via [[BatchWriter]].
    *   7. Checkpoint sequence numbers (only if the write succeeded, to prevent data loss).
    *   8. Mark closed shards with the `SHARD_END` sentinel.
    *   9. Update rate-limit token balance and publish metrics.
    *
    * '''Error handling:''' Exceptions from individual shard polls are caught per-shard and do not
    * abort the cycle. A write failure skips checkpointing so records will be re-polled. Unhandled
    * exceptions increment `consecutiveErrors`; after `maxConsecutiveErrors` the worker signals
    * termination via the latch.
    */
  def pollAndProcess(): Unit = {
    if (terminated) return
    try {
      val cycleStartMs = System.currentTimeMillis()

      refillRateLimitTokens()

      // Skip polling when rate limit tokens are depleted; tokens will recover
      // via refillRateLimitTokens() on the next cycle.
      if (maxRecordsPerSecond.isDefined && rateLimitTokens <= 0) {
        log.debug(
          s"Rate limit enforced: skipping poll cycle (token deficit: ${-rateLimitTokens})"
        )
        return
      }

      // Step 1: Poll all owned shards in parallel.
      val pollResults = pollOwnedShards()

      // Snapshot sequence numbers so we can revert on write failure.
      // This prevents failed records' sequence numbers from leaking into
      // checkpoint writes on subsequent empty-record cycles.
      val prevSeqNums = shardSequenceNumbers

      // Process poll results
      val (allItems, closedShards) = processPollResults(pollResults)

      // Step 2: Discover new shards and try to claim unclaimed/expired ones.
      discoverAndClaimShards(closedShards)

      // Step 2b: Clean up old checkpoint rows
      cleanupClosedShards(closedShards)

      var writeSucceeded = true
      if (allItems.nonEmpty)
        try
          BatchWriter.run(allItems, target, renamesMap, targetTableDesc, targetClient)
        catch {
          case e: BatchWriteExhaustedException =>
            writeSucceeded = false
            metrics.writeFailures.incrementAndGet()
            metrics.deadLetterItems.addAndGet(e.unprocessedCount.toLong)
            log.error("Failed to write batch to target, skipping checkpoint to avoid data loss", e)
          case e: Exception =>
            writeSucceeded = false
            metrics.writeFailures.incrementAndGet()
            log.error("Failed to write batch to target, skipping checkpoint to avoid data loss", e)
        }

      // Step 4: Save checkpoints only after successful writes to avoid skipping failed records.
      if (writeSucceeded) {
        checkpointAfterWrite()

        // Write SHARD_END sentinel for closed shards
        markClosedShards(closedShards)
      } else {
        // Revert sequence numbers so the failed batch's positions aren't
        // persisted in a future empty-record cycle's checkpoint write.
        shardSequenceNumbers = prevSeqNums
      }

      // Reset error counter only after ALL steps (poll, write, checkpoint) succeeded.
      // This ensures that sustained write failures accumulate and eventually trigger
      // termination, rather than being masked by successful polls.
      if (!writeSucceeded) {
        consecutiveErrors += 1
        if (consecutiveErrors >= maxConsecutiveErrors) {
          log.error(
            s"$maxConsecutiveErrors consecutive failures (including write errors), " +
              "stopping stream replication"
          )
          terminated = true
          terminationLatch.countDown()
          return
        } else if (consecutiveErrors % 10 == 0)
          log.warn(s"Sustained failures ($consecutiveErrors consecutive, latest: write failure)")
      } else if (consecutiveErrors > 0) {
        log.info(s"Recovered after $consecutiveErrors consecutive errors")
        consecutiveErrors = 0
      }

      // Rate limiting
      applyRateLimiting(allItems)

      // Observability
      val cycleElapsedMs = System.currentTimeMillis() - cycleStartMs
      updateMetrics(allItems, pollResults, cycleElapsedMs)
    } catch {
      case e: Exception =>
        consecutiveErrors += 1
        if (consecutiveErrors >= maxConsecutiveErrors) {
          log.error(
            s"$maxConsecutiveErrors consecutive polling failures, stopping stream replication",
            e
          )
          terminated = true
          terminationLatch.countDown()
        } else if (consecutiveErrors % 10 == 0)
          log.error(s"Sustained polling failures ($consecutiveErrors consecutive)", e)
        else
          log.warn(s"Error polling DynamoDB stream (failure $consecutiveErrors)", e)
    }
  }

  private def refillRateLimitTokens(): Unit =
    maxRecordsPerSecond.foreach { maxRate =>
      val now = System.currentTimeMillis()
      // Cap elapsed to 1 second to prevent overflow in the multiplication
      // when the system was paused for a long time (e.g., GC, debugging).
      val elapsed = math.min(now - lastRateLimitRefill, 1000L)
      rateLimitTokens = math.min(
        maxRate.toLong,
        rateLimitTokens + (maxRate.toLong * elapsed / 1000L)
      )
      lastRateLimitRefill = now
    }

  private def pollOwnedShards(): Seq[(String, Seq[Record], Option[String])] =
    if (!shardIterators.isEmpty) {
      // Snapshot the sequence numbers before dispatching futures so that the
      // recover blocks (which run on pollingEc threads) read an immutable copy
      // rather than the scheduler-owned var.
      val seqNumSnapshot = shardSequenceNumbers
      val futures =
        shardIterators.asScala.toSeq.map { case (shardId, iterator) =>
          Future(
            pollShard(shardId, iterator)
          ).recover {
            case e: DynamoDbException
                if e.awsErrorDetails() != null &&
                  e.awsErrorDetails().errorCode() == "ExpiredIteratorException" =>
              log.warn(s"Expired iterator for shard $shardId, refreshing from checkpoint")
              val refreshedIter = seqNumSnapshot.get(shardId) match {
                case Some(seqNum) =>
                  try
                    poller.getShardIteratorAfterSequence(
                      streamsClient,
                      streamArn,
                      shardId,
                      seqNum
                    )
                  catch {
                    case _: Exception =>
                      poller.getShardIterator(
                        streamsClient,
                        streamArn,
                        shardId,
                        ShardIteratorType.TRIM_HORIZON
                      )
                  }
                case None =>
                  poller.getShardIterator(
                    streamsClient,
                    streamArn,
                    shardId,
                    ShardIteratorType.TRIM_HORIZON
                  )
              }
              (shardId, Seq.empty[Record], Some(refreshedIter))
            case e: DynamoDbException
                if e.awsErrorDetails() != null &&
                  e.awsErrorDetails().errorCode() == "TrimmedDataAccessException" =>
              log.warn(
                s"Trimmed data for shard $shardId (records older than 24h were trimmed), " +
                  "resetting to TRIM_HORIZON"
              )
              val refreshedIter = poller.getShardIterator(
                streamsClient,
                streamArn,
                shardId,
                ShardIteratorType.TRIM_HORIZON
              )
              (shardId, Seq.empty[Record], Some(refreshedIter))
            case e: Exception =>
              log.warn(s"Failed to poll shard $shardId: ${e.getMessage}")
              // Preserve the existing iterator so the shard is not mistakenly
              // treated as closed (nextIterator=None means SHARD_END).
              (shardId, Seq.empty[Record], Some(iterator))
          }
        }
      val combined = Future.sequence(futures)
      try Await.result(combined, Duration(pollFutureTimeoutSeconds, TimeUnit.SECONDS))
      catch {
        case e: Exception =>
          val completed = futures.flatMap(_.value.collect { case scala.util.Success(r) => r })
          val completedShardIds = completed.map(_._1).toSet
          val timedOutShardIds =
            shardIterators.keySet().asScala.toSet -- completedShardIds
          log.warn(
            s"Poll timed out after ${pollFutureTimeoutSeconds}s for shards: " +
              s"${timedOutShardIds.mkString(", ")} (${completed.size}/${futures.size} completed)"
          )
          completed
      }
    } else Seq.empty

  private type DynamoItem = BatchWriter.DynamoItem

  private def processPollResults(
    pollResults: Seq[(String, Seq[Record], Option[String])]
  ): (Seq[DynamoItem], Set[String]) = {
    val totalRecords = pollResults.map(_._2.size).sum
    val allItems =
      new scala.collection.mutable.ArrayBuffer[DynamoItem](totalRecords)
    val updatedIterators =
      scala.collection.mutable.Map.empty[String, String]
    val updatedSeqNums =
      scala.collection.mutable.Map[String, String](shardSequenceNumbers.toSeq: _*)

    val closedShards = scala.collection.mutable.Set.empty[String]
    for ((shardId, records, nextIter) <- pollResults) {
      for (record <- records) {
        poller
          .recordToItem(
            record,
            BatchWriter.operationTypeColumn,
            BatchWriter.putOperation,
            BatchWriter.deleteOperation
          )
          .foreach(allItems += _)
        val seqNum = record.dynamodb().sequenceNumber()
        if (seqNum != null)
          updatedSeqNums(shardId) = seqNum
      }
      nextIter match {
        case Some(next) => updatedIterators(shardId) = next
        case None       => closedShards += shardId
      }
    }

    // Incrementally update shardIterators: remove closed shards, update iterators.
    // Avoids clearing the map (which would cause the lease renewal thread to see
    // an empty map and skip renewals).
    for (shardId <- closedShards)
      shardIterators.remove(shardId)
    updatedIterators.foreach { case (k, v) => shardIterators.put(k, v) }
    shardSequenceNumbers = updatedSeqNums.toMap

    (allItems.toSeq, closedShards.toSet)
  }

  private def discoverAndClaimShards(
    closedShards: Set[String]
  ): Unit = {
    shardListRefreshCounter += 1
    val timeSinceLastRefresh = System.currentTimeMillis() - lastShardListRefreshMs
    if (
      (shardListRefreshCounter >= shardListRefreshInterval ||
        closedShards.nonEmpty || cachedShards.isEmpty) &&
      timeSinceLastRefresh >= minShardRefreshIntervalMs
    ) {
      cachedShards            = poller.listShards(streamsClient, streamArn)
      shardListRefreshCounter = 0
      lastShardListRefreshMs  = System.currentTimeMillis()
    }
    val shards = cachedShards
    for {
      shard <- shards
      if !shardIterators.containsKey(shard.shardId())
    } {
      val parentId = Option(shard.parentShardId()).filter(_.nonEmpty)
      if (!checkpointMgr.isParentDrained(checkpointClient, checkpointTableName, parentId)) {
        log.debug(
          s"Skipping shard ${shard.shardId()}: parent ${parentId.getOrElse("?")} not yet drained"
        )
      } else
        checkpointMgr.tryClaimShard(
          checkpointClient,
          checkpointTableName,
          shard.shardId(),
          workerId,
          leaseDurationMs,
          parentId
        ) match {
          case Some(checkpointOpt) =>
            log.info(
              s"Claimed shard ${shard.shardId()}" +
                checkpointOpt
                  .map(s => s", resuming from checkpoint $s")
                  .getOrElse(", starting fresh")
            )
            val maybeIterator: Option[String] = checkpointOpt match {
              case Some(seqNum) if seqNum != checkpointMgr.shardEndSentinel =>
                shardSequenceNumbers = shardSequenceNumbers + (shard.shardId() -> seqNum)
                val iter =
                  try
                    poller.getShardIteratorAfterSequence(
                      streamsClient,
                      streamArn,
                      shard.shardId(),
                      seqNum
                    )
                  catch {
                    case e: Exception =>
                      log.warn(
                        s"Failed to resume shard ${shard.shardId()} " +
                          s"from checkpoint: ${e.getMessage}, falling back to TRIM_HORIZON"
                      )
                      poller.getShardIterator(
                        streamsClient,
                        streamArn,
                        shard.shardId(),
                        ShardIteratorType.TRIM_HORIZON
                      )
                  }
                Some(iter)
              case Some(_) =>
                log.info(s"Shard ${shard.shardId()} already fully consumed, skipping")
                checkpointMgr.releaseLease(
                  checkpointClient,
                  checkpointTableName,
                  shard.shardId(),
                  workerId
                )
                None
              case None =>
                Some(
                  poller.getShardIterator(
                    streamsClient,
                    streamArn,
                    shard.shardId(),
                    ShardIteratorType.TRIM_HORIZON
                  )
                )
            }
            maybeIterator.foreach { iterator =>
              shardIterators.put(shard.shardId(), iterator)
            }
          case None =>
            () // Another worker owns this shard
        }
    }
  }

  private def cleanupClosedShards(
    closedShards: Set[String]
  ): Unit = {
    val activeShardIds = cachedShards.map(_.shardId()).toSet
    closedShardAbsentCycles = closedShardAbsentCycles.map {
      case (sid, _) if activeShardIds.contains(sid) => sid -> 0
      case (sid, count)                             => sid -> (count + 1)
    }
    for (shardId <- closedShards if !closedShardAbsentCycles.contains(shardId))
      closedShardAbsentCycles = closedShardAbsentCycles + (shardId -> 0)
    val toCleanup = closedShardAbsentCycles.collect {
      case (sid, count) if count >= shardCleanupThresholdCycles => sid
    }
    for (shardId <- toCleanup)
      try {
        val deleted = checkpointMgr.deleteClosedShardCheckpoint(
          checkpointClient,
          checkpointTableName,
          shardId,
          workerId
        )
        if (deleted)
          log.info(s"Cleaned up checkpoint row for closed shard $shardId")
        else
          log.debug(
            s"Skipping cleanup of shard $shardId: condition not met (reclaimed by another worker)"
          )
      } catch {
        case e: Exception =>
          log.warn(s"Failed to clean up checkpoint for shard $shardId", e)
      }
    closedShardAbsentCycles = closedShardAbsentCycles -- toCleanup
  }

  private def checkpointAfterWrite(): Unit = {
    val shardsToCheckpoint = shardIterators.keySet().asScala.toSeq.flatMap { shardId =>
      shardSequenceNumbers.get(shardId).map(shardId -> _)
    }
    if (shardsToCheckpoint.isEmpty) return

    // Run checkpoints sequentially on the scheduler thread to avoid contending
    // with shard polling on the pollingEc pool.  The number of checkpoints per
    // cycle is bounded by the shard count, and each checkpoint is a single
    // conditional DynamoDB write with short retry backoff, so sequential
    // execution does not meaningfully delay the next poll cycle.
    for ((shardId, seqNum) <- shardsToCheckpoint) {
      val leaseHeld =
        try
          checkpointMgr.renewLeaseAndCheckpoint(
            checkpointClient,
            checkpointTableName,
            shardId,
            workerId,
            Some(seqNum),
            leaseDurationMs
          )
        catch {
          case e: Exception =>
            log.warn(s"Checkpoint write failed for shard $shardId: ${e.getMessage}")
            metrics.checkpointFailures.incrementAndGet()
            true // keep tracking the shard; the next cycle will retry
        }
      if (!leaseHeld) {
        metrics.checkpointFailures.incrementAndGet()
        log.warn(s"Lost lease for shard $shardId during checkpoint, removing from tracking")
        shardIterators.remove(shardId)
        shardSequenceNumbers = shardSequenceNumbers - shardId
      }
    }
  }

  private def markClosedShards(
    closedShards: Set[String]
  ): Unit =
    for (shardId <- closedShards)
      try {
        log.info(s"Shard $shardId closed, writing ${checkpointMgr.shardEndSentinel} checkpoint")
        val leaseHeld = checkpointMgr.renewLeaseAndCheckpoint(
          checkpointClient,
          checkpointTableName,
          shardId,
          workerId,
          Some(checkpointMgr.shardEndSentinel),
          leaseDurationMs
        )
        if (!leaseHeld)
          log.warn(s"Lost lease for closed shard $shardId, another worker will handle it")
        shardSequenceNumbers = shardSequenceNumbers - shardId
      } catch {
        case e: Exception =>
          log.warn(s"Failed to mark shard $shardId as closed: ${e.getMessage}")
          metrics.checkpointFailures.incrementAndGet()
      }

  private def applyRateLimiting(
    allItems: Seq[DynamoItem]
  ): Unit = {
    val recordsThisCycle = allItems.size
    if (maxRecordsPerSecond.isDefined) {
      rateLimitTokens -= recordsThisCycle
      // Token deficit is carried forward and recovered via refillRateLimitTokens()
      // on the next cycle, avoiding Thread.sleep on the scheduler thread.
      if (rateLimitTokens < 0)
        log.debug(
          s"Rate limiting: token deficit of ${-rateLimitTokens}, " +
            s"will recover on next cycle (limit: ${maxRecordsPerSecond.get} records/s)"
        )
    }
  }

  private def updateMetrics(
    allItems: Seq[DynamoItem],
    pollResults: Seq[(String, Seq[Record], Option[String])],
    cycleElapsedMs: Long
  ): Unit = {
    if (cycleElapsedMs > batchIntervalSeconds * 1000L) {
      log.warn(
        s"Poll cycle took ${cycleElapsedMs}ms, exceeding the ${batchIntervalSeconds}s " +
          "poll interval — consumer is falling behind"
      )
    }

    val recordsThisCycle = allItems.size
    totalRecordsProcessed += recordsThisCycle
    pollCycleCount += 1
    metrics.recordsProcessed.set(totalRecordsProcessed)
    metrics.pollCycles.set(pollCycleCount)
    metrics.activeShards.set(shardIterators.size().toLong)
    metrics.lastPollDurationMs.set(cycleElapsedMs)

    val shardAges = pollResults.flatMap { case (shardId, records, _) =>
      records.lastOption.flatMap { record =>
        val ts = record.dynamodb().approximateCreationDateTime()
        if (ts != null)
          Some(shardId -> (System.currentTimeMillis() - ts.toEpochMilli))
        else None
      }
    }
    if (shardAges.nonEmpty) {
      val maxAge = shardAges.map(_._2).max
      metrics.maxIteratorAgeMs.set(maxAge)
    }

    if (pollCycleCount % metricsPublishIntervalCycles == 0) {
      val ageInfo =
        if (shardAges.nonEmpty)
          s", max iterator age: ${metrics.maxIteratorAgeMs.get()}ms"
        else ""
      log.info(
        s"Stream replication stats: " +
          s"${totalRecordsProcessed} total records processed, " +
          s"${shardIterators.size()} active shards, " +
          s"${pollCycleCount} poll cycles completed" +
          ageInfo
      )
      metrics.publishToCloudWatch()
    }
  }

  private def pollShard(
    shardId: String,
    iterator: String
  ): (String, Seq[Record], Option[String]) =
    StreamReplicationWorker.pollShard(
      streamsClient,
      shardId,
      iterator,
      poller            = poller,
      maxRecordsPerPoll = maxRecordsPerPoll
    )
}

private[writers] object StreamReplicationWorker {

  private val log = LogManager.getLogger("com.scylladb.migrator.writers.StreamReplicationWorker")

  /** Backoff parameters for pollShard retries: initial delay, max delay. */
  private val pollShardInitialBackoffMs = 200L
  private val pollShardMaxBackoffMs = 5000L
  private val retryableErrorCodes = CheckpointManager.retryableErrorCodes

  /** Poll a single shard with retry and exponential backoff for rate-limiting errors.
    *
    * Note: uses `Thread.sleep` for retry backoff (200ms-5000ms), blocking the calling pool thread.
    * This is acceptable because the pool is sized for parallel shard polling and the sleep
    * durations are short. Converting to async would require `Future`-based composition throughout
    * the polling pipeline.
    */
  // Note: recursive call is inside try/catch which prevents @tailrec optimization.
  // Stack depth is bounded by maxRetries (default 3), so this is safe.
  private[writers] def pollShard(
    streamsClient: DynamoDbStreamsClient,
    shardId: String,
    iterator: String,
    maxRetries: Int = 3,
    poller: StreamPollerOps,
    maxRecordsPerPoll: Option[Int] = None,
    attempt: Int = 0
  ): (String, Seq[Record], Option[String]) =
    try {
      val (records, nextIter) =
        poller.getRecords(streamsClient, iterator, maxRecordsPerPoll)
      (shardId, records, nextIter)
    } catch {
      case e: DynamoDbException
          if e.awsErrorDetails() != null &&
            retryableErrorCodes.contains(
              e.awsErrorDetails().errorCode()
            ) =>
        val nextAttempt = attempt + 1
        if (nextAttempt > maxRetries) throw e
        val backoffMs =
          math.min(pollShardInitialBackoffMs * (1L << nextAttempt), pollShardMaxBackoffMs)
        log.warn(
          s"Retryable error on shard $shardId " +
            s"(attempt $nextAttempt/$maxRetries), " +
            s"backing off ${backoffMs}ms"
        )
        try Thread.sleep(backoffMs)
        catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            throw e
        }
        pollShard(
          streamsClient,
          shardId,
          iterator,
          maxRetries,
          poller,
          maxRecordsPerPoll,
          nextAttempt
        )
    }
}
