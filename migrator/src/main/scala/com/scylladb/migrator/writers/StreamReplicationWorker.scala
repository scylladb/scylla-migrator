package com.scylladb.migrator.writers

import com.scylladb.migrator.StreamPollerOps
import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DynamoDbException,
  Record,
  ShardIteratorType,
  TableDescription
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** Encapsulates the mutable state and polling logic for stream replication.
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
  private[writers] val shardIterators = new ConcurrentHashMap[String, String]()
  private var shardSequenceNumbers = Map.empty[String, String]
  private var cachedShards = Seq.empty[software.amazon.awssdk.services.dynamodb.model.Shard]
  private var shardListRefreshCounter = 0
  private var lastShardListRefreshMs = 0L
  private var closedShardAbsentCycles = Map.empty[String, Int]

  // -- Error tracking --
  private var consecutiveErrors = 0

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

  /** The main polling loop, called periodically by the scheduler. */
  def pollAndProcess(): Unit =
    try {
      val cycleStartMs = System.currentTimeMillis()

      refillRateLimitTokens()

      // Step 1: Poll all owned shards in parallel.
      val pollResults = pollOwnedShards()

      // Process poll results
      val (allItems, closedShards) = processPollResults(pollResults)

      // Step 2: Discover new shards and try to claim unclaimed/expired ones.
      discoverAndClaimShards(closedShards)

      // Step 2b: Clean up old checkpoint rows
      cleanupClosedShards(closedShards)

      // Reset error counter on success
      if (consecutiveErrors > 0) {
        log.info(s"Recovered after $consecutiveErrors consecutive errors")
        consecutiveErrors = 0
      }

      if (allItems.nonEmpty)
        try
          BatchWriter.run(allItems, target, renamesMap, targetTableDesc, targetClient)
        catch {
          case e: Exception =>
            log.error("Failed to write batch to target, checkpointing already-polled shards", e)
        }

      // Step 4: Save checkpoints after successful writes (or at least polled records).
      checkpointAfterWrite()

      // Write SHARD_END sentinel for closed shards
      markClosedShards(closedShards)

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
          terminationLatch.countDown()
        } else if (consecutiveErrors % 10 == 0)
          log.error(s"Sustained polling failures ($consecutiveErrors consecutive)", e)
        else
          log.warn(s"Error polling DynamoDB stream (failure $consecutiveErrors)", e)
    }

  private def refillRateLimitTokens(): Unit =
    maxRecordsPerSecond.foreach { maxRate =>
      val now = System.currentTimeMillis()
      val elapsed = now - lastRateLimitRefill
      rateLimitTokens = math.min(
        maxRate.toLong,
        rateLimitTokens + (maxRate.toLong * elapsed / 1000L)
      )
      lastRateLimitRefill = now
    }

  private def pollOwnedShards(): Seq[(String, Seq[Record], Option[String])] =
    if (!shardIterators.isEmpty) {
      val futures =
        shardIterators.asScala.toSeq.map { case (shardId, iterator) =>
          Future(
            DynamoStreamReplication.pollShard(
              streamsClient,
              shardId,
              iterator,
              poller            = poller,
              maxRecordsPerPoll = maxRecordsPerPoll
            )
          ).recover {
            case e: DynamoDbException
                if e.awsErrorDetails() != null &&
                  e.awsErrorDetails().errorCode() == "ExpiredIteratorException" =>
              log.warn(s"Expired iterator for shard $shardId, refreshing from checkpoint")
              val refreshedIter = shardSequenceNumbers.get(shardId) match {
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
            case e: Exception =>
              log.warn(s"Failed to poll shard $shardId: ${e.getMessage}")
              (shardId, Seq.empty[Record], None)
          }
        }
      val combined = Future.sequence(futures)
      try Await.result(combined, Duration(pollFutureTimeoutSeconds, TimeUnit.SECONDS))
      catch {
        case e: Exception =>
          log.warn(s"Poll timed out for some shards: ${e.getMessage}")
          futures.flatMap(_.value.collect { case scala.util.Success(result) => result })
      }
    } else Seq.empty

  private type DynamoItem = java.util.Map[String, AttributeValue]

  private def processPollResults(
    pollResults: Seq[(String, Seq[Record], Option[String])]
  ): (
    scala.collection.mutable.ArrayBuffer[Option[DynamoItem]],
    scala.collection.mutable.Set[String]
  ) = {
    val totalRecords = pollResults.map(_._2.size).sum
    val allItems =
      new scala.collection.mutable.ArrayBuffer[Option[DynamoItem]](totalRecords)
    val updatedIterators =
      scala.collection.mutable.Map.empty[String, String]
    val updatedSeqNums =
      scala.collection.mutable.Map[String, String](shardSequenceNumbers.toSeq: _*)

    val closedShards = scala.collection.mutable.Set.empty[String]
    for ((shardId, records, nextIter) <- pollResults) {
      for (record <- records) {
        allItems += poller.recordToItem(
          record,
          BatchWriter.operationTypeColumn,
          BatchWriter.putOperation,
          BatchWriter.deleteOperation
        )
        val seqNum = record.dynamodb().sequenceNumber()
        if (seqNum != null)
          updatedSeqNums(shardId) = seqNum
      }
      nextIter match {
        case Some(next) => updatedIterators(shardId) = next
        case None       => closedShards += shardId
      }
    }

    // Replace shardIterators contents: remove closed shards, update iterators
    shardIterators.clear()
    updatedIterators.foreach { case (k, v) => shardIterators.put(k, v) }
    shardSequenceNumbers = updatedSeqNums.toMap

    (allItems, closedShards)
  }

  private def discoverAndClaimShards(
    closedShards: scala.collection.mutable.Set[String]
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
    closedShards: scala.collection.mutable.Set[String]
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
        checkpointClient.deleteItem(
          software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
            .builder()
            .tableName(checkpointTableName)
            .key(
              Map(checkpointMgr.leaseKeyColumn -> AttributeValue.fromS(shardId)).asJava
            )
            .build()
        )
        log.info(s"Cleaned up checkpoint row for closed shard $shardId")
      } catch {
        case e: Exception =>
          log.warn(s"Failed to clean up checkpoint for shard $shardId", e)
      }
    closedShardAbsentCycles = closedShardAbsentCycles -- toCleanup
  }

  private def checkpointAfterWrite(): Unit =
    for (shardId <- shardIterators.keySet().asScala)
      shardSequenceNumbers.get(shardId).foreach { seqNum =>
        checkpointMgr.renewLeaseAndCheckpoint(
          checkpointClient,
          checkpointTableName,
          shardId,
          workerId,
          Some(seqNum),
          leaseDurationMs
        )
      }

  private def markClosedShards(
    closedShards: scala.collection.mutable.Set[String]
  ): Unit =
    for (shardId <- closedShards) {
      log.info(s"Shard $shardId closed, writing ${checkpointMgr.shardEndSentinel} checkpoint")
      checkpointMgr.renewLeaseAndCheckpoint(
        checkpointClient,
        checkpointTableName,
        shardId,
        workerId,
        Some(checkpointMgr.shardEndSentinel),
        leaseDurationMs
      )
      shardSequenceNumbers = shardSequenceNumbers - shardId
    }

  private def applyRateLimiting(
    allItems: scala.collection.mutable.ArrayBuffer[Option[DynamoItem]]
  ): Unit = {
    val recordsThisCycle = allItems.count(_.isDefined)
    if (maxRecordsPerSecond.isDefined)
      rateLimitTokens -= recordsThisCycle
    if (rateLimitTokens < 0 && maxRecordsPerSecond.isDefined) {
      val sleepMs = (-rateLimitTokens * 1000) / maxRecordsPerSecond.get
      log.info(
        s"Rate limiting: sleeping ${sleepMs}ms to stay within ${maxRecordsPerSecond.get} records/s"
      )
      Thread.sleep(sleepMs)
    }
  }

  private def updateMetrics(
    allItems: scala.collection.mutable.ArrayBuffer[Option[DynamoItem]],
    pollResults: Seq[(String, Seq[Record], Option[String])],
    cycleElapsedMs: Long
  ): Unit = {
    if (cycleElapsedMs > batchIntervalSeconds * 1000L) {
      log.warn(
        s"Poll cycle took ${cycleElapsedMs}ms, exceeding the ${batchIntervalSeconds}s " +
          "poll interval — consumer is falling behind"
      )
    }

    val recordsThisCycle = allItems.count(_.isDefined)
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
}
