package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, DynamoUtils, StreamPollerOps }
import com.scylladb.migrator.config.{ SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DynamoDbException,
  Record,
  TableDescription,
  WriteRequest
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util
import java.util.concurrent.{ CountDownLatch, ExecutorService, Executors, ThreadFactory, TimeUnit }
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  private val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  type DynamoItem = util.Map[String, AttributeValue]

  // --- Delegating accessors for backward compatibility with tests ---
  // These delegate to BatchWriter so existing test code continues to work.

  private[writers] val operationTypeColumn: String = BatchWriter.operationTypeColumn
  private[writers] val putOperation: AttributeValue = BatchWriter.putOperation
  private[writers] val deleteOperation: AttributeValue = BatchWriter.deleteOperation
  private[writers] val batchWriteItemLimit: Int = BatchWriter.batchWriteItemLimit

  /** Default maximum consecutive polling failures before stopping stream replication. */
  private val defaultMaxConsecutiveErrors = 50

  /** Backoff parameters for pollShard retries: initial delay, max delay. */
  private val pollShardInitialBackoffMs = 200L
  private val pollShardMaxBackoffMs = 5000L

  /** DynamoDB Streams error codes that are safe to retry with backoff. */
  private val retryableErrorCodes = Set(
    "LimitExceededException",
    "InternalServerError",
    "ProvisionedThroughputExceededException"
  )

  /** Default lease duration in milliseconds. */
  private val defaultLeaseDurationMs = 60000L

  // --- Delegating accessors for backward compatibility with tests ---
  // These delegate to DefaultCheckpointManager so existing test code
  // (e.g., DynamoStreamReplication.createCheckpointTable(...)) continues to work.

  private[writers] val leaseKeyColumn: String = DefaultCheckpointManager.leaseKeyColumn
  private[writers] val shardEndSentinel: String = DefaultCheckpointManager.shardEndSentinel

  private[writers] def createCheckpointTable(client: DynamoDbClient, tableName: String): Unit =
    DefaultCheckpointManager.createCheckpointTable(client, tableName)

  private[writers] def getCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String
  ): Option[String] =
    DefaultCheckpointManager.getCheckpoint(client, tableName, shardId)

  private[writers] def isParentDrained(
    client: DynamoDbClient,
    tableName: String,
    parentShardId: Option[String]
  ): Boolean =
    DefaultCheckpointManager.isParentDrained(client, tableName, parentShardId)

  private[writers] def tryClaimShard(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    leaseDurationMs: Long = defaultLeaseDurationMs,
    parentShardId: Option[String] = None
  ): Option[Option[String]] =
    DefaultCheckpointManager.tryClaimShard(
      client,
      tableName,
      shardId,
      workerId,
      leaseDurationMs,
      parentShardId
    )

  private[writers] def renewLeaseAndCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    maybeSeqNum: Option[String],
    leaseDurationMs: Long = defaultLeaseDurationMs
  ): Boolean =
    DefaultCheckpointManager.renewLeaseAndCheckpoint(
      client,
      tableName,
      shardId,
      workerId,
      maybeSeqNum,
      leaseDurationMs
    )

  private[writers] def requestLeaseTransfer(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    requestingWorkerId: String
  ): Boolean =
    DefaultCheckpointManager.requestLeaseTransfer(client, tableName, shardId, requestingWorkerId)

  private[writers] def releaseLease(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String
  ): Unit =
    DefaultCheckpointManager.releaseLease(client, tableName, shardId, workerId)

  private[writers] def retryRandom[T](
    expression: => T,
    numRetriesLeft: Int,
    maxBackOffMillis: Int,
    random: scala.util.Random = scala.util.Random
  ): T =
    DefaultCheckpointManager.retryRandom(expression, numRetriesLeft, maxBackOffMillis, random)

  /** Build a deterministic checkpoint table name for the given source settings. Includes a hash of
    * the endpoint and region to avoid collisions when migrating tables with the same name from
    * different endpoints.
    */
  private[writers] def buildCheckpointTableName(src: SourceSettings.DynamoDB): String = {
    val endpointStr = src.endpoint.map(_.renderEndpoint).getOrElse("")
    val regionStr = src.region.getOrElse("")
    val suffix = if (endpointStr.nonEmpty || regionStr.nonEmpty) {
      val digest = java.security.MessageDigest.getInstance("SHA-256")
      val hashBytes = digest.digest((endpointStr + "|" + regionStr).getBytes("UTF-8"))
      val hash = hashBytes.take(4).map(b => f"${b & 0xff}%02x").mkString
      s"_$hash"
    } else ""
    s"migrator_${src.table}$suffix"
  }

  /** Groups the thread pools used by stream replication for cleaner lifecycle management. */
  private[writers] case class ExecutorResources(
    scheduler: ExecutorService,
    pollingPool: ExecutorService,
    leaseRenewalScheduler: ExecutorService
  )

  /** Groups the checkpoint/lease identity needed to release leases on shutdown. */
  private[writers] case class LeaseIdentity(
    checkpointClient: DynamoDbClient,
    checkpointTableName: String,
    workerId: String,
    checkpointMgr: CheckpointManager
  )

  /** Handle for managing the lifecycle of stream replication. */
  class StreamHandle(
    executors: ExecutorResources,
    latch: CountDownLatch,
    clients: Seq[AutoCloseable],
    leaseId: LeaseIdentity,
    ownedShards: () => Set[String]
  ) {

    /** JVM shutdown hook reference, set after construction so it can be deregistered on stop(). */
    @volatile private[writers] var shutdownHook: Option[Thread] = None

    /** Block until stream replication stops due to sustained errors. */
    def awaitTermination(): Unit = latch.await()

    /** Block until stream replication stops, with a timeout. Returns true if terminated, false if
      * timed out.
      */
    def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
      latch.await(timeout, unit)

    /** Gracefully stop stream replication. Releases all owned leases so other workers can
      * immediately claim them without waiting for lease expiry.
      */
    def stop(): Unit = {
      // Deregister the JVM shutdown hook to prevent double-stop
      shutdownHook.foreach { hook =>
        try Runtime.getRuntime.removeShutdownHook(hook)
        catch { case _: IllegalStateException => () } // JVM already shutting down
      }
      executors.scheduler.shutdown()
      executors.leaseRenewalScheduler.shutdown()
      executors.pollingPool.shutdown()
      // Wait for in-flight work to complete; force-stop if timeout expires
      if (!executors.scheduler.awaitTermination(30, TimeUnit.SECONDS))
        executors.scheduler.shutdownNow()
      if (!executors.leaseRenewalScheduler.awaitTermination(10, TimeUnit.SECONDS))
        executors.leaseRenewalScheduler.shutdownNow()
      if (!executors.pollingPool.awaitTermination(30, TimeUnit.SECONDS))
        executors.pollingPool.shutdownNow()
      // Release all owned leases so they're instantly claimable
      for (shardId <- ownedShards()) {
        leaseId.checkpointMgr.releaseLease(
          leaseId.checkpointClient,
          leaseId.checkpointTableName,
          shardId,
          leaseId.workerId
        )
        log.info(s"Released lease for shard $shardId on shutdown")
      }
      // Close SDK clients to release HTTP connection pools
      clients.foreach { c =>
        try c.close()
        catch { case e: Exception => log.warn("Error closing client", e) }
      }
      latch.countDown()
    }
  }

  private[writers] def run(
    items: Iterable[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription,
    client: DynamoDbClient
  ): Unit =
    BatchWriter.run(items, target, renamesMap, targetTableDesc, client)

  private[writers] def flushBatch(
    client: DynamoDbClient,
    tableName: String,
    batch: util.ArrayList[WriteRequest],
    batchKeys: util.HashSet[String]
  ): Unit =
    BatchWriter.flushBatch(client, tableName, batch, batchKeys)

  /** Start streaming replication from a DynamoDB stream. Returns a [[StreamHandle]] for lifecycle
    * management.
    */
  def startStreaming(
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    poller: StreamPollerOps = DynamoStreamPoller,
    checkpointMgr: CheckpointManager = DefaultCheckpointManager
  ): StreamHandle = {
    val batchIntervalSeconds =
      math.max(1, src.streamingPollIntervalSeconds.getOrElse(5))
    val maxConsecutiveErrors =
      src.streamingMaxConsecutiveErrors.getOrElse(defaultMaxConsecutiveErrors)
    val leaseDurationMs =
      src.streamingLeaseDurationMs.getOrElse(defaultLeaseDurationMs)
    val maxRecordsPerPoll = src.streamingMaxRecordsPerPoll
    val maxRecordsPerSecond = src.streamingMaxRecordsPerSecond
    val pollFutureTimeoutSeconds = src.streamingPollFutureTimeoutSeconds.getOrElse(60)
    val enableCloudWatch =
      src.streamingEnableCloudWatchMetrics.getOrElse(false)

    val metrics = new StreamMetrics(src.table, src.region, enableCloudWatch)

    val sourceClient =
      DynamoUtils.buildDynamoClient(
        src.endpoint,
        src.finalCredentials.map(_.toProvider),
        src.region,
        Seq.empty
      )
    val streamsClient =
      DynamoUtils.buildDynamoStreamsClient(
        src.endpoint,
        src.finalCredentials.map(_.toProvider),
        src.region,
        src.streamApiCallTimeoutSeconds.getOrElse(30),
        src.streamApiCallAttemptTimeoutSeconds.getOrElse(10)
      )

    val targetClient =
      DynamoUtils.buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        Seq.empty
      )

    val streamArn = poller.getStreamArn(sourceClient, src.table)
    log.info(s"Stream ARN: $streamArn")

    val workerId = {
      val host =
        try java.net.InetAddress.getLocalHost.getHostName
        catch { case _: Exception => "unknown" }
      s"$host-${java.util.UUID.randomUUID()}"
    }
    log.info(s"Worker ID: $workerId")

    val checkpointTableName = buildCheckpointTableName(src)
    val checkpointClient = sourceClient
    checkpointMgr.createCheckpointTable(checkpointClient, checkpointTableName)
    log.info(s"Checkpoint table: $checkpointTableName")

    val pollingPool = Executors.newFixedThreadPool(
      src.streamingPollingPoolSize.getOrElse(
        math.max(4, Runtime.getRuntime.availableProcessors())
      )
    )
    implicit val pollingEc: ExecutionContext =
      ExecutionContext.fromExecutorService(pollingPool)

    val terminationLatch = new CountDownLatch(1)

    val worker = new StreamReplicationWorker(
      streamsClient            = streamsClient,
      targetClient             = targetClient,
      checkpointClient         = checkpointClient,
      target                   = target,
      targetTableDesc          = targetTableDesc,
      renamesMap               = renamesMap,
      poller                   = poller,
      checkpointMgr            = checkpointMgr,
      streamArn                = streamArn,
      workerId                 = workerId,
      checkpointTableName      = checkpointTableName,
      batchIntervalSeconds     = batchIntervalSeconds,
      maxConsecutiveErrors     = maxConsecutiveErrors,
      leaseDurationMs          = leaseDurationMs,
      maxRecordsPerPoll        = maxRecordsPerPoll,
      maxRecordsPerSecond      = maxRecordsPerSecond,
      pollFutureTimeoutSeconds = pollFutureTimeoutSeconds,
      metrics                  = metrics,
      terminationLatch         = terminationLatch
    )

    val leaseRenewalScheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "dynamo-lease-renewer")
          t.setDaemon(true)
          t
        }
      }
    )
    val renewalIntervalMs = leaseDurationMs / 3

    leaseRenewalScheduler.scheduleWithFixedDelay(
      () =>
        try
          for (shardId <- worker.shardIterators.keySet().asScala)
            try
              checkpointMgr.renewLeaseAndCheckpoint(
                checkpointClient,
                checkpointTableName,
                shardId,
                workerId,
                maybeSeqNum = None,
                leaseDurationMs
              )
            catch {
              case e: Exception =>
                log.debug(s"Background lease renewal failed for shard $shardId", e)
            }
        catch {
          case e: Exception =>
            log.warn("Error in background lease renewal", e)
        },
      renewalIntervalMs,
      renewalIntervalMs,
      TimeUnit.MILLISECONDS
    )

    val scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "dynamo-stream-poller")
          t.setDaemon(true)
          t
        }
      }
    )
    scheduler.scheduleWithFixedDelay(
      () => worker.pollAndProcess(),
      0L,
      batchIntervalSeconds.toLong,
      TimeUnit.SECONDS
    )

    val executorResources = ExecutorResources(scheduler, pollingPool, leaseRenewalScheduler)
    val leaseIdentity =
      LeaseIdentity(checkpointClient, checkpointTableName, workerId, checkpointMgr)

    val handle = new StreamHandle(
      executorResources,
      terminationLatch,
      Seq(sourceClient, streamsClient, targetClient, metrics),
      leaseIdentity,
      () => worker.shardIterators.keySet().asScala.toSet
    )

    val hook = new Thread(s"dynamo-stream-shutdown-$workerId") {
      override def run(): Unit = {
        log.info("JVM shutdown hook triggered, stopping stream replication")
        handle.stop()
      }
    }
    Runtime.getRuntime.addShutdownHook(hook)
    handle.shutdownHook = Some(hook)

    handle
  }

  /** Poll a single shard with retry and exponential backoff for rate-limiting errors.
    *
    * Note: uses `Thread.sleep` for retry backoff (200ms-5000ms), blocking the calling pool thread.
    * This is acceptable because the pool is sized for parallel shard polling and the sleep
    * durations are short. Converting to async would require `Future`-based composition throughout
    * the polling pipeline.
    */
  private[writers] def pollShard(
    streamsClient: DynamoDbStreamsClient,
    shardId: String,
    iterator: String,
    maxRetries: Int = 3,
    poller: StreamPollerOps = DynamoStreamPoller,
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
        Thread.sleep(backoffMs)
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
