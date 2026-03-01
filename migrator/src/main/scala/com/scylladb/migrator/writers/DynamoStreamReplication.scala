package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, DynamoUtils, StreamPollerOps }
import com.scylladb.migrator.config.{ SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util.concurrent.{ CountDownLatch, ExecutorService, Executors, ThreadFactory, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext

object DynamoStreamReplication {
  private val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  /** Default maximum consecutive polling failures before stopping stream replication. */
  private val defaultMaxConsecutiveErrors = 50

  /** Default lease duration in milliseconds. */
  private val defaultLeaseDurationMs = 60000L

  /** Build a deterministic checkpoint table name for the given source settings. Includes a hash of
    * the endpoint and region to avoid collisions when migrating tables with the same name from
    * different endpoints.
    */
  /** Compute a short hex hash (4 bytes / 8 hex chars) of the given input using SHA-256. */
  private def shortHash(input: String): String = {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    digest.digest(input.getBytes("UTF-8")).take(4).map(b => f"${b & 0xff}%02x").mkString
  }

  /** DynamoDB table names must be 3-255 characters. */
  private val maxTableNameLength = 255

  private[writers] def buildCheckpointTableName(src: SourceSettings.DynamoDB): String = {
    val endpointStr = src.endpoint.map(_.renderEndpoint).getOrElse("")
    val regionStr = src.region.getOrElse("")
    val suffix = if (endpointStr.nonEmpty || regionStr.nonEmpty) {
      s"_${shortHash(endpointStr + "|" + regionStr)}"
    } else ""
    val prefix = "migrator_"
    // DynamoDB table names allow only [a-zA-Z0-9_.-]
    val sanitizedTable = src.table.replaceAll("[^a-zA-Z0-9_.-]", "_")
    val maxSourceTableLen = maxTableNameLength - prefix.length - suffix.length
    val truncatedTable =
      if (sanitizedTable.length > maxSourceTableLen) {
        // Include a hash of the full table name to avoid collisions when two
        // tables share a long prefix that would produce the same truncated name.
        val tableHash = shortHash(src.table)
        val tableHashSuffix = s"_$tableHash"
        sanitizedTable.take(maxSourceTableLen - tableHashSuffix.length) + tableHashSuffix
      } else sanitizedTable
    s"$prefix$truncatedTable$suffix"
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

    /** Guard against concurrent stop() calls (e.g. explicit stop + shutdown hook race). */
    private val stopped = new AtomicBoolean(false)

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
      if (!stopped.compareAndSet(false, true)) return
      // Deregister the JVM shutdown hook to prevent double-stop
      shutdownHook.foreach { hook =>
        try Runtime.getRuntime.removeShutdownHook(hook)
        catch { case _: IllegalStateException => () } // JVM already shutting down
      }
      // Release all owned leases first so they're instantly claimable by other
      // workers, before waiting for executors to terminate (which can take up
      // to 70s and risks being force-killed during JVM shutdown).
      executors.scheduler.shutdown()
      executors.leaseRenewalScheduler.shutdown()
      // Shut down the polling pool early so in-flight futures (from a running
      // pollAndProcess on the scheduler thread) get interrupted promptly rather
      // than blocking Await.result for up to pollFutureTimeoutSeconds (60s).
      executors.pollingPool.shutdownNow()
      for (shardId <- ownedShards())
        try {
          leaseId.checkpointMgr.releaseLease(
            leaseId.checkpointClient,
            leaseId.checkpointTableName,
            shardId,
            leaseId.workerId
          )
          log.info(s"Released lease for shard $shardId on shutdown")
        } catch {
          case e: Exception =>
            log.warn(s"Failed to release lease for shard $shardId on shutdown", e)
        }
      // Wait for in-flight work to complete; force-stop if timeout expires
      if (!executors.scheduler.awaitTermination(30, TimeUnit.SECONDS))
        executors.scheduler.shutdownNow()
      if (!executors.leaseRenewalScheduler.awaitTermination(10, TimeUnit.SECONDS))
        executors.leaseRenewalScheduler.shutdownNow()
      if (!executors.pollingPool.awaitTermination(10, TimeUnit.SECONDS))
        executors.pollingPool.shutdownNow()
      // Close SDK clients to release HTTP connection pools
      clients.foreach { c =>
        try c.close()
        catch { case e: Exception => log.warn("Error closing client", e) }
      }
      latch.countDown()
    }
  }

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

    // Validate configuration values
    require(
      leaseDurationMs > 0,
      s"streamingLeaseDurationMs must be positive, got $leaseDurationMs"
    )
    require(
      maxConsecutiveErrors > 0,
      s"streamingMaxConsecutiveErrors must be positive, got $maxConsecutiveErrors"
    )
    require(
      pollFutureTimeoutSeconds > 0,
      s"streamingPollFutureTimeoutSeconds must be positive, got $pollFutureTimeoutSeconds"
    )
    maxRecordsPerPoll.foreach { v =>
      require(v > 0, s"streamingMaxRecordsPerPoll must be positive, got $v")
    }
    maxRecordsPerSecond.foreach { v =>
      require(v > 0, s"streamingMaxRecordsPerSecond must be positive, got $v")
    }
    src.streamApiCallTimeoutSeconds.foreach { v =>
      require(v > 0, s"streamApiCallTimeoutSeconds must be positive, got $v")
    }
    src.streamApiCallAttemptTimeoutSeconds.foreach { v =>
      require(v > 0, s"streamApiCallAttemptTimeoutSeconds must be positive, got $v")
    }
    src.streamingPollingPoolSize.foreach { v =>
      require(v > 0, s"streamingPollingPoolSize must be positive, got $v")
    }

    val metrics = new StreamMetrics(
      src.table,
      src.region,
      enableCloudWatch,
      src.finalCredentials.map(_.toProvider)
    )

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
        src.streamApiCallTimeoutSeconds,
        src.streamApiCallAttemptTimeoutSeconds
      )

    val targetClient =
      DynamoUtils.buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        Seq.empty
      )

    // All clients are created; wrap the rest in try/catch to close them on failure.
    val allClients: Seq[AutoCloseable] = Seq(sourceClient, streamsClient, targetClient, metrics)
    try
      startStreamingWithClients(
        src,
        target,
        targetTableDesc,
        renamesMap,
        poller,
        checkpointMgr,
        sourceClient,
        streamsClient,
        targetClient,
        metrics,
        allClients,
        batchIntervalSeconds,
        maxConsecutiveErrors,
        leaseDurationMs,
        maxRecordsPerPoll,
        maxRecordsPerSecond,
        pollFutureTimeoutSeconds
      )
    catch {
      case e: Exception =>
        allClients.foreach { c =>
          try c.close()
          catch { case ex: Exception => log.warn("Error closing client during cleanup", ex) }
        }
        throw e
    }
  }

  private def startStreamingWithClients(
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    poller: StreamPollerOps,
    checkpointMgr: CheckpointManager,
    sourceClient: DynamoDbClient,
    streamsClient: DynamoDbStreamsClient,
    targetClient: DynamoDbClient,
    metrics: StreamMetrics,
    allClients: Seq[AutoCloseable],
    batchIntervalSeconds: Int,
    maxConsecutiveErrors: Int,
    leaseDurationMs: Long,
    maxRecordsPerPoll: Option[Int],
    maxRecordsPerSecond: Option[Int],
    pollFutureTimeoutSeconds: Int
  ): StreamHandle = {

    val streamArn = poller.getStreamArn(sourceClient, src.table)
    log.info(s"Stream ARN: $streamArn")

    val workerId = {
      val host =
        try {
          val hostname = java.net.InetAddress.getLocalHost.getHostName
          // Hash hostname to avoid leaking infrastructure names in shared checkpoint table
          shortHash(hostname)
        } catch { case _: Exception => "unknown" }
      s"$host-${java.util.UUID.randomUUID()}"
    }
    log.info(s"Worker ID: $workerId")

    val checkpointTableName = buildCheckpointTableName(src)
    // Intentionally alias sourceClient: the checkpoint table lives in the same
    // account/region as the source table. The client is closed once via the
    // `clients` seq passed to StreamHandle (no double-close).
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
          for (shardId <- worker.ownedShardIds)
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
      allClients,
      leaseIdentity,
      () => worker.ownedShardIds
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

}
