package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, DynamoUtils, StreamPollerOps }
import com.scylladb.migrator.config.{ SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BatchWriteItemRequest,
  BillingMode,
  ConditionalCheckFailedException,
  CreateTableRequest,
  DeleteRequest,
  DescribeTableRequest,
  DynamoDbException,
  KeySchemaElement,
  KeyType,
  PutRequest,
  Record,
  ResourceNotFoundException,
  ReturnValue,
  ScalarAttributeType,
  ShardIteratorType,
  TableDescription,
  UpdateItemRequest,
  WriteRequest
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util
import java.util.concurrent.{ CountDownLatch, ExecutorService, Executors, ThreadFactory, TimeUnit }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  type DynamoItem = util.Map[String, AttributeValue]

  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = AttributeValue.fromBool(true)
  private val deleteOperation = AttributeValue.fromBool(false)

  /** Default maximum consecutive polling failures before stopping stream replication. */
  private val defaultMaxConsecutiveErrors = 50

  /** DynamoDB Streams error codes that are safe to retry with backoff. */
  private val retryableErrorCodes = Set(
    "LimitExceededException",
    "InternalServerError",
    "ProvisionedThroughputExceededException"
  )

  /** Column names matching the KCL checkpoint table schema, plus lease columns. */
  private val leaseKeyColumn = "leaseKey"
  private val checkpointColumn = "checkpoint"
  private val leaseOwnerColumn = "leaseOwner"
  private val leaseExpiryColumn = "leaseExpiryEpochMs"

  /** Default lease duration in milliseconds. If a worker doesn't renew within this window, other
    * workers can claim the shard. Set to 60s (12x the default 5s poll interval) to tolerate
    * transient delays without spurious takeover.
    */
  private val defaultLeaseDurationMs = 60000L

  /** Handle for managing the lifecycle of stream replication. */
  class StreamHandle(
    scheduler: ExecutorService,
    pollingPool: ExecutorService,
    latch: CountDownLatch,
    clients: Seq[AutoCloseable]
  ) {

    /** Block until stream replication stops due to sustained errors. */
    def awaitTermination(): Unit = latch.await()

    /** Block until stream replication stops, with a timeout. Returns true if terminated, false if
      * timed out.
      */
    def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
      latch.await(timeout, unit)

    /** Gracefully stop stream replication. */
    def stop(): Unit = {
      scheduler.shutdown()
      pollingPool.shutdown()
      // Wait for in-flight work to complete
      scheduler.awaitTermination(30, TimeUnit.SECONDS)
      pollingPool.awaitTermination(30, TimeUnit.SECONDS)
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
    targetClient: Option[DynamoDbClient] = None
  ): Unit = {
    val msgs = items.flatten
    if (msgs.isEmpty) {
      log.info("No changes to apply")
      return
    }

    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet
    var putCount = 0L
    var deleteCount = 0L

    val client = targetClient.getOrElse(
      DynamoUtils.buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        Seq.empty
      )
    )
    val ownsClient = targetClient.isEmpty
    try {
      // Batch puts and deletes using BatchWriteItem (up to 25 items per call).
      // BatchWriteItem does not guarantee ordering and rejects duplicate keys
      // in a single request, so we flush whenever a key already in the current
      // batch is encountered again.
      val batch = new util.ArrayList[WriteRequest]()
      val batchKeys = new util.HashSet[util.Map[String, AttributeValue]]()

      msgs.foreach { item =>
        val isPut = item.get(operationTypeColumn) == putOperation

        val itemWithoutOp = item.asScala.collect {
          case (k, v) if k != operationTypeColumn => k -> v
        }.asJava

        // Extract the key attributes for duplicate detection
        val keyMap: util.Map[String, AttributeValue] = itemWithoutOp.asScala
          .filter { case (key, _) => keyAttributeNames.contains(key) }
          .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
          .asJava

        // Flush if this key is already in the batch (preserves ordering)
        if (batchKeys.contains(keyMap))
          flushBatch(client, target.table, batch, batchKeys)

        if (isPut) {
          putCount += 1
          val finalItem = itemWithoutOp.asScala.map { case (key, value) =>
            renamesMap.getOrElse(key, key) -> value
          }.asJava
          batch.add(
            WriteRequest
              .builder()
              .putRequest(PutRequest.builder().item(finalItem).build())
              .build()
          )
        } else {
          deleteCount += 1
          batch.add(
            WriteRequest
              .builder()
              .deleteRequest(DeleteRequest.builder().key(keyMap).build())
              .build()
          )
        }
        batchKeys.add(keyMap)

        if (batch.size() >= 25)
          flushBatch(client, target.table, batch, batchKeys)
      }

      if (!batch.isEmpty)
        flushBatch(client, target.table, batch, batchKeys)
    } finally
      if (ownsClient) client.close()

    log.info(s"""
                |Changes applied:
                |  - $putCount items UPSERTED
                |  - $deleteCount items DELETED
                |""".stripMargin)
  }

  /** Flush a batch of write requests using BatchWriteItem, retrying unprocessed items. */
  private def flushBatch(
    client: DynamoDbClient,
    tableName: String,
    batch: util.ArrayList[WriteRequest],
    batchKeys: util.HashSet[util.Map[String, AttributeValue]]
  ): Unit = {
    var unprocessed: util.List[WriteRequest] = batch
    var attempt = 0
    while (!unprocessed.isEmpty) {
      val request = BatchWriteItemRequest
        .builder()
        .requestItems(Map(tableName -> unprocessed).asJava)
        .build()
      try {
        val response = client.batchWriteItem(request)
        val remaining = response.unprocessedItems()
        unprocessed =
          if (remaining != null && remaining.containsKey(tableName))
            remaining.get(tableName)
          else
            util.Collections.emptyList()
        if (!unprocessed.isEmpty) {
          attempt += 1
          if (attempt > 5) {
            log.error(
              s"Giving up on ${unprocessed.size()} unprocessed items after $attempt attempts"
            )
            unprocessed = util.Collections.emptyList()
          } else {
            val backoffMs = math.min(100L * (1L << attempt), 3000L)
            log.warn(
              s"${unprocessed.size()} unprocessed items, retrying after ${backoffMs}ms"
            )
            Thread.sleep(backoffMs)
          }
        }
      } catch {
        case e: Exception =>
          log.error(s"Failed to batch write to $tableName", e)
          unprocessed = util.Collections.emptyList()
      }
    }
    batch.clear()
    batchKeys.clear()
  }

  /** Start streaming replication from a DynamoDB stream. Returns a [[StreamHandle]] for lifecycle
    * management.
    *
    * Multiple runners can share the same checkpoint table and coordinate via shard leasing. Each
    * runner claims a subset of shards; if a runner dies, its leases expire and other runners pick
    * up the orphaned shards.
    */
  def startStreaming(
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    poller: StreamPollerOps = DynamoStreamPoller
  ): StreamHandle = {
    val batchIntervalSeconds =
      src.streamingPollIntervalSeconds.getOrElse(5)
    val maxConsecutiveErrors =
      src.streamingMaxConsecutiveErrors.getOrElse(defaultMaxConsecutiveErrors)
    val leaseDurationMs =
      src.streamingLeaseDurationMs.getOrElse(defaultLeaseDurationMs)
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
        src.region
      )

    // Create target client once and reuse across all polling cycles
    val targetClient =
      DynamoUtils.buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        Seq.empty
      )

    val streamArn = poller.getStreamArn(sourceClient, src.table)
    log.info(s"Stream ARN: $streamArn")

    // Unique worker ID for shard lease ownership
    val workerId = {
      val host =
        try java.net.InetAddress.getLocalHost.getHostName
        catch { case _: Exception => "unknown" }
      s"$host-${java.util.UUID.randomUUID()}"
    }
    log.info(s"Worker ID: $workerId")

    // Shard state tracking
    var shardIterators = Map.empty[String, String]
    var initialized = false
    var consecutiveErrors = 0

    // Deterministic checkpoint table name so multiple runners share it and
    // restarts can resume from the last checkpoint.
    val checkpointTableName = s"migrator_${src.table}"
    val checkpointClient =
      DynamoUtils.buildDynamoClient(
        src.endpoint,
        src.finalCredentials.map(_.toProvider),
        src.region,
        Seq.empty
      )
    createCheckpointTable(checkpointClient, checkpointTableName)
    log.info(s"Checkpoint table: $checkpointTableName")
    var shardSequenceNumbers = Map.empty[String, String]

    // Thread pool for parallel shard polling
    val pollingPool = Executors.newFixedThreadPool(
      src.streamingPollingPoolSize.getOrElse(
        math.max(4, Runtime.getRuntime.availableProcessors())
      )
    )
    implicit val pollingEc: ExecutionContext =
      ExecutionContext.fromExecutorService(pollingPool)

    val terminationLatch = new CountDownLatch(1)

    // Observability counters (Item 10)
    var totalRecordsProcessed = 0L
    var pollCycleCount = 0L

    def pollAndProcess(): Unit =
      try {
        // Step 1: Poll all owned shards in parallel.
        // By polling before discovering new shards, we avoid the race where
        // a shard closes between listShards and getRecords, causing its
        // last records to be missed.
        val pollResults = if (shardIterators.nonEmpty) {
          val futures =
            shardIterators.toSeq.map { case (shardId, iterator) =>
              Future(
                pollShard(streamsClient, shardId, iterator, poller = poller)
              ).recover { case e: Exception =>
                log.warn(
                  s"Failed to poll shard $shardId: ${e.getMessage}"
                )
                (shardId, Seq.empty[Record], None)
              }
            }
          Await.result(
            Future.sequence(futures),
            Duration(60, TimeUnit.SECONDS)
          )
        } else Seq.empty

        // Process poll results
        val allItems =
          scala.collection.mutable.Buffer.empty[Option[DynamoItem]]
        val updatedIterators =
          scala.collection.mutable.Map.empty[String, String]
        val updatedSeqNums =
          scala.collection.mutable.Map[String, String](
            shardSequenceNumbers.toSeq: _*
          )

        for ((shardId, records, nextIter) <- pollResults) {
          for (record <- records) {
            allItems += poller.recordToItem(
              record,
              operationTypeColumn,
              putOperation,
              deleteOperation
            )
            val seqNum = record.dynamodb().sequenceNumber()
            if (seqNum != null)
              updatedSeqNums(shardId) = seqNum
          }
          nextIter match {
            case Some(next) => updatedIterators(shardId) = next
            case None       => // shard closed or poll failed
          }
        }

        shardIterators       = updatedIterators.toMap
        shardSequenceNumbers = updatedSeqNums.toMap

        // Step 2: Renew leases and save checkpoints for all tracked shards.
        val lostShards = scala.collection.mutable.Set.empty[String]
        for (shardId <- shardIterators.keys) {
          val renewed = renewLeaseAndCheckpoint(
            checkpointClient,
            checkpointTableName,
            shardId,
            workerId,
            shardSequenceNumbers.get(shardId),
            leaseDurationMs
          )
          if (!renewed) {
            log.info(
              s"Lost lease for shard $shardId, another worker has taken over"
            )
            lostShards += shardId
          }
        }
        shardIterators       = shardIterators -- lostShards
        shardSequenceNumbers = shardSequenceNumbers -- lostShards

        // Step 3: Discover new shards and try to claim unclaimed/expired ones.
        // This happens after polling so that closing shards have already
        // been read before they disappear from the shard list.
        val shards =
          poller.listShards(streamsClient, streamArn)
        for {
          shard <- shards
          if !shardIterators.contains(shard.shardId())
        }
          tryClaimShard(
            checkpointClient,
            checkpointTableName,
            shard.shardId(),
            workerId,
            leaseDurationMs
          ) match {
            case Some(checkpointOpt) =>
              log.info(
                s"Claimed shard ${shard.shardId()}" +
                  checkpointOpt
                    .map(s => s", resuming from checkpoint $s")
                    .getOrElse(", starting fresh")
              )
              val iterator = checkpointOpt match {
                case Some(seqNum) =>
                  shardSequenceNumbers = shardSequenceNumbers + (shard.shardId() -> seqNum)
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
                          s"from checkpoint: ${e.getMessage}, " +
                          "falling back to TRIM_HORIZON"
                      )
                      poller.getShardIterator(
                        streamsClient,
                        streamArn,
                        shard.shardId(),
                        ShardIteratorType.TRIM_HORIZON
                      )
                  }
                case None =>
                  val iterType =
                    if (initialized) ShardIteratorType.LATEST
                    else ShardIteratorType.TRIM_HORIZON
                  poller.getShardIterator(
                    streamsClient,
                    streamArn,
                    shard.shardId(),
                    iterType
                  )
              }
              shardIterators = shardIterators + (shard.shardId() -> iterator)
            case None =>
              () // Another worker owns this shard
          }
        initialized = true

        // Reset error counter on success
        if (consecutiveErrors > 0) {
          log.info(
            s"Recovered after $consecutiveErrors consecutive errors"
          )
          consecutiveErrors = 0
        }

        if (allItems.nonEmpty)
          run(allItems, target, renamesMap, targetTableDesc, Some(targetClient))

        // Periodic summary stats (every 60 cycles ~= every 5 minutes at default interval)
        val recordsThisCycle = allItems.count(_.isDefined)
        totalRecordsProcessed += recordsThisCycle
        pollCycleCount += 1
        if (pollCycleCount % 60 == 0) {
          log.info(
            s"Stream replication stats: " +
              s"${totalRecordsProcessed} total records processed, " +
              s"${shardIterators.size} active shards, " +
              s"${pollCycleCount} poll cycles completed"
          )
        }
      } catch {
        case e: Exception =>
          consecutiveErrors += 1
          if (consecutiveErrors >= maxConsecutiveErrors) {
            log.error(
              s"$maxConsecutiveErrors consecutive polling failures, " +
                "stopping stream replication",
              e
            )
            terminationLatch.countDown()
          } else if (consecutiveErrors % 10 == 0)
            log.error(
              s"Sustained polling failures " +
                s"($consecutiveErrors consecutive)",
              e
            )
          else
            log.warn(
              s"Error polling DynamoDB stream " +
                s"(failure $consecutiveErrors)",
              e
            )
      }

    // Schedule periodic polling with fixed delay to prevent overlapping.
    // initialDelay=0 fires the first poll on the scheduler thread, avoiding
    // the data race of calling pollAndProcess() from the calling thread.
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
      () => pollAndProcess(),
      0L,
      batchIntervalSeconds.toLong,
      TimeUnit.SECONDS
    )

    new StreamHandle(
      scheduler,
      pollingPool,
      terminationLatch,
      Seq(sourceClient, streamsClient, checkpointClient, targetClient)
    )
  }

  /** Poll a single shard with retry and exponential backoff for rate-limiting errors. DynamoDB
    * Streams limits to 5 GetRecords calls/second/shard; this backoff ensures we respect that limit
    * even under contention.
    *
    * Note: on retry, the same iterator value is reused. For `LimitExceededException` and
    * `ProvisionedThroughputExceededException` this is correct — the iterator wasn't consumed. For
    * `InternalServerError`, the iterator may have been partially consumed; however, DynamoDB
    * Streams guarantees at-least-once delivery, so any missed records will reappear on the next
    * shard iterator obtained from the last checkpointed sequence number.
    */
  private[writers] def pollShard(
    streamsClient: DynamoDbStreamsClient,
    shardId: String,
    iterator: String,
    maxRetries: Int = 3,
    poller: StreamPollerOps = DynamoStreamPoller
  ): (String, Seq[Record], Option[String]) = {
    var attempt = 0
    while (true)
      try {
        val (records, nextIter) =
          poller.getRecords(streamsClient, iterator)
        return (shardId, records, nextIter)
      } catch {
        case e: DynamoDbException
            if e.awsErrorDetails() != null &&
              retryableErrorCodes.contains(
                e.awsErrorDetails().errorCode()
              ) =>
          attempt += 1
          if (attempt > maxRetries) throw e
          val backoffMs =
            math.min(200L * (1L << attempt), 5000L)
          log.warn(
            s"Retryable error on shard $shardId " +
              s"(attempt $attempt/$maxRetries), " +
              s"backing off ${backoffMs}ms"
          )
          Thread.sleep(backoffMs)
      }
    // Unreachable, but required for Scala return type inference
    throw new RuntimeException("Unreachable")
  }

  /** Create the checkpoint table in DynamoDB if it doesn't already exist. Uses `leaseKey` (String,
    * HASH key) as the partition key. Lease and checkpoint data are stored as non-key attributes.
    */
  private[writers] def createCheckpointTable(
    client: DynamoDbClient,
    tableName: String
  ): Unit =
    try {
      client.describeTable(
        DescribeTableRequest.builder().tableName(tableName).build()
      )
      log.info(s"Checkpoint table $tableName already exists")
    } catch {
      case _: ResourceNotFoundException =>
        client.createTable(
          CreateTableRequest
            .builder()
            .tableName(tableName)
            .keySchema(
              KeySchemaElement
                .builder()
                .attributeName(leaseKeyColumn)
                .keyType(KeyType.HASH)
                .build()
            )
            .attributeDefinitions(
              AttributeDefinition
                .builder()
                .attributeName(leaseKeyColumn)
                .attributeType(ScalarAttributeType.S)
                .build()
            )
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build()
        )
        client
          .waiter()
          .waitUntilTableExists(
            DescribeTableRequest.builder().tableName(tableName).build()
          )
        log.info(s"Created checkpoint table $tableName")
    }

  /** Try to claim an unowned or expired shard. Uses a conditional UpdateItem so that the existing
    * checkpoint value (if any) is preserved while the lease owner and expiry are updated.
    *
    * @return
    *   `Some(Some(seqNum))` if claimed and a checkpoint exists, `Some(None)` if claimed without a
    *   prior checkpoint, `None` if the shard is owned by another active worker.
    */
  private[writers] def tryClaimShard(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    leaseDurationMs: Long = defaultLeaseDurationMs
  ): Option[Option[String]] =
    try {
      val result = retryRandom(
        client.updateItem(
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(
              Map(
                leaseKeyColumn -> AttributeValue.fromS(shardId)
              ).asJava
            )
            .updateExpression("SET #owner = :owner, #expiry = :expiry")
            .conditionExpression(
              "attribute_not_exists(#lk) OR #expiry < :now OR #owner = :me"
            )
            .expressionAttributeNames(
              Map(
                "#lk"     -> leaseKeyColumn,
                "#owner"  -> leaseOwnerColumn,
                "#expiry" -> leaseExpiryColumn
              ).asJava
            )
            .expressionAttributeValues(
              Map(
                ":owner" -> AttributeValue.fromS(workerId),
                ":expiry" -> AttributeValue.fromN(
                  (System.currentTimeMillis() + leaseDurationMs).toString
                ),
                ":now" -> AttributeValue.fromN(
                  System.currentTimeMillis().toString
                ),
                ":me" -> AttributeValue.fromS(workerId)
              ).asJava
            )
            .returnValues(ReturnValue.ALL_NEW)
            .build()
        ),
        numRetriesLeft   = 4,
        maxBackOffMillis = 100
      )
      val attrs = result.attributes()
      val checkpoint =
        if (attrs.containsKey(checkpointColumn))
          Some(attrs.get(checkpointColumn).s())
        else
          None
      Some(checkpoint)
    } catch {
      case _: ConditionalCheckFailedException => None
      case e: Exception =>
        log.warn(s"Failed to claim shard $shardId", e)
        None
    }

  /** Renew the lease for an owned shard and optionally update its checkpoint. Uses a conditional
    * UpdateItem that only succeeds if this worker still owns the shard.
    *
    * @return
    *   `true` if the lease was renewed, `false` if the lease was lost (another worker took over).
    */
  private[writers] def renewLeaseAndCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    maybeSeqNum: Option[String],
    leaseDurationMs: Long = defaultLeaseDurationMs
  ): Boolean =
    try {
      val updateExpr = maybeSeqNum match {
        case Some(_) => "SET #expiry = :expiry, #ckpt = :ckpt"
        case None    => "SET #expiry = :expiry"
      }
      val attrNames = {
        val base = Map(
          "#owner"  -> leaseOwnerColumn,
          "#expiry" -> leaseExpiryColumn
        )
        maybeSeqNum match {
          case Some(_) => base + ("#ckpt" -> checkpointColumn)
          case None    => base
        }
      }
      val attrValues = {
        val base = Map(
          ":me" -> AttributeValue.fromS(workerId),
          ":expiry" -> AttributeValue.fromN(
            (System.currentTimeMillis() + leaseDurationMs).toString
          )
        )
        maybeSeqNum match {
          case Some(s) => base + (":ckpt" -> AttributeValue.fromS(s))
          case None    => base
        }
      }
      retryRandom(
        client.updateItem(
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(
              Map(
                leaseKeyColumn -> AttributeValue.fromS(shardId)
              ).asJava
            )
            .updateExpression(updateExpr)
            .conditionExpression("#owner = :me")
            .expressionAttributeNames(attrNames.asJava)
            .expressionAttributeValues(attrValues.asJava)
            .build()
        ),
        numRetriesLeft   = 4,
        maxBackOffMillis = 100
      )
      true
    } catch {
      case _: ConditionalCheckFailedException => false
      case e: Exception =>
        log.warn(s"Failed to renew lease for shard $shardId", e)
        true // don't give up the shard on transient errors
    }

  /** Retry with random backoff, matching the old KCL KinesisRecordProcessor.retryRandom behavior.
    * Retries on throttling and transient DynamoDB errors.
    */
  @annotation.tailrec
  private[writers] def retryRandom[T](
    expression: => T,
    numRetriesLeft: Int,
    maxBackOffMillis: Int
  ): T =
    scala.util.Try(expression) match {
      case scala.util.Success(x) => x
      case scala.util.Failure(e) =>
        e match {
          case ddbEx: DynamoDbException
              if numRetriesLeft > 1 &&
                ddbEx.awsErrorDetails() != null &&
                retryableErrorCodes.contains(
                  ddbEx.awsErrorDetails().errorCode()
                ) =>
            val backOffMillis =
              scala.util.Random.nextInt(maxBackOffMillis)
            Thread.sleep(backOffMillis)
            log.warn(
              s"Retryable exception, backing off ${backOffMillis}ms",
              e
            )
            retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis)
          case _ =>
            throw e
        }
    }
}
