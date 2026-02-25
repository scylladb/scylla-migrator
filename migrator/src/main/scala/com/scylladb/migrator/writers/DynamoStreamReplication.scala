package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, DynamoUtils }
import com.scylladb.migrator.config.{ SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CreateTableRequest,
  DeleteItemRequest,
  DescribeTableRequest,
  DynamoDbException,
  KeySchemaElement,
  KeyType,
  PutItemRequest,
  Record,
  ResourceNotFoundException,
  ScalarAttributeType,
  ShardIteratorType,
  TableDescription
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

  /** Maximum consecutive polling failures before stopping stream replication. */
  private val maxConsecutiveErrors = 50

  /** DynamoDB Streams error codes that are safe to retry with backoff. */
  private val retryableErrorCodes = Set(
    "LimitExceededException",
    "InternalServerError",
    "ProvisionedThroughputExceededException"
  )

  /** Handle for managing the lifecycle of stream replication. */
  class StreamHandle(
    scheduler: ExecutorService,
    pollingPool: ExecutorService,
    latch: CountDownLatch
  ) {

    /** Block until stream replication stops due to sustained errors. */
    def awaitTermination(): Unit = latch.await()

    /** Gracefully stop stream replication. */
    def stop(): Unit = {
      scheduler.shutdown()
      pollingPool.shutdown()
      latch.countDown()
    }
  }

  private[writers] def run(
    items: Iterable[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription
  ): Unit = {
    val msgs = items.flatten
    if (msgs.isEmpty) {
      log.info("No changes to apply")
      return
    }

    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet
    var putCount = 0L
    var deleteCount = 0L

    val client =
      DynamoUtils.buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        Seq.empty
      )
    try
      msgs.foreach { item =>
        val isPut = item.get(operationTypeColumn) == putOperation

        val itemWithoutOp = item.asScala.collect {
          case (k, v) if k != operationTypeColumn => k -> v
        }.asJava

        if (isPut) {
          putCount += 1
          val finalItem = itemWithoutOp.asScala.map { case (key, value) =>
            renamesMap.getOrElse(key, key) -> value
          }.asJava
          try
            client.putItem(
              PutItemRequest.builder().tableName(target.table).item(finalItem).build()
            )
          catch {
            case e: Exception =>
              log.error(s"Failed to put item into ${target.table}", e)
          }
        } else {
          deleteCount += 1
          val keyToDelete = itemWithoutOp.asScala
            .filter { case (key, _) => keyAttributeNames.contains(key) }
            .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
            .asJava
          try
            client.deleteItem(
              DeleteItemRequest.builder().tableName(target.table).key(keyToDelete).build()
            )
          catch {
            case e: Exception =>
              log.error(s"Failed to delete item from ${target.table}", e)
          }
        }
      }
    finally
      client.close()

    log.info(s"""
                |Changes applied:
                |  - $putCount items UPSERTED
                |  - $deleteCount items DELETED
                |""".stripMargin)
  }

  /** Start streaming replication from a DynamoDB stream. Returns a [[StreamHandle]] for lifecycle
    * management. Replaces the previous StreamingContext-based approach with a direct
    * ScheduledExecutorService for simpler, more predictable scheduling.
    */
  def startStreaming(
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    batchIntervalSeconds: Int = 5
  ): StreamHandle = {
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

    val streamArn = DynamoStreamPoller.getStreamArn(sourceClient, src.table)
    log.info(s"Stream ARN: $streamArn")

    // Shard state tracking
    var shardIterators = Map.empty[String, String]
    var initialized = false
    var consecutiveErrors = 0

    // Checkpoint table in DynamoDB (same as old KCL-based approach).
    // Each run creates a fresh table, matching the old behavior where
    // checkpointAppName = s"migrator_${src.table}_${System.currentTimeMillis()}"
    val checkpointTableName =
      s"migrator_${src.table}_${System.currentTimeMillis()}"
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
      math.max(4, Runtime.getRuntime.availableProcessors())
    )
    implicit val pollingEc: ExecutionContext =
      ExecutionContext.fromExecutorService(pollingPool)

    val terminationLatch = new CountDownLatch(1)

    def pollAndProcess(): Unit =
      try {
        // Step 1: Poll all existing tracked shards in parallel.
        // By polling before discovering new shards, we avoid the race where
        // a shard closes between listShards and getRecords, causing its
        // last records to be missed.
        val pollResults = if (shardIterators.nonEmpty) {
          val futures =
            shardIterators.toSeq.map { case (shardId, iterator) =>
              Future(
                pollShard(streamsClient, shardId, iterator)
              ).recover { case e: Exception =>
                log.warn(
                  s"Failed to poll shard $shardId: ${e.getMessage}"
                )
                // Shard will be removed from tracking and re-initialized
                // from checkpoint on next discovery pass
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
            allItems += DynamoStreamPoller.recordToItem(
              record,
              operationTypeColumn,
              putOperation,
              deleteOperation
            )
            // Track sequence number for checkpointing
            val seqNum = record.dynamodb().sequenceNumber()
            if (seqNum != null)
              updatedSeqNums(shardId) = seqNum
          }
          nextIter match {
            case Some(next) => updatedIterators(shardId) = next
            case None       => // shard closed or poll failed
          }
        }

        shardIterators = updatedIterators.toMap
        shardSequenceNumbers = updatedSeqNums.toMap

        // Step 2: Discover new shards and initialize their iterators.
        // This happens after polling so that closing shards have already
        // been read before they disappear from the shard list.
        val shards =
          DynamoStreamPoller.listShards(streamsClient, streamArn)
        for {
          shard <- shards
          if !shardIterators.contains(shard.shardId())
        } {
          val iterator =
            shardSequenceNumbers.get(shard.shardId()) match {
              case Some(seqNum) =>
                // Resume from checkpoint
                try
                  DynamoStreamPoller.getShardIteratorAfterSequence(
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
                    DynamoStreamPoller.getShardIterator(
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
                DynamoStreamPoller.getShardIterator(
                  streamsClient,
                  streamArn,
                  shard.shardId(),
                  iterType
                )
            }
          shardIterators = shardIterators + (shard.shardId() -> iterator)
        }
        initialized = true

        // Save checkpoint to DynamoDB table
        saveCheckpoint(checkpointClient, checkpointTableName, shardSequenceNumbers)

        // Reset error counter on success
        if (consecutiveErrors > 0) {
          log.info(
            s"Recovered after $consecutiveErrors consecutive errors"
          )
          consecutiveErrors = 0
        }

        if (allItems.nonEmpty)
          run(allItems, target, renamesMap, targetTableDesc)
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

    // Initial poll
    pollAndProcess()

    // Schedule periodic polling with fixed delay to prevent overlapping
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
      batchIntervalSeconds.toLong,
      batchIntervalSeconds.toLong,
      TimeUnit.SECONDS
    )

    new StreamHandle(scheduler, pollingPool, terminationLatch)
  }

  /** Poll a single shard with retry and exponential backoff for rate-limiting errors. DynamoDB
    * Streams limits to 5 GetRecords calls/second/shard; this backoff ensures we respect that limit
    * even under contention.
    */
  private def pollShard(
    streamsClient: DynamoDbStreamsClient,
    shardId: String,
    iterator: String,
    maxRetries: Int = 3
  ): (String, Seq[Record], Option[String]) = {
    var attempt = 0
    while (true)
      try {
        val (records, nextIter) =
          DynamoStreamPoller.getRecords(streamsClient, iterator)
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

  /** Column names matching the KCL checkpoint table schema. */
  private val leaseKeyColumn = "leaseKey"
  private val checkpointColumn = "checkpoint"

  /** Create the checkpoint table in DynamoDB if it doesn't already exist. Uses the same schema as
    * KCL: `leaseKey` (String, HASH key) with a `checkpoint` attribute for the sequence number.
    */
  private def createCheckpointTable(
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

  /** Save per-shard sequence numbers to the checkpoint DynamoDB table. Each shard gets its own item
    * with `leaseKey` = shardId and `checkpoint` = sequenceNumber.
    */
  private def saveCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    sequenceNumbers: Map[String, String]
  ): Unit =
    try
      sequenceNumbers.foreach { case (shardId, seqNum) =>
        client.putItem(
          PutItemRequest
            .builder()
            .tableName(tableName)
            .item(
              Map(
                leaseKeyColumn   -> AttributeValue.fromS(shardId),
                checkpointColumn -> AttributeValue.fromS(seqNum)
              ).asJava
            )
            .build()
        )
      }
    catch {
      case e: Exception =>
        log.warn("Failed to save checkpoint", e)
    }
}
