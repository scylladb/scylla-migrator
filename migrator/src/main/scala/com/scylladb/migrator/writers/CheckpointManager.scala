package com.scylladb.migrator.writers

import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  ConditionalCheckFailedException,
  CreateTableRequest,
  DescribeTableRequest,
  DynamoDbException,
  KeySchemaElement,
  KeyType,
  ResourceInUseException,
  ResourceNotFoundException,
  ReturnValue,
  ScalarAttributeType,
  UpdateItemRequest
}

import scala.jdk.CollectionConverters._

/** Trait encapsulating checkpoint and lease operations for DynamoDB stream replication.
  *
  * ==Checkpoint Table Schema==
  *
  * The checkpoint table uses a single hash key and the following columns:
  *
  * {{{
  * leaseKey          (S)  — Hash key. The DynamoDB Streams shard ID.
  * checkpoint        (S)  — Last successfully processed sequence number, or "SHARD_END" sentinel.
  * leaseOwner        (S)  — Worker ID that currently owns this shard's lease.
  * leaseExpiryEpochMs(N)  — Epoch millis when the lease expires; 0 means released.
  * parentShardId     (S)  — Optional parent shard ID for ordering child-after-parent processing.
  * leaseTransferTo   (S)  — Worker ID requesting a graceful lease transfer (removed on release).
  * leaseCounter      (N)  — Monotonically increasing counter incremented on each renewal.
  * }}}
  *
  * ==Lease Protocol==
  *
  * '''Claim:''' A shard is claimable when the row does not exist, the lease has expired
  * (`leaseExpiryEpochMs < now`), or the requesting worker already owns it (`leaseOwner = me`). All
  * claims use conditional writes to prevent races.
  *
  * '''Renewal:''' The lease owner periodically extends `leaseExpiryEpochMs` and optionally updates
  * `checkpoint`. The condition `leaseOwner = me` ensures stolen leases are detected.
  *
  * '''Transfer:''' A worker requests transfer by setting `leaseTransferTo`. The current owner
  * detects this on its next renewal, releases the lease, and the requester claims it.
  *
  * Extracted from `DynamoStreamReplication` to reduce coupling: `StreamReplicationWorker` depends
  * on this trait rather than importing a concrete companion object, making it testable in
  * isolation.
  */
trait CheckpointManager {

  /** Column names matching the KCL checkpoint table schema, plus lease columns. */
  val leaseKeyColumn: String
  val checkpointColumn: String
  val leaseOwnerColumn: String
  val leaseExpiryColumn: String
  val parentShardIdColumn: String
  val leaseTransferToColumn: String
  val leaseCounterColumn: String

  /** Sentinel checkpoint value indicating a shard has been fully consumed. */
  val shardEndSentinel: String

  def createCheckpointTable(client: DynamoDbClient, tableName: String): Unit

  def getCheckpoint(client: DynamoDbClient, tableName: String, shardId: String): Option[String]

  def isParentDrained(
    client: DynamoDbClient,
    tableName: String,
    parentShardId: Option[String]
  ): Boolean

  def tryClaimShard(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    leaseDurationMs: Long = 60000L,
    parentShardId: Option[String] = None
  ): Option[Option[String]]

  def renewLeaseAndCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    maybeSeqNum: Option[String],
    leaseDurationMs: Long = 60000L
  ): Boolean

  def requestLeaseTransfer(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    requestingWorkerId: String
  ): Boolean

  def releaseLease(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String
  ): Unit
}

/** Default implementation of [[CheckpointManager]] backed by DynamoDB conditional writes. */
object DefaultCheckpointManager extends CheckpointManager {

  private val log = LogManager.getLogger("com.scylladb.migrator.writers.CheckpointManager")

  override val leaseKeyColumn = "leaseKey"
  override val checkpointColumn = "checkpoint"
  override val leaseOwnerColumn = "leaseOwner"
  override val leaseExpiryColumn = "leaseExpiryEpochMs"
  override val parentShardIdColumn = "parentShardId"
  override val leaseTransferToColumn = "leaseTransferTo"
  override val leaseCounterColumn = "leaseCounter"

  override val shardEndSentinel = "SHARD_END"

  /** DynamoDB Streams error codes that are safe to retry with backoff. */
  private val retryableErrorCodes = Set(
    "LimitExceededException",
    "InternalServerError",
    "ProvisionedThroughputExceededException"
  )

  override def createCheckpointTable(client: DynamoDbClient, tableName: String): Unit =
    try {
      client.describeTable(
        DescribeTableRequest.builder().tableName(tableName).build()
      )
      log.info(s"Checkpoint table $tableName already exists")
    } catch {
      case _: ResourceNotFoundException =>
        try
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
        catch {
          case _: ResourceInUseException =>
            log.info(s"Checkpoint table $tableName was created by another worker concurrently")
        }
        client
          .waiter()
          .waitUntilTableExists(
            DescribeTableRequest.builder().tableName(tableName).build()
          )
        log.info(s"Checkpoint table $tableName ready")
    }

  override def getCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String
  ): Option[String] =
    try {
      val result = client.getItem(
        software.amazon.awssdk.services.dynamodb.model.GetItemRequest
          .builder()
          .tableName(tableName)
          .key(Map(leaseKeyColumn -> AttributeValue.fromS(shardId)).asJava)
          .projectionExpression(checkpointColumn)
          .consistentRead(true)
          .build()
      )
      val item = result.item()
      if (item != null && item.containsKey(checkpointColumn))
        Some(item.get(checkpointColumn).s())
      else
        None
    } catch {
      case _: ResourceNotFoundException =>
        None
      case e: Exception =>
        log.warn(s"Failed to look up checkpoint for shard $shardId", e)
        throw e
    }

  override def isParentDrained(
    client: DynamoDbClient,
    tableName: String,
    parentShardId: Option[String]
  ): Boolean =
    parentShardId match {
      case None         => true
      case Some(parent) => getCheckpoint(client, tableName, parent).contains(shardEndSentinel)
    }

  override def tryClaimShard(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    leaseDurationMs: Long = 60000L,
    parentShardId: Option[String] = None
  ): Option[Option[String]] =
    try {
      val baseUpdateExpr = "SET #owner = :owner, #expiry = :expiry"
      val updateExpr = parentShardId match {
        case Some(_) => baseUpdateExpr + ", #parent = :parent"
        case None    => baseUpdateExpr
      }
      val baseAttrNames = Map(
        "#lk"     -> leaseKeyColumn,
        "#owner"  -> leaseOwnerColumn,
        "#expiry" -> leaseExpiryColumn
      )
      val attrNames = parentShardId match {
        case Some(_) => baseAttrNames + ("#parent" -> parentShardIdColumn)
        case None    => baseAttrNames
      }
      val nowMs = System.currentTimeMillis()
      val baseAttrValues = Map(
        ":owner" -> AttributeValue.fromS(workerId),
        ":expiry" -> AttributeValue.fromN(
          (nowMs + leaseDurationMs).toString
        ),
        ":now" -> AttributeValue.fromN(
          nowMs.toString
        ),
        ":me" -> AttributeValue.fromS(workerId)
      )
      val attrValues = parentShardId match {
        case Some(p) => baseAttrValues + (":parent" -> AttributeValue.fromS(p))
        case None    => baseAttrValues
      }
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
            .updateExpression(updateExpr)
            .conditionExpression(
              "attribute_not_exists(#lk) OR #expiry < :now OR #owner = :me"
            )
            .expressionAttributeNames(attrNames.asJava)
            .expressionAttributeValues(attrValues.asJava)
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

  override def renewLeaseAndCheckpoint(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String,
    maybeSeqNum: Option[String],
    leaseDurationMs: Long = 60000L
  ): Boolean =
    try {
      val baseExpr = maybeSeqNum match {
        case Some(_) => "SET #expiry = :expiry, #ckpt = :ckpt"
        case None    => "SET #expiry = :expiry"
      }
      val updateExpr = baseExpr + " ADD #counter :one"
      val attrNames = {
        val base = Map(
          "#owner"   -> leaseOwnerColumn,
          "#expiry"  -> leaseExpiryColumn,
          "#counter" -> leaseCounterColumn
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
          ),
          ":one" -> AttributeValue.fromN("1")
        )
        maybeSeqNum match {
          case Some(s) => base + (":ckpt" -> AttributeValue.fromS(s))
          case None    => base
        }
      }
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
            .updateExpression(updateExpr)
            .conditionExpression("#owner = :me")
            .expressionAttributeNames(attrNames.asJava)
            .expressionAttributeValues(attrValues.asJava)
            .returnValues(ReturnValue.ALL_NEW)
            .build()
        ),
        numRetriesLeft   = 4,
        maxBackOffMillis = 100
      )
      val attrs = result.attributes()
      val transferRequested = attrs.containsKey(leaseTransferToColumn) && {
        val transferTo = attrs.get(leaseTransferToColumn).s()
        transferTo != null && transferTo.nonEmpty && transferTo != workerId
      }
      if (transferRequested) {
        val transferTo = attrs.get(leaseTransferToColumn).s()
        log.info(
          s"Lease transfer requested for shard $shardId to $transferTo, releasing"
        )
        releaseLease(client, tableName, shardId, workerId)
        false
      } else
        true
    } catch {
      case _: ConditionalCheckFailedException => false
      case e: Exception =>
        log.warn(s"Failed to renew lease for shard $shardId", e)
        throw e
    }

  override def requestLeaseTransfer(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    requestingWorkerId: String
  ): Boolean =
    try {
      client.updateItem(
        UpdateItemRequest
          .builder()
          .tableName(tableName)
          .key(Map(leaseKeyColumn -> AttributeValue.fromS(shardId)).asJava)
          .updateExpression("SET #transfer = :requester")
          .conditionExpression("attribute_exists(#lk)")
          .expressionAttributeNames(
            Map(
              "#lk"       -> leaseKeyColumn,
              "#transfer" -> leaseTransferToColumn
            ).asJava
          )
          .expressionAttributeValues(
            Map(":requester" -> AttributeValue.fromS(requestingWorkerId)).asJava
          )
          .build()
      )
      true
    } catch {
      case _: ConditionalCheckFailedException => false
      case e: Exception =>
        log.warn(s"Failed to request lease transfer for shard $shardId", e)
        false
    }

  override def releaseLease(
    client: DynamoDbClient,
    tableName: String,
    shardId: String,
    workerId: String
  ): Unit =
    try
      client.updateItem(
        UpdateItemRequest
          .builder()
          .tableName(tableName)
          .key(Map(leaseKeyColumn -> AttributeValue.fromS(shardId)).asJava)
          .updateExpression(
            "SET #expiry = :zero REMOVE #owner, #transfer"
          )
          .conditionExpression("#owner = :me")
          .expressionAttributeNames(
            Map(
              "#owner"    -> leaseOwnerColumn,
              "#expiry"   -> leaseExpiryColumn,
              "#transfer" -> leaseTransferToColumn
            ).asJava
          )
          .expressionAttributeValues(
            Map(
              ":me"   -> AttributeValue.fromS(workerId),
              ":zero" -> AttributeValue.fromN("0")
            ).asJava
          )
          .build()
      )
    catch {
      case e: Exception =>
        log.warn(s"Failed to release lease for shard $shardId", e)
    }

  /** Retry with random backoff for throttling and transient DynamoDB errors.
    *
    * Note: uses `Thread.sleep` for backoff, blocking the calling thread. The max backoff is
    * typically 100ms (for checkpoint operations), making the blocking impact negligible.
    */
  @annotation.tailrec
  private[writers] def retryRandom[T](
    expression: => T,
    numRetriesLeft: Int,
    maxBackOffMillis: Int,
    random: scala.util.Random = scala.util.Random
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
              random.nextInt(maxBackOffMillis)
            Thread.sleep(backOffMillis)
            log.warn(
              s"Retryable exception, backing off ${backOffMillis}ms",
              e
            )
            retryRandom(expression, numRetriesLeft - 1, maxBackOffMillis, random)
          case _ =>
            throw e
        }
    }
}
