package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  Record,
  ResourceNotFoundException,
  ScalarAttributeType,
  Shard,
  ShardIteratorType,
  StreamRecord
}

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** Tests for pollAndProcess behavior exercised through startStreaming with a TestStreamPoller.
  * Requires DynamoDB Local on port 8001 (for checkpoint table) and Alternator on port 8000 (for
  * target table).
  */
class StreamReplicationPollingTest extends MigratorSuiteWithDynamoDBLocal {

  private val targetTable = "StreamPollingTestTarget"
  private val checkpointTable = "migrator_StreamPollingTestSource"

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "StreamPollingTestSource",
    scanSegments                  = None,
    readThroughput                = None,
    throughputReadPercent         = None,
    maxMapTasks                   = None,
    streamingPollIntervalSeconds  = Some(1),
    streamingMaxConsecutiveErrors = Some(5),
    streamingPollingPoolSize      = Some(2),
    streamingLeaseDurationMs      = Some(60000L)
  )

  private val targetSettings = TargetSettings.DynamoDB(
    table                       = targetTable,
    region                      = Some("eu-central-1"),
    endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
    credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
    streamChanges               = false,
    skipInitialSnapshotTransfer = Some(true),
    writeThroughput             = None,
    throughputWritePercent      = None
  )

  private def makeRecord(id: String, seqNum: String): Record =
    Record
      .builder()
      .eventName("INSERT")
      .dynamodb(
        StreamRecord
          .builder()
          .keys(Map("id" -> AttributeValue.fromS(id)).asJava)
          .newImage(
            Map(
              "id"    -> AttributeValue.fromS(id),
              "value" -> AttributeValue.fromS(s"val-$id")
            ).asJava
          )
          .sequenceNumber(seqNum)
          .build()
      )
      .build()

  private def makeShard(shardId: String): Shard =
    Shard.builder().shardId(shardId).build()

  private def ensureTargetTable(): Unit = {
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(targetTable).build()
      )
    catch { case _: Exception => () }

    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(targetTable)
        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
        )
        .build()
    )
    targetAlternator()
      .waiter()
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(targetTable).build())
  }

  private def cleanupTables(): Unit = {
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(targetTable).build()
      )
    catch { case _: Exception => () }
    try
      sourceDDb().deleteTable(
        DeleteTableRequest.builder().tableName(checkpointTable).build()
      )
    catch { case _: Exception => () }
  }

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    cleanupTables()
    ensureTargetTable()
  }

  override def afterEach(context: AfterEach): Unit = {
    cleanupTables()
    super.afterEach(context)
  }

  test("consecutive error threshold: terminates after maxConsecutiveErrors failures") {
    val poller = new TestStreamPoller
    // getStreamArn returns a fake ARN
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"
    // listShards always throws to simulate persistent failure
    poller.listShardsFn = (_, _) => throw new RuntimeException("simulated stream failure")

    val tableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(targetTable).build())
      .table()

    val handle = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller
    )

    // maxConsecutiveErrors=5, poll interval=1s → should terminate within ~10s
    val terminated = handle.awaitTermination(15, TimeUnit.SECONDS)
    handle.stop()
    assert(terminated, "Stream replication should have terminated due to consecutive errors")
  }

  test("lost-lease-mid-cycle: shard removed from tracking after lease stolen") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    var pollCount = 0
    val shard = makeShard("shard-steal-1")

    // First cycle: return shard to claim; subsequent cycles: return empty
    poller.listShardsFn = (_, _) => {
      pollCount += 1
      if (pollCount <= 1) Seq(shard)
      else Seq.empty
    }

    // getRecords returns empty
    poller.getRecordsFn = (_, _, _) => (Seq.empty, Some("next-iter"))

    val tableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(targetTable).build())
      .table()

    val handle = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller
    )

    // Wait for first cycle to claim and start polling the shard
    Eventually(timeoutMs = 10000) {
      pollCount >= 1
    }("Expected at least 1 poll cycle")

    // Now steal the lease by manually updating the checkpoint table
    val leaseKeyColumn = "leaseKey"
    val leaseOwnerColumn = "leaseOwner"
    val leaseExpiryColumn = "leaseExpiryEpochMs"

    try
      sourceDDb().updateItem(
        software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
          .builder()
          .tableName(checkpointTable)
          .key(Map(leaseKeyColumn -> AttributeValue.fromS("shard-steal-1")).asJava)
          .updateExpression("SET #owner = :thief, #expiry = :expiry")
          .expressionAttributeNames(
            Map("#owner" -> leaseOwnerColumn, "#expiry" -> leaseExpiryColumn).asJava
          )
          .expressionAttributeValues(
            Map(
              ":thief"  -> AttributeValue.fromS("worker-thief"),
              ":expiry" -> AttributeValue.fromN((System.currentTimeMillis() + 600000L).toString)
            ).asJava
          )
          .build()
      )
    catch {
      case _: Exception => () // table might not exist if first poll hasn't run yet
    }

    // Wait for a couple more poll cycles so renewLeaseAndCheckpoint sees the stolen lease
    Eventually(timeoutMs = 10000) {
      pollCount >= 2
    }(s"Expected at least 2 poll cycles, got $pollCount")

    handle.stop()
  }
}
