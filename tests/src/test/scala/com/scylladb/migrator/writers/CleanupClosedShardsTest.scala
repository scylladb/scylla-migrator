package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Tests for cleanupClosedShards behavior in StreamReplicationWorker.
  *
  * Verifies that checkpoint rows for closed shards (where getRecords returns nextIterator=None) are
  * tracked and eventually cleaned up after they've been absent from listShards for enough cycles.
  */
class CleanupClosedShardsTest extends MigratorSuiteWithDynamoDBLocal {

  private val targetTable = "CleanupShardsTarget"
  private val checkpointTable = "migrator_CleanupShardsSource"

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "CleanupShardsSource",
    scanSegments                  = None,
    readThroughput                = None,
    throughputReadPercent         = None,
    maxMapTasks                   = None,
    streamingPollIntervalSeconds  = Some(1),
    streamingMaxConsecutiveErrors = Some(50),
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

  test("closed shard gets SHARD_END checkpoint written") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    val shard = Shard.builder().shardId("shard-closing-1").build()
    val pollCount = new AtomicInteger(0)

    poller.listShardsFn = (_, _) =>
      if (pollCount.get() <= 1) Seq(shard)
      else Seq.empty // shard disappears after a few cycles

    // First call returns records with nextIterator=None (shard closed)
    poller.getRecordsFn = (_, _, _) => {
      pollCount.incrementAndGet()
      (Seq.empty, None) // None signals shard is closed
    }

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

    // Wait until SHARD_END is written to the checkpoint table
    Eventually(timeoutMs = 10000) {
      DefaultCheckpointManager
        .getCheckpoint(sourceDDb(), checkpointTable, "shard-closing-1")
        .contains(DefaultCheckpointManager.shardEndSentinel)
    }("Expected SHARD_END checkpoint for shard-closing-1")

    handle.stop()
  }

  test("system remains stable when shards close and disappear from listing") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    val shard = Shard.builder().shardId("shard-vanish-1").build()
    val pollCount = new AtomicInteger(0)

    // Shard appears in first cycle, then disappears
    poller.listShardsFn = (_, _) =>
      if (pollCount.get() <= 1) Seq(shard)
      else Seq.empty

    // Return empty records with None (closed) immediately
    poller.getRecordsFn = (_, _, _) => {
      pollCount.incrementAndGet()
      (Seq.empty, None)
    }

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

    Eventually(timeoutMs = 10000) {
      pollCount.get() >= 2
    }(s"Expected at least 2 poll cycles, got ${pollCount.get()}")

    handle.stop()
  }
}
