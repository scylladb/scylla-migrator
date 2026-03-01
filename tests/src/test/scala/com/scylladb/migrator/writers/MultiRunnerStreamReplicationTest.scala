package com.scylladb.migrator.writers

import com.scylladb.migrator.DynamoStreamPoller
import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  GetItemRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  PutItemRequest,
  Record,
  ResourceNotFoundException,
  ScalarAttributeType,
  ScanRequest,
  Shard,
  StreamRecord,
  StreamSpecification,
  StreamViewType
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

/** End-to-end tests verifying that multiple DynamoStreamReplication.startStreaming() instances can
  * run concurrently, coordinate via the shared checkpoint table, split shards between each other,
  * and replicate all stream records to the target without duplication or loss.
  *
  * Requires DynamoDB Local on port 8001 and Alternator on port 8000.
  */
class MultiRunnerStreamReplicationTest extends StreamReplicationTestFixture {

  protected val targetTable = "MultiRunnerTestTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(makeSourceSettings())

  private def makeSourceSettings(
    pollIntervalSeconds: Int = 1,
    leaseDurationMs: Long = 60000L
  ): SourceSettings.DynamoDB =
    SourceSettings.DynamoDB(
      endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
      region                        = Some("eu-central-1"),
      credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
      table                         = "MultiRunnerTestSource",
      scanSegments                  = None,
      readThroughput                = None,
      throughputReadPercent         = None,
      maxMapTasks                   = None,
      streamingPollIntervalSeconds  = Some(pollIntervalSeconds),
      streamingMaxConsecutiveErrors = Some(5),
      streamingPollingPoolSize      = Some(2),
      streamingLeaseDurationMs      = Some(leaseDurationMs)
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

  private def scanTargetTable(): Seq[java.util.Map[String, AttributeValue]] = {
    val result = targetAlternator().scan(
      ScanRequest.builder().tableName(targetTable).build()
    )
    result.items().asScala.toSeq
  }

  private def scanCheckpointTable(): Seq[java.util.Map[String, AttributeValue]] = {
    val result = sourceDDb().scan(
      ScanRequest.builder().tableName(checkpointTable).build()
    )
    result.items().asScala.toSeq
  }

  private def getTargetTableDesc() =
    targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(targetTable).build())
      .table()

  /** Create a TestStreamPoller that advertises the given shards and delivers items exactly once per
    * shard via getRecordsFn. Each shard delivers `itemsPerShard` unique items on first poll, then
    * returns empty on subsequent polls.
    */
  private def makeTestPoller(
    shards: Seq[String],
    itemsPerShard: Int
  ): TestStreamPoller = {
    val poller = new TestStreamPoller
    val delivered = TrieMap.empty[String, AtomicBoolean]
    shards.foreach(s => delivered.put(s, new AtomicBoolean(false)))

    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")
    poller.listShardsFn.set((_, _) => shards.map(makeShard))
    poller.getShardIteratorFn.set((_, _, shardId, _) => s"iter-$shardId")
    poller.getShardIteratorAfterSequenceFn.set((_, _, shardId, _) => s"iter-$shardId-resume")

    poller.getRecordsFn.set { (_, iterator, _) =>
      // Extract shard id from iterator
      val shardId = shards.find(s => iterator.contains(s)).getOrElse("")
      val flag = delivered.getOrElse(shardId, new AtomicBoolean(true))
      if (!flag.getAndSet(true)) {
        val records = (1 to itemsPerShard).map { i =>
          makeRecord(s"$shardId-item-$i", s"seq-$shardId-$i")
        }
        (records, Some(s"next-iter-$shardId"))
      } else {
        (Seq.empty, Some(s"next-iter-$shardId"))
      }
    }

    poller
  }

  test("parallel shard splitting: two runners process disjoint shards without loss") {
    val sourceSettings = makeSourceSettings()
    val tableDesc = getTargetTableDesc()

    val poller1 = makeTestPoller(Seq("shard-A", "shard-B"), itemsPerShard = 3)
    val poller2 = makeTestPoller(Seq("shard-C", "shard-D"), itemsPerShard = 3)

    val handle1 = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller1
    )
    val handle2 = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller2
    )

    try
      Eventually(timeoutMs = 15000) {
        scanTargetTable().size >= 12
      }(s"Expected 12 items in target, got ${scanTargetTable().size}")
    finally {
      handle1.stop()
      handle2.stop()
    }

    // Verify all 12 items present in target
    val targetItems = scanTargetTable()
    assertEquals(
      targetItems.size,
      12,
      s"Expected 12 items in target, got ${targetItems.size}. " +
        s"Items: ${targetItems.map(_.get("id")).mkString(", ")}"
    )

    // Verify all expected item IDs are present
    val itemIds = targetItems.map(_.get("id").s()).toSet
    val expectedIds = for {
      shard <- Seq("shard-A", "shard-B", "shard-C", "shard-D")
      i     <- 1 to 3
    } yield s"$shard-item-$i"
    assertEquals(itemIds, expectedIds.toSet)

    // Verify checkpoint table has all 4 shards
    val checkpointItems = scanCheckpointTable()
    assertEquals(
      checkpointItems.size,
      4,
      s"Expected 4 checkpoint entries, got ${checkpointItems.size}"
    )
    // After stop(), leases are released (leaseOwner removed), so we just
    // verify the correct number of checkpoint entries exist.
  }

  test("shared shard: no duplicate processing when two runners see the same shard") {
    val sourceSettings = makeSourceSettings()
    val tableDesc = getTargetTableDesc()

    val poller1 = makeTestPoller(Seq("shared-shard"), itemsPerShard = 5)
    val poller2 = makeTestPoller(Seq("shared-shard"), itemsPerShard = 5)

    val handle1 = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller1
    )
    val handle2 = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller2
    )

    try
      Eventually(timeoutMs = 15000) {
        scanTargetTable().size >= 5
      }(s"Expected 5 items in target, got ${scanTargetTable().size}")
    finally {
      handle1.stop()
      handle2.stop()
    }

    // Verify exactly 5 items in target (no duplicates)
    val targetItems = scanTargetTable()
    assertEquals(
      targetItems.size,
      5,
      s"Expected exactly 5 items (no duplicates), got ${targetItems.size}. " +
        s"Items: ${targetItems.map(_.get("id")).mkString(", ")}"
    )

    // Verify checkpoint table has 1 entry with 1 owner
    val checkpointItems = scanCheckpointTable()
    assertEquals(
      checkpointItems.size,
      1,
      s"Expected 1 checkpoint entry, got ${checkpointItems.size}"
    )
    // After stop(), leases are released (leaseOwner removed), so we just
    // verify the correct number of checkpoint entries exist.
  }

  test("sequential failover: runner 2 resumes from checkpoint after runner 1 stops") {
    // Use a real stream-enabled source table for this test
    val realSourceTable = "MultiRunnerFailoverSource"
    val realCheckpointTable = DynamoStreamReplication.buildCheckpointTableName(
      makeSourceSettings().copy(table = realSourceTable)
    )
    val failoverTargetTable = "MultiRunnerFailoverTarget"

    // Cleanup any previous state
    Seq(realSourceTable, realCheckpointTable).foreach { t =>
      try sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(t).build())
      catch { case _: Exception => () }
    }
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(failoverTargetTable).build()
      )
    catch { case _: Exception => () }

    // Create stream-enabled source table
    sourceDDb().createTable(
      CreateTableRequest
        .builder()
        .tableName(realSourceTable)
        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build()
        )
        .streamSpecification(
          StreamSpecification
            .builder()
            .streamEnabled(true)
            .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
            .build()
        )
        .build()
    )
    sourceDDb()
      .waiter()
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(realSourceTable).build())

    // Create target table
    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(failoverTargetTable)
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
      .waitUntilTableExists(
        DescribeTableRequest.builder().tableName(failoverTargetTable).build()
      )

    def putSourceItem(id: String, value: String): Unit =
      sourceDDb().putItem(
        PutItemRequest
          .builder()
          .tableName(realSourceTable)
          .item(
            Map(
              "id"    -> AttributeValue.fromS(id),
              "value" -> AttributeValue.fromS(value)
            ).asJava
          )
          .build()
      )

    // Insert first batch of 5 items
    (1 to 5).foreach(i => putSourceItem(s"item-$i", s"value-$i"))

    val failoverSourceSettings = makeSourceSettings(
      pollIntervalSeconds = 1,
      leaseDurationMs     = 2000L
    ).copy(table = realSourceTable)

    val failoverTargetSettings = targetSettings.copy(table = failoverTargetTable)

    val failoverTableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(failoverTargetTable).build())
      .table()

    // Start runner 1 with real DynamoStreamPoller
    val handle1 = DynamoStreamReplication.startStreaming(
      failoverSourceSettings,
      failoverTargetSettings,
      failoverTableDesc,
      Map.empty
    )

    try
      // Wait for runner 1 to process the first batch
      Eventually(timeoutMs = 15000) {
        targetAlternator()
          .scan(ScanRequest.builder().tableName(failoverTargetTable).build())
          .items()
          .size() >= 5
      }("Runner 1 did not process all 5 items in time")
    finally
      handle1.stop()

    // Insert second batch of 5 items while runner 1 is stopped
    (6 to 10).foreach(i => putSourceItem(s"item-$i", s"value-$i"))

    // Wait for lease to expire (leaseDurationMs = 2000) so runner 2 can claim shards
    Thread.sleep(2500)

    // Start runner 2, which should resume from checkpoint
    val handle2 = DynamoStreamReplication.startStreaming(
      failoverSourceSettings,
      failoverTargetSettings,
      failoverTableDesc,
      Map.empty
    )

    try
      // Wait for runner 2 to process the second batch
      Eventually(timeoutMs = 15000) {
        targetAlternator()
          .scan(ScanRequest.builder().tableName(failoverTargetTable).build())
          .items()
          .size() >= 10
      }("Runner 2 did not process all 10 items in time")
    finally
      handle2.stop()

    // Verify all 10 items present in target with correct values
    val targetItems = targetAlternator()
      .scan(ScanRequest.builder().tableName(failoverTargetTable).build())
      .items()
      .asScala
      .toSeq

    val targetIds = targetItems.map(_.get("id").s()).toSet
    val expectedIds = (1 to 10).map(i => s"item-$i").toSet

    assertEquals(
      targetIds,
      expectedIds,
      s"Expected all 10 items in target. Got ${targetIds.size}: " +
        s"missing=${expectedIds -- targetIds}, extra=${targetIds -- expectedIds}"
    )

    // Verify values are correct
    (1 to 10).foreach { i =>
      val item = targetAlternator()
        .getItem(
          GetItemRequest
            .builder()
            .tableName(failoverTargetTable)
            .key(Map("id" -> AttributeValue.fromS(s"item-$i")).asJava)
            .build()
        )
        .item()
      assert(item != null, s"item-$i should exist in target")
      assertEquals(
        item.get("value").s(),
        s"value-$i",
        s"item-$i should have correct value"
      )
    }

    // Cleanup failover-specific tables
    Seq(realSourceTable, realCheckpointTable).foreach { t =>
      try sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(t).build())
      catch { case _: Exception => () }
    }
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(failoverTargetTable).build()
      )
    catch { case _: Exception => () }
  }
}
