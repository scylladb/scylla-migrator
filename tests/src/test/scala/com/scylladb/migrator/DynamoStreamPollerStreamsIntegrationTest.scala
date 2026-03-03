package com.scylladb.migrator

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  PutItemRequest,
  ResourceNotFoundException,
  ScalarAttributeType,
  ShardIteratorType,
  StreamSpecification,
  StreamViewType
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.net.URI
import scala.jdk.CollectionConverters._

/** Integration tests for DynamoStreamPoller thin-wrapper methods using DynamoDB Local streams.
  * Requires DynamoDB Local on port 8001 with streams support.
  */
class DynamoStreamPollerStreamsIntegrationTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "StreamPollerStreamsTest"

  private lazy val streamsClient: DynamoDbStreamsClient =
    DynamoDbStreamsClient
      .builder()
      .region(Region.of("dummy"))
      .endpointOverride(new URI("http://localhost:8001"))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
      )
      .build()

  private def createStreamEnabledTable(): String = {
    try {
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      sourceDDb()
        .waiter()
        .waitUntilTableNotExists(DescribeTableRequest.builder().tableName(tableName).build())
    } catch { case _: ResourceNotFoundException => () }

    sourceDDb().createTable(
      CreateTableRequest
        .builder()
        .tableName(tableName)
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
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build())

    // Return the stream ARN
    DynamoStreamPoller.getStreamArn(sourceDDb(), tableName)
  }

  private def putItem(id: String, value: String): Unit =
    sourceDDb().putItem(
      PutItemRequest
        .builder()
        .tableName(tableName)
        .item(
          Map(
            "id"    -> AttributeValue.fromS(id),
            "value" -> AttributeValue.fromS(value)
          ).asJava
        )
        .build()
    )

  test("getStreamArn: returns valid ARN for stream-enabled table") {
    val streamArn = createStreamEnabledTable()
    assert(streamArn != null)
    assert(streamArn.nonEmpty)
    assert(streamArn.contains(tableName), s"ARN should contain table name: $streamArn")
  }

  test("listShards: returns shards for a stream") {
    val streamArn = createStreamEnabledTable()
    // Insert an item to ensure at least one shard exists
    putItem("ls-1", "val1")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty, "Expected at least one shard")
    shards.foreach { shard =>
      assert(shard.shardId() != null)
      assert(shard.shardId().nonEmpty)
    }
  }

  test("getShardIterator: returns iterator with TRIM_HORIZON") {
    val streamArn = createStreamEnabledTable()
    putItem("gi-1", "val1")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty)

    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shards.head.shardId(),
      ShardIteratorType.TRIM_HORIZON
    )
    assert(iterator != null)
    assert(iterator.nonEmpty)
  }

  test("getShardIterator: returns iterator with LATEST") {
    val streamArn = createStreamEnabledTable()
    putItem("gi-2", "val2")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty)

    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shards.head.shardId(),
      ShardIteratorType.LATEST
    )
    assert(iterator != null)
    assert(iterator.nonEmpty)
  }

  test("getRecords: reads records from a shard") {
    val streamArn = createStreamEnabledTable()
    putItem("gr-1", "val1")
    putItem("gr-2", "val2")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty)

    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shards.head.shardId(),
      ShardIteratorType.TRIM_HORIZON
    )

    val (records, nextIter) = DynamoStreamPoller.getRecords(streamsClient, iterator)
    assert(records.nonEmpty, "Expected records from stream")
    records.foreach { record =>
      assert(record.dynamodb() != null)
      assert(record.dynamodb().sequenceNumber() != null)
    }
  }

  test("getShardIteratorAfterSequence: resumes from a sequence number") {
    val streamArn = createStreamEnabledTable()
    putItem("gas-1", "val1")
    putItem("gas-2", "val2")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty)
    val shardId = shards.head.shardId()

    // Read first batch to get a sequence number
    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shardId,
      ShardIteratorType.TRIM_HORIZON
    )
    val (records, _) = DynamoStreamPoller.getRecords(streamsClient, iterator)
    assert(records.nonEmpty, "Expected at least one record to get a sequence number")

    val seqNum = records.head.dynamodb().sequenceNumber()

    // Resume after that sequence number
    val afterIter = DynamoStreamPoller.getShardIteratorAfterSequence(
      streamsClient,
      streamArn,
      shardId,
      seqNum
    )
    assert(afterIter != null)
    assert(afterIter.nonEmpty)

    // Reading from the "after" iterator should skip the first record
    val (afterRecords, _) = DynamoStreamPoller.getRecords(streamsClient, afterIter)
    // The records after the first sequence number should not include the first record
    afterRecords.foreach { record =>
      assert(
        record.dynamodb().sequenceNumber() != records.head.dynamodb().sequenceNumber(),
        "After-sequence iterator should skip the record at the given sequence number"
      )
    }
  }

  test("recordToItem: converts real stream records correctly") {
    val streamArn = createStreamEnabledTable()
    putItem("rti-1", "val1")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shards.head.shardId(),
      ShardIteratorType.TRIM_HORIZON
    )
    val (records, _) = DynamoStreamPoller.getRecords(streamsClient, iterator)
    assert(records.nonEmpty)

    val putMarker = AttributeValue.fromBool(true)
    val deleteMarker = AttributeValue.fromBool(false)
    val result = DynamoStreamPoller.recordToItem(records.head, "_op", putMarker, deleteMarker)
    assert(result.isDefined)
    val item = result.get.asScala.toMap
    assertEquals(item("_op"), putMarker)
    assertEquals(item("id"), AttributeValue.fromS("rti-1"))
  }

  test("resume-from-checkpoint: AFTER_SEQUENCE_NUMBER resumes past checkpointed record") {
    val streamArn = createStreamEnabledTable()
    // Insert 3 items to create stream records
    putItem("rfc-1", "val1")
    putItem("rfc-2", "val2")
    putItem("rfc-3", "val3")

    val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
    assert(shards.nonEmpty)
    val shardId = shards.head.shardId()

    // Read all records from the beginning
    val iterator = DynamoStreamPoller.getShardIterator(
      streamsClient,
      streamArn,
      shardId,
      ShardIteratorType.TRIM_HORIZON
    )
    val (allRecords, _) = DynamoStreamPoller.getRecords(streamsClient, iterator)
    assert(allRecords.size >= 2, s"Expected at least 2 records, got ${allRecords.size}")

    // Use the first record's sequence number as the "checkpoint"
    val checkpointSeqNum = allRecords.head.dynamodb().sequenceNumber()

    // Resume from the checkpoint using AFTER_SEQUENCE_NUMBER (this is what
    // startStreaming does when tryClaimShard returns a stored checkpoint)
    val resumeIter = DynamoStreamPoller.getShardIteratorAfterSequence(
      streamsClient,
      streamArn,
      shardId,
      checkpointSeqNum
    )
    val (resumedRecords, _) = DynamoStreamPoller.getRecords(streamsClient, resumeIter)

    // Verify none of the resumed records have the checkpointed sequence number
    resumedRecords.foreach { record =>
      assert(
        record.dynamodb().sequenceNumber() != checkpointSeqNum,
        s"Resumed records should not include the checkpointed sequence number $checkpointSeqNum"
      )
    }

    // Verify we got fewer records than the full read (we skipped at least the first one)
    assert(
      resumedRecords.size < allRecords.size,
      s"Resumed read (${resumedRecords.size}) should have fewer records than full read (${allRecords.size})"
    )
  }

  override def afterEach(context: AfterEach): Unit = {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
    catch { case _: Exception => () }
    super.afterEach(context)
  }

  override def afterAll(): Unit = {
    streamsClient.close()
    super.afterAll()
  }
}
