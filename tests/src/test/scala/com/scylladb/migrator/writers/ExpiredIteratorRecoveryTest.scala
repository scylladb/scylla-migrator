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

/** Tests for expired iterator recovery in pollOwnedShards.
  *
  * Simulates an ExpiredIteratorException on the first getRecords call, then verifies that the worker
  * recovers by refreshing the iterator (from checkpoint or TRIM_HORIZON) and continues processing.
  */
class ExpiredIteratorRecoveryTest extends MigratorSuiteWithDynamoDBLocal {

  private val targetTable = "ExpiredIterRecoveryTarget"
  private val checkpointTable = "migrator_ExpiredIterRecoverySource"

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "ExpiredIterRecoverySource",
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

  test("recovers from ExpiredIteratorException by refreshing iterator from checkpoint") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    val shard = Shard.builder().shardId("shard-expired-1").build()
    poller.listShardsFn = (_, _) => Seq(shard)

    val getRecordsCallCount = new AtomicInteger(0)

    // First call throws ExpiredIteratorException, subsequent calls succeed with empty records
    poller.getRecordsFn = (_, iterator, _) => {
      val callNum = getRecordsCallCount.incrementAndGet()
      if (callNum == 1) {
        // Simulate an expired iterator - DynamoDB SDK throws DynamoDbException with this error code
        throw DynamoDbException
          .builder()
          .message("Iterator expired")
          .awsErrorDetails(
            software.amazon.awssdk.awscore.exception.AwsErrorDetails
              .builder()
              .errorCode("ExpiredIteratorException")
              .errorMessage("Iterator expired")
              .build()
          )
          .build()
      }
      (Seq.empty, Some("refreshed-iter"))
    }

    // getShardIterator should be called as fallback after the expired iterator
    var shardIteratorRequested = false
    poller.getShardIteratorFn = (_, _, _, iterType) => {
      shardIteratorRequested = true
      "fallback-trim-horizon-iter"
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

    // Wait for a few poll cycles
    Thread.sleep(5000)

    // The worker should have recovered: first call expired, then it refreshed the iterator
    assert(
      getRecordsCallCount.get() >= 2,
      s"Expected at least 2 getRecords calls (1 expired + 1 recovered), got ${getRecordsCallCount.get()}"
    )

    handle.stop()
  }

  test("recovers from ExpiredIteratorException with TRIM_HORIZON when checkpoint lookup fails") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    val shard = Shard.builder().shardId("shard-expired-2").build()
    poller.listShardsFn = (_, _) => Seq(shard)

    val getRecordsCallCount = new AtomicInteger(0)

    poller.getRecordsFn = (_, _, _) => {
      val callNum = getRecordsCallCount.incrementAndGet()
      if (callNum == 1)
        throw DynamoDbException
          .builder()
          .message("Iterator expired")
          .awsErrorDetails(
            software.amazon.awssdk.awscore.exception.AwsErrorDetails
              .builder()
              .errorCode("ExpiredIteratorException")
              .errorMessage("Iterator expired")
              .build()
          )
          .build()
      (Seq.empty, Some("refreshed-iter"))
    }

    // getShardIteratorAfterSequence fails (simulating no valid checkpoint)
    poller.getShardIteratorAfterSequenceFn = (_, _, _, _) =>
      throw new RuntimeException("No valid sequence number")

    // Should fall back to TRIM_HORIZON
    var trimHorizonRequested = false
    poller.getShardIteratorFn = (_, _, _, iterType) => {
      if (iterType == ShardIteratorType.TRIM_HORIZON) trimHorizonRequested = true
      "trim-horizon-iter"
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

    Thread.sleep(5000)

    assert(
      getRecordsCallCount.get() >= 2,
      s"Expected recovery after expired iterator, got ${getRecordsCallCount.get()} calls"
    )

    handle.stop()
  }
}
