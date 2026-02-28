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
import scala.jdk.CollectionConverters._

/** Verifies that records are NOT checkpointed when BatchWriter.run() throws, preventing the
  * data-loss scenario where failed records are permanently skipped.
  */
class WriteFailureCheckpointTest extends MigratorSuiteWithDynamoDBLocal {

  private val targetTable = "WriteFailureCheckpointTarget"
  private val checkpointTable = "migrator_WriteFailureCheckpointSource"

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "WriteFailureCheckpointSource",
    scanSegments                  = None,
    readThroughput                = None,
    throughputReadPercent         = None,
    maxMapTasks                   = None,
    streamingPollIntervalSeconds  = Some(1),
    streamingMaxConsecutiveErrors = Some(10),
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

  test("checkpoint is NOT written when BatchWriter fails (data-loss prevention)") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn = (_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s"

    val shard = Shard.builder().shardId("shard-fail-1").build()
    var pollCount = 0

    poller.listShardsFn = (_, _) => Seq(shard)

    // Return records with sequence numbers on first poll, then empty
    poller.getRecordsFn = (_, _, _) => {
      pollCount += 1
      if (pollCount == 1) {
        val record = Record
          .builder()
          .eventName("INSERT")
          .dynamodb(
            StreamRecord
              .builder()
              .keys(Map("id" -> AttributeValue.fromS("rec-1")).asJava)
              .newImage(
                Map(
                  "id"    -> AttributeValue.fromS("rec-1"),
                  "value" -> AttributeValue.fromS("val-1")
                ).asJava
              )
              .sequenceNumber("seq-001")
              .build()
          )
          .build()
        (Seq(record), Some("next-iter"))
      } else
        (Seq.empty, Some("next-iter"))
    }

    // Delete the target table to force BatchWriter.run() to throw
    targetAlternator().deleteTable(
      DeleteTableRequest.builder().tableName(targetTable).build()
    )

    val tableDesc = TableDescription
      .builder()
      .tableName(targetTable)
      .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
      .build()

    val handle = DynamoStreamReplication.startStreaming(
      sourceSettings,
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller
    )

    // Wait for a few poll cycles
    Thread.sleep(4000)

    // The checkpoint should NOT have been advanced since the write failed.
    // If no checkpoint was written, getCheckpoint returns None (no seq num recorded).
    val checkpoint =
      DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-fail-1")
    assert(
      checkpoint.isEmpty || checkpoint.contains("SHARD_END") == false,
      s"Checkpoint should not contain a sequence number after write failure, got: $checkpoint"
    )

    handle.stop()
  }
}
