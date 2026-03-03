package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._
import scala.jdk.CollectionConverters._

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Verifies that records are NOT checkpointed when BatchWriter.run() throws, preventing the
  * data-loss scenario where failed records are permanently skipped.
  */
class WriteFailureCheckpointTest extends StreamReplicationTestFixture {

  protected val targetTable = "WriteFailureCheckpointTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)
  override protected def createTargetTableOnSetup: Boolean = false

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("localhost", 8001)),
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
    endpoint                    = Some(DynamoDBEndpoint("localhost", 8000)),
    credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
    streamChanges               = false,
    skipInitialSnapshotTransfer = Some(true),
    writeThroughput             = None,
    throughputWritePercent      = None
  )

  test("checkpoint is NOT written when BatchWriter fails (data-loss prevention)") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-fail-1").build()
    val pollCount = new AtomicInteger(0)

    poller.listShardsFn.set((_, _) => Seq(shard))

    // Return records with sequence numbers on first poll, then empty
    poller.getRecordsFn.set { (_, _, _) =>
      val count = pollCount.incrementAndGet()
      if (count == 1) {
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

    // Target table is not created (createTargetTableOnSetup=false),
    // so BatchWriter.run() will throw when attempting to write.
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

    try {
      // Wait for a few poll cycles so the write failure has been attempted
      Eventually(timeoutMs = 10000) {
        pollCount.get() >= 2
      }(s"Expected at least 2 poll cycles, got ${pollCount.get()}")

      // Verify the shard was actually claimed (checkpoint row exists).
      // Without this check, the test passes vacuously if shard discovery
      // failed silently and no checkpoint row was ever created.
      val checkpointRow = sourceDDb()
        .getItem(
          GetItemRequest
            .builder()
            .tableName(checkpointTable)
            .key(Map("leaseKey" -> AttributeValue.fromS("shard-fail-1")).asJava)
            .consistentRead(true)
            .build()
        )
        .item()
      assert(
        checkpointRow != null && !checkpointRow.isEmpty,
        "Checkpoint row must exist, proving the shard was claimed"
      )

      // The checkpoint should NOT have been advanced since the write failed.
      val checkpoint =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-fail-1")
      assert(
        checkpoint.isEmpty,
        s"Checkpoint should not have been written after write failure, got: $checkpoint"
      )

      // Verify the write was actually attempted: pollCount >= 2 means the
      // poller delivered records on cycle 1 and polled again on cycle 2.
      // Since records were delivered (pollCount==1 returned items), the
      // worker must have attempted a write.
      assert(
        pollCount.get() >= 2,
        s"Expected records to be delivered and write attempted, pollCount=${pollCount.get()}"
      )
    } finally
      handle.stop()
  }
}
