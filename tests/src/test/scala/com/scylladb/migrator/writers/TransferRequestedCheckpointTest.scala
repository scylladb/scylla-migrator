package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import scala.jdk.CollectionConverters._

/** Tests that when a lease transfer is requested (LeaseRenewResult.TransferRequested),
  * the worker checkpoints, releases the lease, and removes the shard from tracking.
  *
  * The transfer request is triggered by setting the `leaseTransferTo` column in the
  * checkpoint table to a different worker ID. The owning worker detects this during
  * `checkpointAfterWrite` and releases the lease.
  */
class TransferRequestedCheckpointTest extends StreamReplicationTestFixture {

  protected val targetTable = "TransferReqCkptTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "TransferReqCkptSource",
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

  test("shard is released after checkpoint when lease transfer is requested") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-transfer-1").build()
    val pollCount = new AtomicInteger(0)
    val recordsDelivered = new AtomicBoolean(false)

    poller.listShardsFn.set((_, _) => Seq(shard))
    poller.getShardIteratorFn.set((_, _, shardId, _) => s"iter-$shardId")
    poller.getShardIteratorAfterSequenceFn.set((_, _, shardId, _) => s"iter-after-$shardId")

    // Return one record on first call, empty thereafter
    poller.getRecordsFn.set { (_, _, _) =>
      pollCount.incrementAndGet()
      if (!recordsDelivered.getAndSet(true)) {
        val record = Record
          .builder()
          .eventName("INSERT")
          .dynamodb(
            StreamRecord
              .builder()
              .keys(Map("id" -> AttributeValue.fromS("transfer-item")).asJava)
              .newImage(
                Map(
                  "id"    -> AttributeValue.fromS("transfer-item"),
                  "value" -> AttributeValue.fromS("val-transfer")
                ).asJava
              )
              .sequenceNumber("seq-transfer-1")
              .build()
          )
          .build()
        (Seq(record), Some("next-iter"))
      } else
        (Seq.empty, Some("next-iter"))
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

    try {
      // Wait for the shard to be claimed and records checkpointed
      Eventually(timeoutMs = 10000) {
        val ckpt =
          DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-transfer-1")
        ckpt.contains("seq-transfer-1")
      }("Expected checkpoint to be written for shard-transfer-1")

      // Now request a lease transfer by setting leaseTransferTo on the checkpoint row
      sourceDDb().updateItem(
        UpdateItemRequest
          .builder()
          .tableName(checkpointTable)
          .key(Map("leaseKey" -> AttributeValue.fromS("shard-transfer-1")).asJava)
          .updateExpression("SET #transfer = :requester")
          .expressionAttributeNames(Map("#transfer" -> "leaseTransferTo").asJava)
          .expressionAttributeValues(
            Map(":requester" -> AttributeValue.fromS("other-worker")).asJava
          )
          .build()
      )

      // Wait for the worker to detect the transfer request and release the lease.
      // After release, the leaseOwner should be removed and leaseExpiryEpochMs set to 0.
      Eventually(timeoutMs = 30000, intervalMs = 500) {
        val row = sourceDDb()
          .getItem(
            GetItemRequest
              .builder()
              .tableName(checkpointTable)
              .key(Map("leaseKey" -> AttributeValue.fromS("shard-transfer-1")).asJava)
              .consistentRead(true)
              .build()
          )
          .item()
        row != null && {
          val expiry = row.get("leaseExpiryEpochMs")
          // After releaseLease: expiry is set to 0 and owner is removed
          expiry != null && expiry.n() == "0"
        }
      }(
        "Expected lease to be released (expiry=0) after transfer request was detected"
      )

      // Verify the checkpoint was preserved (data was saved before release)
      val finalCkpt =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-transfer-1")
      assertEquals(
        finalCkpt,
        Some("seq-transfer-1"),
        "Checkpoint should be preserved after lease transfer"
      )
    } finally
      handle.stop()
  }
}
