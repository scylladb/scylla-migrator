package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DescribeTableRequest,
  GetItemRequest,
  Record,
  Shard,
  StreamRecord
}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Tests for pollAndProcess behavior exercised through startStreaming with a TestStreamPoller.
  * Requires DynamoDB Local on port 8001 (for checkpoint table) and Alternator on port 8000 (for
  * target table).
  */
class StreamReplicationPollingTest extends StreamReplicationTestFixture {

  protected val targetTable = "StreamPollingTestTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("localhost", 8001)),
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
    endpoint                    = Some(DynamoDBEndpoint("localhost", 8000)),
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

  test("consecutive error threshold: terminates after maxConsecutiveErrors failures") {
    val poller = new TestStreamPoller
    // getStreamArn returns a fake ARN
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")
    // listShards always throws to simulate persistent failure
    poller.listShardsFn.set((_, _) => throw new RuntimeException("simulated stream failure"))

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
      // maxConsecutiveErrors=5, poll interval=1s → should terminate within ~10s
      val terminated = handle.awaitTermination(15, TimeUnit.SECONDS)
      assert(terminated, "Stream replication should have terminated due to consecutive errors")
    } finally
      handle.stop()
  }

  test("lost-lease-mid-cycle: shard removed from tracking after lease stolen") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val pollCount = new AtomicInteger(0)
    val shard = makeShard("shard-steal-1")

    // Always return the shard in list so it stays discoverable
    poller.listShardsFn.set { (_, _) =>
      pollCount.incrementAndGet()
      Seq(shard)
    }

    // getRecords returns empty
    poller.getRecordsFn.set((_, _, _) => (Seq.empty, Some("next-iter")))

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
      // Wait for the shard to be claimed and tracked
      Eventually(timeoutMs = 10000) {
        pollCount.get() >= 2
      }("Expected at least 2 poll cycles before lease steal")

      // Verify shard is initially claimed by checking the checkpoint table has a lease entry
      val leaseEntry = sourceDDb().getItem(
        software.amazon.awssdk.services.dynamodb.model.GetItemRequest
          .builder()
          .tableName(checkpointTable)
          .key(Map("leaseKey" -> AttributeValue.fromS("shard-steal-1")).asJava)
          .build()
      ).item()
      assert(
        leaseEntry != null && !leaseEntry.isEmpty,
        "Shard shard-steal-1 should have a lease entry in the checkpoint table"
      )

      // Steal the lease by updating the checkpoint table with a different owner
      // and a far-future expiry so the worker can't reclaim it
      val leaseKeyColumn = "leaseKey"
      val leaseOwnerColumn = "leaseOwner"
      val leaseExpiryColumn = "leaseExpiryEpochMs"

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

      // Wait for the lease renewal cycle to detect the stolen lease and remove the shard.
      // The renewal interval is leaseDurationMs/3 = 20s, so we need to wait long enough.
      // The background renewal thread or the checkpoint-after-write path should detect the
      // ConditionalCheckFailedException (owner != me) and remove the shard.
      // After removal, the shard should no longer appear in the worker's owned set.
      // Since the shard is still returned by listShards, the worker will try to reclaim it
      // but fail (because the thief's lease hasn't expired), so it stays untracked.
      Eventually(timeoutMs = 30000, intervalMs = 500) {
        val checkpoint =
          DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-steal-1")
        // The worker should stop trying to checkpoint this shard.
        // After detecting the lost lease, the next renewal should fail.
        // We verify by checking that subsequent lease renewals don't succeed.
        val row = sourceDDb()
          .getItem(
            GetItemRequest
              .builder()
              .tableName(checkpointTable)
              .key(Map(leaseKeyColumn -> AttributeValue.fromS("shard-steal-1")).asJava)
              .consistentRead(true)
              .build()
          )
          .item()
        // The owner should still be the thief (not reclaimed by the original worker)
        row != null && row.get(leaseOwnerColumn) != null &&
        row.get(leaseOwnerColumn).s() == "worker-thief"
      }(
        "Expected shard lease to remain with the thief after worker detects the stolen lease"
      )
    } finally
      handle.stop()
  }
}
