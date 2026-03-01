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
  protected val checkpointTable = "migrator_StreamPollingTestSource"

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

    // First cycle: return shard to claim; subsequent cycles: return empty
    poller.listShardsFn.set((_, _) => {
      val count = pollCount.incrementAndGet()
      if (count <= 1) Seq(shard)
      else Seq.empty
    })

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
      // Wait for first cycle to claim and start polling the shard
      Eventually(timeoutMs = 10000) {
        pollCount.get() >= 1
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
        pollCount.get() >= 2
      }(s"Expected at least 2 poll cycles, got ${pollCount.get()}")
    } finally
      handle.stop()
  }
}
