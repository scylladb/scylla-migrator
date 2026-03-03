package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import scala.jdk.CollectionConverters._

/** Tests that when a shard poll hangs beyond `pollFutureTimeoutSeconds`, partial results from
  * completed futures are still collected and processed, rather than discarding all results.
  */
class PollFutureTimeoutTest extends StreamReplicationTestFixture {

  protected val targetTable = "PollFutureTimeoutTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)

  // Use a short poll future timeout to make the test fast
  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                         = Some(DynamoDBEndpoint("localhost", 8001)),
    region                           = Some("eu-central-1"),
    credentials                      = Some(AWSCredentials("dummy", "dummy", None)),
    table                            = "PollFutureTimeoutSource",
    scanSegments                     = None,
    readThroughput                   = None,
    throughputReadPercent            = None,
    maxMapTasks                      = None,
    streamingPollIntervalSeconds     = Some(1),
    streamingMaxConsecutiveErrors    = Some(10),
    streamingPollingPoolSize         = Some(2),
    streamingLeaseDurationMs         = Some(60000L),
    streamingPollFutureTimeoutSeconds = Some(3)
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

  test("partial results collected when one shard poll hangs beyond timeout") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shardFast = Shard.builder().shardId("shard-fast").build()
    val shardSlow = Shard.builder().shardId("shard-slow").build()
    poller.listShardsFn.set((_, _) => Seq(shardFast, shardSlow))

    val fastShardRecordWritten = new AtomicBoolean(false)
    val slowShardHung = new AtomicBoolean(false)
    val slowShardLatch = new CountDownLatch(1)
    val pollCycleCount = new AtomicInteger(0)

    poller.getShardIteratorFn.set((_, _, shardId, _) => s"iter-$shardId")
    poller.getShardIteratorAfterSequenceFn.set((_, _, shardId, _) => s"iter-after-$shardId")

    poller.getRecordsFn.set { (_, iterator, _) =>
      if (iterator.contains("shard-slow")) {
        // First call blocks until interrupted (or latch is released) to trigger the timeout.
        // Using a latch instead of Thread.sleep ensures clean interrupt propagation.
        if (!slowShardHung.getAndSet(true)) {
          slowShardLatch.await() // Blocks until interrupted by pool shutdown
        }
        (Seq.empty, Some("next-iter-slow"))
      } else {
        pollCycleCount.incrementAndGet()
        if (!fastShardRecordWritten.getAndSet(true)) {
          // Fast shard returns a record on first poll
          val record = Record
            .builder()
            .eventName("INSERT")
            .dynamodb(
              StreamRecord
                .builder()
                .keys(Map("id" -> AttributeValue.fromS("fast-item")).asJava)
                .newImage(
                  Map(
                    "id"    -> AttributeValue.fromS("fast-item"),
                    "value" -> AttributeValue.fromS("val-fast")
                  ).asJava
                )
                .sequenceNumber("seq-fast-1")
                .build()
            )
            .build()
          (Seq(record), Some("next-iter-fast"))
        } else
          (Seq.empty, Some("next-iter-fast"))
      }
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
      // Wait for the fast shard's record to be written to the target.
      // If partial result collection works, the fast shard's record will be
      // processed even though the slow shard timed out.
      Eventually(timeoutMs = 15000) {
        val item = targetAlternator()
          .getItem(
            GetItemRequest
              .builder()
              .tableName(targetTable)
              .key(Map("id" -> AttributeValue.fromS("fast-item")).asJava)
              .build()
          )
          .item()
        item != null && !item.isEmpty
      }(
        "Expected fast shard's record to be written to target despite slow shard timeout"
      )

      // Verify the fast shard has a checkpoint (proving items were processed)
      val ckptFast =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-fast")
      assertEquals(
        ckptFast,
        Some("seq-fast-1"),
        "Fast shard should be checkpointed even when slow shard times out"
      )
    } finally
      handle.stop()
  }
}
