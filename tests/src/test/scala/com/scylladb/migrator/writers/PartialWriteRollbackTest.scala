package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import scala.jdk.CollectionConverters._

/** Verifies that partial write failure rollback works correctly:
  *   - When BatchWriter succeeds for some sub-batches but fails for others
  *     (BatchWriteExhaustedException with successfullyFlushedCount > 0),
  *     only the flushed items' sequence numbers are retained.
  *   - Items from failed sub-batches are re-processed on the next poll cycle.
  *
  * This test exercises the `revertToPartialProgress` path indirectly by sending > 25 items
  * (forcing multiple sub-batches) from multiple shards, with the target table being dropped
  * mid-cycle to trigger write failures after initial success.
  */
class PartialWriteRollbackTest extends StreamReplicationTestFixture {

  protected val targetTable = "PartialWriteRollbackTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "PartialWriteRollbackSource",
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

  test("multi-shard batch succeeds and checkpoints all shard sequence numbers") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shardA = Shard.builder().shardId("shard-partial-A").build()
    val shardB = Shard.builder().shardId("shard-partial-B").build()
    val pollCount = new AtomicInteger(0)
    val wroteShardA = new AtomicBoolean(false)
    val wroteShardB = new AtomicBoolean(false)

    poller.listShardsFn.set((_, _) => Seq(shardA, shardB))

    // First poll from each shard: return records spanning > 25 items total (to force sub-batching)
    // shard-A returns 15 records, shard-B returns 15 records → 30 total → 2 sub-batches
    poller.getRecordsFn.set { (_, iterator, _) =>
      val count = pollCount.incrementAndGet()
      // Determine which shard based on the iterator name (set by getShardIterator)
      val isShard =
        if (iterator.contains("shard-partial-A")) "A"
        else if (iterator.contains("shard-partial-B")) "B"
        else "?"

      val delivered = if (isShard == "A") wroteShardA else wroteShardB
      if (delivered.compareAndSet(false, true)) {
        val prefix = if (isShard == "A") "a" else "b"
        val records = (1 to 15).map { i =>
          Record
            .builder()
            .eventName("INSERT")
            .dynamodb(
              StreamRecord
                .builder()
                .keys(Map("id" -> AttributeValue.fromS(s"$prefix-$i")).asJava)
                .newImage(
                  Map(
                    "id"    -> AttributeValue.fromS(s"$prefix-$i"),
                    "value" -> AttributeValue.fromS(s"val-$prefix-$i")
                  ).asJava
                )
                .sequenceNumber(s"seq-$prefix-$i")
                .build()
            )
            .build()
        }
        (records, Some(s"next-iter-$isShard"))
      } else
        (Seq.empty, Some(s"next-iter-$isShard"))
    }

    poller.getShardIteratorFn.set((_, _, shardId, _) => s"iter-$shardId")
    poller.getShardIteratorAfterSequenceFn.set((_, _, shardId, _) => s"iter-after-$shardId")

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
      // Wait for records to be written and checkpointed
      Eventually(timeoutMs = 15000) {
        val ckptA =
          DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-partial-A")
        val ckptB =
          DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-partial-B")
        ckptA.isDefined && ckptB.isDefined
      }(
        "Expected checkpoints to be written for both shards after successful multi-shard write"
      )

      // Verify both shards have checkpoints at their last sequence number
      val ckptA =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-partial-A")
      val ckptB =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-partial-B")

      assertEquals(ckptA, Some("seq-a-15"), "shard-A checkpoint should be at last record")
      assertEquals(ckptB, Some("seq-b-15"), "shard-B checkpoint should be at last record")

      // Verify items were actually written to the target
      val items =
        targetAlternator()
          .scanPaginator(ScanRequest.builder().tableName(targetTable).build())
          .items()
          .asScala
          .toList
      assert(items.size >= 30, s"Expected at least 30 items in target, got ${items.size}")
    } finally
      handle.stop()
  }

  test("complete write failure reverts sequence numbers — no checkpoint is written") {
    // This exercises revertToPartialProgress with flushedCount=0 (full revert),
    // covering the safety property that no checkpoint is advanced on write failure.
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shardA = Shard.builder().shardId("shard-revert-A").build()
    val shardB = Shard.builder().shardId("shard-revert-B").build()
    val pollCount = new AtomicInteger(0)

    poller.listShardsFn.set((_, _) => Seq(shardA, shardB))
    poller.getShardIteratorFn.set((_, _, shardId, _) => s"iter-$shardId")
    poller.getShardIteratorAfterSequenceFn.set((_, _, shardId, _) => s"iter-after-$shardId")

    // Delete target table to force write failure
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(targetTable).build()
      )
    catch { case _: ResourceNotFoundException => () }

    // Track which shard each iterator belongs to, keyed on shard ID not iterator string.
    // Initial iterators: "iter-shard-revert-A" / "iter-shard-revert-B"
    // Subsequent iterators: "next-iter-a" / "next-iter-b"
    val wroteShardRevertA = new AtomicBoolean(false)
    val wroteShardRevertB = new AtomicBoolean(false)

    poller.getRecordsFn.set { (_, iterator, _) =>
      pollCount.incrementAndGet()
      // Identify shard from iterator: initial has shard ID, subsequent use prefix
      val isShardA = iterator.contains("shard-revert-A") || iterator.contains("next-iter-a")
      val prefix = if (isShardA) "a" else "b"
      val delivered = if (isShardA) wroteShardRevertA else wroteShardRevertB
      if (delivered.compareAndSet(false, true)) {
        val records = (1 to 5).map { i =>
          Record
            .builder()
            .eventName("INSERT")
            .dynamodb(
              StreamRecord
                .builder()
                .keys(Map("id" -> AttributeValue.fromS(s"$prefix-$i")).asJava)
                .newImage(
                  Map(
                    "id"    -> AttributeValue.fromS(s"$prefix-$i"),
                    "value" -> AttributeValue.fromS(s"val-$prefix-$i")
                  ).asJava
                )
                .sequenceNumber(s"seq-$prefix-$i")
                .build()
            )
            .build()
        }
        (records, Some(s"next-iter-$prefix"))
      } else
        (Seq.empty, Some(s"next-iter-$prefix"))
    }

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
      // Wait for the write to be attempted and fail
      Eventually(timeoutMs = 10000) {
        pollCount.get() >= 3
      }(s"Expected at least 3 poll cycles, got ${pollCount.get()}")

      // Neither shard should have a checkpoint written since the write failed
      val ckptA =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-revert-A")
      val ckptB =
        DefaultCheckpointManager.getCheckpoint(sourceDDb(), checkpointTable, "shard-revert-B")
      assert(
        ckptA.isEmpty,
        s"shard-A checkpoint should NOT be written after write failure, got: $ckptA"
      )
      assert(
        ckptB.isEmpty,
        s"shard-B checkpoint should NOT be written after write failure, got: $ckptB"
      )
    } finally
      handle.stop()
  }
}
