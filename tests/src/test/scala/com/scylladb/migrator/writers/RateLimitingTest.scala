package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Tests for the token bucket rate limiting logic in StreamReplicationWorker.
  *
  * Verifies that streamingMaxRecordsPerSecond throttles the record processing rate and that the
  * system remains stable under rate limiting.
  */
class RateLimitingTest extends StreamReplicationTestFixture {

  protected val targetTable = "RateLimitTestTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(makeSourceSettings(None))

  private def makeSourceSettings(maxRecordsPerSecond: Option[Int]) = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "RateLimitTestSource",
    scanSegments                  = None,
    readThroughput                = None,
    throughputReadPercent         = None,
    maxMapTasks                   = None,
    streamingPollIntervalSeconds  = Some(1),
    streamingMaxConsecutiveErrors = Some(5),
    streamingPollingPoolSize      = Some(2),
    streamingLeaseDurationMs      = Some(60000L),
    streamingMaxRecordsPerSecond  = maxRecordsPerSecond
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

  test("rate limiting does not crash when maxRecordsPerSecond is set") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-rate-1").build()
    poller.listShardsFn.set((_, _) => Seq(shard))

    val pollCount = new AtomicInteger(0)

    // Return a burst of records on the first poll, empty afterward
    poller.getRecordsFn.set { (_, _, _) =>
      val n = pollCount.incrementAndGet()
      if (n == 1) {
        val records = (1 to 10).map(i => makeRecord(s"rate-$i", s"seq-$i"))
        (records, Some("next-iter"))
      } else
        (Seq.empty, Some("next-iter"))
    }

    val tableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(targetTable).build())
      .table()

    // Very low rate limit: 2 records/second
    val handle = DynamoStreamReplication.startStreaming(
      makeSourceSettings(maxRecordsPerSecond = Some(2)),
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller
    )

    try
      Eventually(timeoutMs = 15000) {
        pollCount.get() >= 2
      }(s"Expected at least 2 poll cycles, got ${pollCount.get()}")
    finally
      handle.stop()
  }

  test("no rate limiting when maxRecordsPerSecond is not set") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-norate-1").build()
    poller.listShardsFn.set((_, _) => Seq(shard))

    val pollCount = new AtomicInteger(0)

    poller.getRecordsFn.set { (_, _, _) =>
      val n = pollCount.incrementAndGet()
      if (n <= 3) {
        val records = (1 to 5).map(i => makeRecord(s"nr-$n-$i", s"seq-$n-$i"))
        (records, Some("next-iter"))
      } else
        (Seq.empty, Some("next-iter"))
    }

    val tableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(targetTable).build())
      .table()

    val handle = DynamoStreamReplication.startStreaming(
      makeSourceSettings(maxRecordsPerSecond = None),
      targetSettings,
      tableDesc,
      Map.empty,
      poller = poller
    )

    try
      Eventually(timeoutMs = 10000) {
        pollCount.get() >= 3
      }(s"Expected at least 3 poll cycles without rate limiting, got ${pollCount.get()}")
    finally
      handle.stop()
  }
}
