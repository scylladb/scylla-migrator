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

/** Tests for TrimmedDataAccessException recovery in pollOwnedShards.
  *
  * When stream records older than 24 hours are purged, DynamoDB returns a
  * TrimmedDataAccessException. The worker should reset the shard to TRIM_HORIZON
  * and continue processing.
  */
class TrimmedDataRecoveryTest extends StreamReplicationTestFixture {

  protected val targetTable = "TrimmedDataRecoveryTarget"
  protected lazy val checkpointTable =
    DynamoStreamReplication.buildCheckpointTableName(sourceSettings)

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "TrimmedDataRecoverySource",
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

  test("recovers from TrimmedDataAccessException by resetting to TRIM_HORIZON") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-trimmed-1").build()
    poller.listShardsFn.set((_, _) => Seq(shard))

    val getRecordsCallCount = new AtomicInteger(0)

    // First call throws TrimmedDataAccessException, subsequent calls succeed
    poller.getRecordsFn.set { (_, _, _) =>
      val callNum = getRecordsCallCount.incrementAndGet()
      if (callNum == 1) {
        throw DynamoDbException
          .builder()
          .message("Requested data has been trimmed")
          .awsErrorDetails(
            software.amazon.awssdk.awscore.exception.AwsErrorDetails
              .builder()
              .errorCode("TrimmedDataAccessException")
              .errorMessage("Requested data has been trimmed")
              .build()
          )
          .build()
      }
      (Seq.empty, Some("refreshed-iter"))
    }

    val trimHorizonRequested = new AtomicBoolean(false)
    poller.getShardIteratorFn.set { (_, _, _, iterType) =>
      if (iterType == ShardIteratorType.TRIM_HORIZON) trimHorizonRequested.set(true)
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

    try {
      Eventually(timeoutMs = 10000) {
        getRecordsCallCount.get() >= 2
      }(
        s"Expected at least 2 getRecords calls (1 trimmed + 1 recovered), got ${getRecordsCallCount.get()}"
      )

      assert(
        trimHorizonRequested.get(),
        "Expected TRIM_HORIZON iterator request after TrimmedDataAccessException"
      )
    } finally
      handle.stop()
  }
}
