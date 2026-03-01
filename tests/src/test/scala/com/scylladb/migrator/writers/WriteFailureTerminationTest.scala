package com.scylladb.migrator.writers

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

/** Verifies that sustained write failures accumulate `consecutiveErrors` and eventually
  * trigger worker termination via the `maxConsecutiveErrors` threshold.
  *
  * Before the fix, `consecutiveErrors` was reset after a successful poll but BEFORE the write,
  * so a pattern of "poll ok, write fail" never accumulated errors and never triggered termination.
  */
class WriteFailureTerminationTest extends StreamReplicationTestFixture {

  protected val targetTable = "WriteFailureTermTarget"
  protected val checkpointTable = "migrator_WriteFailureTermSource"
  override protected def createTargetTableOnSetup: Boolean = false

  // Use a very small maxConsecutiveErrors so the test terminates quickly
  private val maxErrors = 3

  private val sourceSettings = SourceSettings.DynamoDB(
    endpoint                      = Some(DynamoDBEndpoint("http://localhost", 8001)),
    region                        = Some("eu-central-1"),
    credentials                   = Some(AWSCredentials("dummy", "dummy", None)),
    table                         = "WriteFailureTermSource",
    scanSegments                  = None,
    readThroughput                = None,
    throughputReadPercent         = None,
    maxMapTasks                   = None,
    streamingPollIntervalSeconds  = Some(1),
    streamingMaxConsecutiveErrors = Some(maxErrors),
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

  test("sustained write failures trigger termination via maxConsecutiveErrors") {
    val poller = new TestStreamPoller
    poller.getStreamArnFn.set((_, _) => "arn:aws:dynamodb:us-east-1:000:table/t/stream/s")

    val shard = Shard.builder().shardId("shard-term-1").build()
    val pollCount = new AtomicInteger(0)

    poller.listShardsFn.set((_, _) => Seq(shard))

    // Every poll returns records so BatchWriter.run is invoked every cycle
    poller.getRecordsFn.set((_, _, _) => {
      pollCount.incrementAndGet()
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
            .sequenceNumber(s"seq-${pollCount.get()}")
            .build()
        )
        .build()
      (Seq(record), Some("next-iter"))
    })

    // Target table does NOT exist, so BatchWriter.run() will throw on every cycle
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
      // The worker should terminate after maxConsecutiveErrors write failures.
      // With maxErrors=3 and pollInterval=1s, this should happen within ~10s.
      val terminated = handle.awaitTermination(20, TimeUnit.SECONDS)
      assert(
        terminated,
        s"Worker should have terminated after $maxErrors consecutive write failures, " +
          s"but timed out. Poll count: ${pollCount.get()}"
      )
    } finally
      handle.stop()
  }
}
