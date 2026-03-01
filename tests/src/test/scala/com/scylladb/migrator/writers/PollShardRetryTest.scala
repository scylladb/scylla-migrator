package com.scylladb.migrator.writers

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DynamoDbException,
  Record,
  StreamRecord
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import scala.jdk.CollectionConverters._

class PollShardRetryTest extends munit.FunSuite {

  private def makeDynamoException(errorCode: String): DynamoDbException =
    DynamoDbException
      .builder()
      .awsErrorDetails(
        AwsErrorDetails
          .builder()
          .errorCode(errorCode)
          .errorMessage("test")
          .build()
      )
      .message("test")
      .build()
      .asInstanceOf[DynamoDbException]

  private def makeRecord(seqNum: String): Record =
    Record
      .builder()
      .eventName("INSERT")
      .dynamodb(
        StreamRecord
          .builder()
          .keys(Map("id" -> AttributeValue.fromS("k1")).asJava)
          .newImage(Map("id" -> AttributeValue.fromS("k1")).asJava)
          .sequenceNumber(seqNum)
          .build()
      )
      .build()

  // Null is safe here — pollShard passes it through to the poller, which is our test double
  private val nullClient = null.asInstanceOf[DynamoDbStreamsClient]

  test("pollShard: succeeds on first try") {
    val poller = new TestStreamPoller
    val records = Seq(makeRecord("001"))
    poller.getRecordsFn.set((_, _, _) => (records, Some("next-iter")))

    val (shardId, recs, nextIter) =
      StreamReplicationWorker.pollShard(nullClient, "shard-1", "iter-1", poller = poller)
    assertEquals(shardId, "shard-1")
    assertEquals(recs.size, 1)
    assertEquals(nextIter, Some("next-iter"))
  }

  test("pollShard: retries on LimitExceededException then succeeds") {
    val poller = new TestStreamPoller
    var callCount = 0
    poller.getRecordsFn.set((_, _, _) => {
      callCount += 1
      if (callCount <= 2) throw makeDynamoException("LimitExceededException")
      (Seq(makeRecord("002")), Some("next"))
    })

    val (_, recs, _) =
      StreamReplicationWorker.pollShard(
        nullClient,
        "shard-1",
        "iter-1",
        maxRetries = 3,
        poller     = poller
      )
    assertEquals(callCount, 3)
    assertEquals(recs.size, 1)
  }

  test("pollShard: retries on ProvisionedThroughputExceededException") {
    val poller = new TestStreamPoller
    var callCount = 0
    poller.getRecordsFn.set((_, _, _) => {
      callCount += 1
      if (callCount <= 1) throw makeDynamoException("ProvisionedThroughputExceededException")
      (Seq(makeRecord("003")), None)
    })

    val (_, recs, nextIter) =
      StreamReplicationWorker.pollShard(
        nullClient,
        "shard-1",
        "iter-1",
        maxRetries = 3,
        poller     = poller
      )
    assertEquals(callCount, 2)
    assertEquals(recs.size, 1)
    assertEquals(nextIter, None)
  }

  test("pollShard: exhausts retries and throws") {
    val poller = new TestStreamPoller
    var callCount = 0
    poller.getRecordsFn.set((_, _, _) => {
      callCount += 1
      throw makeDynamoException("LimitExceededException")
    })

    val ex = intercept[DynamoDbException] {
      StreamReplicationWorker.pollShard(
        nullClient,
        "shard-1",
        "iter-1",
        maxRetries = 2,
        poller     = poller
      )
    }
    assertEquals(ex.awsErrorDetails().errorCode(), "LimitExceededException")
    // 1 initial + 2 retries = 3 calls
    assertEquals(callCount, 3)
  }

  test("pollShard: non-retryable DynamoDbException thrown immediately") {
    val poller = new TestStreamPoller
    var callCount = 0
    poller.getRecordsFn.set((_, _, _) => {
      callCount += 1
      throw makeDynamoException("ValidationException")
    })

    intercept[DynamoDbException] {
      StreamReplicationWorker.pollShard(
        nullClient,
        "shard-1",
        "iter-1",
        maxRetries = 5,
        poller     = poller
      )
    }
    assertEquals(callCount, 1)
  }

  test("pollShard: non-DynamoDB exception thrown immediately") {
    val poller = new TestStreamPoller
    var callCount = 0
    poller.getRecordsFn.set((_, _, _) => {
      callCount += 1
      throw new RuntimeException("network error")
    })

    intercept[RuntimeException] {
      StreamReplicationWorker.pollShard(
        nullClient,
        "shard-1",
        "iter-1",
        maxRetries = 5,
        poller     = poller
      )
    }
    assertEquals(callCount, 1)
  }

  test("pollShard: shard closed (nextIterator = None)") {
    val poller = new TestStreamPoller
    poller.getRecordsFn.set((_, _, _) => (Seq(makeRecord("004")), None))

    val (shardId, recs, nextIter) =
      StreamReplicationWorker.pollShard(nullClient, "shard-1", "iter-1", poller = poller)
    assertEquals(shardId, "shard-1")
    assertEquals(recs.size, 1)
    assertEquals(nextIter, None)
  }
}
