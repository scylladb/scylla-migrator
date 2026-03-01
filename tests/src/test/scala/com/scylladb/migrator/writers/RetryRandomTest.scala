package com.scylladb.migrator.writers

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

class RetryRandomTest extends munit.FunSuite {

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

  test("retryRandom: succeeds on first try") {
    var callCount = 0
    val result = DefaultCheckpointManager.retryRandom(
      { callCount += 1; 42 },
      numRetriesLeft   = 3,
      maxBackOffMillis = 10
    )
    assertEquals(result, 42)
    assertEquals(callCount, 1)
  }

  test("retryRandom: retries on retryable DynamoDbException and eventually succeeds") {
    var callCount = 0
    val result = DefaultCheckpointManager.retryRandom(
      {
        callCount += 1
        if (callCount < 3)
          throw makeDynamoException("LimitExceededException")
        "success"
      },
      numRetriesLeft   = 4,
      maxBackOffMillis = 10
    )
    assertEquals(result, "success")
    assertEquals(callCount, 3)
  }

  test("retryRandom: exhausts retries and throws") {
    var callCount = 0
    val ex = intercept[DynamoDbException] {
      DefaultCheckpointManager.retryRandom(
        {
          callCount += 1
          throw makeDynamoException("ProvisionedThroughputExceededException")
        },
        numRetriesLeft   = 3,
        maxBackOffMillis = 10
      )
    }
    assertEquals(callCount, 3)
    assertEquals(ex.awsErrorDetails().errorCode(), "ProvisionedThroughputExceededException")
  }

  test("retryRandom: non-retryable exception thrown immediately without retry") {
    var callCount = 0
    val ex = intercept[DynamoDbException] {
      DefaultCheckpointManager.retryRandom(
        {
          callCount += 1
          throw makeDynamoException("ValidationException")
        },
        numRetriesLeft   = 5,
        maxBackOffMillis = 10
      )
    }
    assertEquals(callCount, 1)
    assertEquals(ex.awsErrorDetails().errorCode(), "ValidationException")
  }

  test("retryRandom: non-DynamoDB exception thrown immediately") {
    var callCount = 0
    val ex = intercept[RuntimeException] {
      DefaultCheckpointManager.retryRandom(
        {
          callCount += 1
          throw new RuntimeException("generic error")
        },
        numRetriesLeft   = 5,
        maxBackOffMillis = 10
      )
    }
    assertEquals(callCount, 1)
    assertEquals(ex.getMessage, "generic error")
  }
}
