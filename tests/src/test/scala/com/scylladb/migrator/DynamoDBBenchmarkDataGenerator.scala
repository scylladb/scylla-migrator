package com.scylladb.migrator

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BatchWriteItemRequest,
  BillingMode,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  PutRequest,
  ResourceNotFoundException,
  ScalarAttributeType,
  WriteRequest
}

import java.util.concurrent.{ Executors, Semaphore, ThreadFactory, TimeUnit }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object DynamoDBBenchmarkDataGenerator {

  /** Create a simple DynamoDB table with on-demand billing for maximum throughput. */
  def createSimpleTable(client: DynamoDbClient, tableName: String): Unit = {
    // Drop if exists
    val deleted =
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
        true
      } catch {
        case _: ResourceNotFoundException => false
      }
    if (deleted)
      client
        .waiter()
        .waitUntilTableNotExists(
          DescribeTableRequest.builder().tableName(tableName).build()
        )

    client.createTable(
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .keySchema(
          KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .build()
    )

    client
      .waiter()
      .waitUntilTableExists(
        DescribeTableRequest.builder().tableName(tableName).build()
      )
  }

  /** Insert rows using BatchWriteItem (25 items per batch, DynamoDB maximum).
    *
    * Uses a thread pool with a semaphore to run multiple batches concurrently
    * while limiting the number of in-flight requests.
    */
  def insertSimpleRows(
    client: DynamoDbClient,
    tableName: String,
    rowCount: Int,
    batchSize: Int = 25,
    maxConcurrent: Int = 32
  ): Unit = {
    val daemonThreadFactory: ThreadFactory = new ThreadFactory {
      private val counter = new AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"ddb-batch-writer-${counter.getAndIncrement()}")
        t.setDaemon(true)
        t
      }
    }
    val executor = Executors.newFixedThreadPool(maxConcurrent, daemonThreadFactory)
    val semaphore = new Semaphore(maxConcurrent)
    val firstError = new AtomicReference[Throwable](null)

    try {
      for (batch <- (0 until rowCount).grouped(batchSize) if firstError.get() == null) {
        val writeRequests = batch.map { i =>
          WriteRequest
            .builder()
            .putRequest(
              PutRequest
                .builder()
                .item(
                  Map(
                    "id"   -> AttributeValue.fromS(s"id-$i"),
                    "col1" -> AttributeValue.fromS(s"value-$i"),
                    "col2" -> AttributeValue.fromN(i.toString),
                    "col3" -> AttributeValue.fromN((i.toLong * 1000L).toString)
                  ).asJava
                )
                .build()
            )
            .build()
        }

        val request = BatchWriteItemRequest
          .builder()
          .requestItems(Map(tableName -> writeRequests.asJava).asJava)
          .build()

        semaphore.acquire()
        executor.submit(new Runnable {
          def run(): Unit =
            try writeBatchWithRetry(client, request)
            catch {
              case e: Throwable =>
                firstError.compareAndSet(null, e)
            } finally semaphore.release()
        })
      }

      // Wait for all remaining tasks to complete
      semaphore.acquire(maxConcurrent)
      semaphore.release(maxConcurrent)
    } finally {
      executor.shutdown()
      if (!executor.awaitTermination(10, TimeUnit.MINUTES))
        throw new RuntimeException("DynamoDB batch insert threads did not terminate within 10 minutes")
    }

    val error = firstError.get()
    if (error != null)
      throw new RuntimeException(s"Batch write failed for table $tableName", error)
  }

  /** Retry batch writes until all items are processed, up to maxRetries attempts.
    * Uses exponential backoff with jitter to avoid thundering herd.
    */
  @tailrec
  private def writeBatchWithRetry(
    client: DynamoDbClient,
    request: BatchWriteItemRequest,
    retriesLeft: Int = 50,
    baseDelayMs: Long = 50
  ): Unit = {
    val response = client.batchWriteItem(request)
    val unprocessed = response.unprocessedItems()
    if (unprocessed != null && !unprocessed.isEmpty) {
      require(retriesLeft > 0, s"Exceeded max retries for batch write with ${unprocessed.size()} unprocessed items")
      val delay = baseDelayMs + java.util.concurrent.ThreadLocalRandom.current().nextLong(baseDelayMs)
      Thread.sleep(delay)
      writeBatchWithRetry(
        client,
        BatchWriteItemRequest.builder().requestItems(unprocessed).build(),
        retriesLeft - 1,
        Math.min(baseDelayMs * 2, 5000)
      )
    }
  }

  /** Verify a sample of rows in the target to detect data corruption.
    *
    * Note: uses Scala's `assert` rather than munit assertions, so failures
    * appear as `java.lang.AssertionError` (test errors) rather than munit
    * `ComparisonFailException` with diffs. This is acceptable because the
    * method lives outside any test suite and the assertion messages include
    * expected/actual values.
    */
  def spotCheckRows(client: DynamoDbClient, tableName: String, rowCount: Int): Unit = {
    import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
    val sampleIndices = BenchmarkDataGenerator.sampleIndices(rowCount)
    for (i <- sampleIndices) {
      val resp = client.getItem(
        GetItemRequest
          .builder()
          .tableName(tableName)
          .key(Map("id" -> AttributeValue.fromS(s"id-$i")).asJava)
          .build()
      )
      val item = resp.item()
      assert(item != null && !item.isEmpty, s"Spot-check: row id-$i not found in target")
      val col1 = item.get("col1")
      assert(col1 != null, s"Spot-check: col1 attribute missing for id-$i")
      assert(
        col1.s() == s"value-$i",
        s"Spot-check: col1 mismatch for id-$i: expected value-$i, got ${col1.s()}"
      )
      val col2 = item.get("col2")
      assert(col2 != null, s"Spot-check: col2 attribute missing for id-$i")
      assert(
        col2.n() == i.toString,
        s"Spot-check: col2 mismatch for id-$i: expected $i, got ${col2.n()}"
      )
      val col3 = item.get("col3")
      assert(col3 != null, s"Spot-check: col3 attribute missing for id-$i")
      assert(
        col3.n() == (i.toLong * 1000L).toString,
        s"Spot-check: col3 mismatch for id-$i: expected ${i.toLong * 1000L}, got ${col3.n()}"
      )
    }
  }

  /** Count all items in a DynamoDB table using paginated scan. */
  def countItems(client: DynamoDbClient, tableName: String): Long = {
    import software.amazon.awssdk.services.dynamodb.model.ScanRequest
    client
      .scanPaginator(
        ScanRequest.builder().tableName(tableName).select("COUNT").build()
      )
      .stream()
      .mapToLong(_.count().toLong)
      .sum()
  }
}
