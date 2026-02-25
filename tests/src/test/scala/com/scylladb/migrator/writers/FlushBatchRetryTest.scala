package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, TargetSettings }
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Tests for the batch write logic in BatchWriter.run(), which exercises the flushAndClearBatch
  * retry path indirectly through successful batch writes.
  */
class FlushBatchRetryTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "FlushBatchRetryTest"
  private val operationTypeColumn = BatchWriter.operationTypeColumn
  private val putOperation = BatchWriter.putOperation
  private val deleteOperation = BatchWriter.deleteOperation

  private val targetSettings = TargetSettings.DynamoDB(
    table                       = tableName,
    region                      = Some("eu-central-1"),
    endpoint                    = Some(DynamoDBEndpoint("localhost", 8000)),
    credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
    streamChanges               = false,
    skipInitialSnapshotTransfer = None,
    writeThroughput             = None,
    throughputWritePercent      = None
  )

  private def createTable(): TableDescription = {
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(tableName).build()
      )
    catch { case _: Exception => () }

    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
        )
        .build()
    )
    targetAlternator()
      .waiter()
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build())
    targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
      .table()
  }

  private def makeItem(
    id: String,
    isPut: Boolean
  ): util.Map[String, AttributeValue] = {
    val item = new util.HashMap[String, AttributeValue]()
    item.put("id", AttributeValue.fromS(id))
    if (isPut) {
      item.put("value", AttributeValue.fromS(s"val-$id"))
      item.put(operationTypeColumn, putOperation)
    } else {
      item.put(operationTypeColumn, deleteOperation)
    }
    item
  }

  private def scanAll(): List[Map[String, AttributeValue]] =
    targetAlternator()
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .asScala
      .map(_.asScala.toMap)
      .toList

  test("run() handles batch of exactly 25 items (one full batch)") {
    val tableDesc = createTable()
    val items = (1 to 25).map(i => makeItem(s"item-$i", isPut = true))

    BatchWriter.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 25)
  }

  test("run() handles batch exceeding 25 items (multiple flushes)") {
    val tableDesc = createTable()
    val items = (1 to 60).map(i => makeItem(s"item-$i", isPut = true))

    BatchWriter.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 60)
  }

  test("run() handles duplicate keys in batch (forces flush to preserve ordering)") {
    val tableDesc = createTable()
    val items = Seq(
      makeItem("dup-1", isPut = true),
      makeItem("dup-2", isPut = true),
      makeItem("dup-1", isPut = true) // duplicate key forces a flush
    )

    BatchWriter.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 2) // dup-1 and dup-2
  }

  test("run() handles mixed puts and deletes") {
    val tableDesc = createTable()
    // First insert some items
    val inserts = (1 to 5).map(i => makeItem(s"item-$i", isPut = true))
    BatchWriter.run(inserts, targetSettings, Map.empty, tableDesc, targetAlternator())
    assertEquals(scanAll().size, 5)

    // Now delete some and add new ones
    val mixed = Seq(
      makeItem("item-2", isPut = false), // delete
      makeItem("item-4", isPut = false), // delete
      makeItem("item-6", isPut = true) // insert
    )
    BatchWriter.run(mixed, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    val ids = results.map(_("id").s()).toSet
    assertEquals(ids, Set("item-1", "item-3", "item-5", "item-6"))
  }

  test("run() with empty items is a no-op") {
    val tableDesc = createTable()
    // Should not throw
    BatchWriter.run(Seq.empty, targetSettings, Map.empty, tableDesc, targetAlternator())
    assertEquals(scanAll().size, 0)
  }

  /** Creates a DynamoDB client proxy that delegates batchWriteItem to a custom function and
    * forwards all other calls to the real client.
    */
  private def proxyClient(
    real: DynamoDbClient,
    batchWriteFn: BatchWriteItemRequest => BatchWriteItemResponse
  ): DynamoDbClient = {
    val handler = new java.lang.reflect.InvocationHandler {
      def invoke(
        proxy: Any,
        method: java.lang.reflect.Method,
        args: Array[AnyRef]
      ): AnyRef =
        if (method.getName == "batchWriteItem" && args != null && args.length == 1)
          batchWriteFn(args(0).asInstanceOf[BatchWriteItemRequest])
        else if (args != null)
          method.invoke(real, args: _*)
        else
          method.invoke(real)
    }
    java.lang.reflect.Proxy
      .newProxyInstance(
        classOf[DynamoDbClient].getClassLoader,
        Array(classOf[DynamoDbClient]),
        handler
      )
      .asInstanceOf[DynamoDbClient]
  }

  test("flushAndClearBatch retries on unprocessed items and succeeds") {
    createTable()
    val callCount = new AtomicInteger(0)

    val items = (1 to 3).map(i => makeItem(s"retry-item-$i", isPut = true))
    val writeRequests = items.map { item =>
      val finalItem = new util.HashMap[String, AttributeValue](item)
      finalItem.remove(operationTypeColumn)
      WriteRequest
        .builder()
        .putRequest(PutRequest.builder().item(finalItem).build())
        .build()
    }

    val proxy = proxyClient(
      targetAlternator(),
      { request =>
        val count = callCount.incrementAndGet()
        if (count == 1) {
          // First call: write first 2 items, return last item as unprocessed
          val allWrites = request.requestItems().get(tableName)
          val processed = allWrites.subList(0, 2)
          val unprocessed = allWrites.subList(2, 3)
          // Write the processed items for real
          targetAlternator().batchWriteItem(
            BatchWriteItemRequest
              .builder()
              .requestItems(Map(tableName -> processed).asJava)
              .build()
          )
          BatchWriteItemResponse
            .builder()
            .unprocessedItems(Map(tableName -> unprocessed).asJava)
            .build()
        } else {
          // Subsequent calls: write everything
          targetAlternator().batchWriteItem(request)
        }
      }
    )

    val batch = new util.ArrayList[WriteRequest](writeRequests.asJava)
    val keys = new util.HashSet[String]()
    BatchWriter.flushAndClearBatch(proxy, tableName, batch, keys)

    // Verify all 3 items were written
    val results = scanAll()
    assertEquals(results.size, 3, s"Expected 3 items after retry, got ${results.size}")
    // Verify the retry actually happened
    assert(callCount.get() >= 2, s"Expected at least 2 batchWriteItem calls, got ${callCount.get()}")
  }

  test("flushAndClearBatch throws BatchWriteExhaustedException after max retries") {
    createTable()
    val callCount = new AtomicInteger(0)

    val item = makeItem("exhaust-item-1", isPut = true)
    val finalItem = new util.HashMap[String, AttributeValue](item)
    finalItem.remove(operationTypeColumn)
    val writeRequest = WriteRequest
      .builder()
      .putRequest(PutRequest.builder().item(finalItem).build())
      .build()

    // Client that always returns the item as unprocessed
    val proxy = proxyClient(
      targetAlternator(),
      { _ =>
        callCount.incrementAndGet()
        BatchWriteItemResponse
          .builder()
          .unprocessedItems(Map(tableName -> Seq(writeRequest).asJava).asJava)
          .build()
      }
    )

    val batch = new util.ArrayList[WriteRequest]()
    batch.add(writeRequest)
    val keys = new util.HashSet[String]()

    val ex = intercept[BatchWriteExhaustedException] {
      BatchWriter.flushAndClearBatch(proxy, tableName, batch, keys)
    }
    assert(ex.unprocessedCount == 1, s"Expected 1 unprocessed item, got ${ex.unprocessedCount}")
    assert(
      callCount.get() > 1,
      s"Expected multiple retry attempts, got ${callCount.get()}"
    )
  }
}
