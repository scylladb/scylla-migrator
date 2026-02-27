package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import scala.jdk.CollectionConverters._

/** Tests for flushBatch retry behavior with unprocessed items.
  *
  * Since DynamoDB Local does not return unprocessed items under normal conditions, these tests
  * exercise the batch boundary conditions and verify the retry logic through run() which calls
  * flushBatch internally.
  */
class FlushBatchUnprocessedTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "FlushBatchUnprocessedTest"

  private val targetSettings = com.scylladb.migrator.config.TargetSettings.DynamoDB(
    table                       = tableName,
    region                      = Some("eu-central-1"),
    endpoint = Some(com.scylladb.migrator.config.DynamoDBEndpoint("http://localhost", 8000)),
    credentials =
      Some(com.scylladb.migrator.config.AWSCredentials("dummy", "dummy", None)),
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
  ): Option[util.Map[String, AttributeValue]] = {
    val item = new util.HashMap[String, AttributeValue]()
    item.put("id", AttributeValue.fromS(id))
    if (isPut) {
      item.put("value", AttributeValue.fromS(s"val-$id"))
      item.put(DynamoStreamReplication.operationTypeColumn, DynamoStreamReplication.putOperation)
    } else {
      item.put(DynamoStreamReplication.operationTypeColumn, DynamoStreamReplication.deleteOperation)
    }
    Some(item)
  }

  private def scanAll(): List[Map[String, AttributeValue]] =
    targetAlternator()
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .asScala
      .map(_.asScala.toMap)
      .toList

  test("flushBatch directly handles empty batch without error") {
    createTable()
    val batch = new util.ArrayList[WriteRequest]()
    val batchKeys = new util.HashSet[String]()
    // flushBatch with empty batch should be a no-op (while loop condition is immediately false)
    DynamoStreamReplication.flushBatch(targetAlternator(), tableName, batch, batchKeys)
  }

  test("flushBatch directly writes a single item") {
    createTable()
    val batch = new util.ArrayList[WriteRequest]()
    val batchKeys = new util.HashSet[String]()
    batch.add(
      WriteRequest
        .builder()
        .putRequest(
          PutRequest
            .builder()
            .item(Map("id" -> AttributeValue.fromS("direct-1"), "v" -> AttributeValue.fromS("x")).asJava)
            .build()
        )
        .build()
    )
    batchKeys.add("id=direct-1")
    DynamoStreamReplication.flushBatch(targetAlternator(), tableName, batch, batchKeys)

    assert(batch.isEmpty, "batch should be cleared after flush")
    assert(batchKeys.isEmpty, "batchKeys should be cleared after flush")
    assertEquals(scanAll().size, 1)
  }

  test("flushBatch directly writes exactly 25 items (max batch size)") {
    createTable()
    val batch = new util.ArrayList[WriteRequest]()
    val batchKeys = new util.HashSet[String]()
    for (i <- 1 to DynamoStreamReplication.batchWriteItemLimit) {
      batch.add(
        WriteRequest
          .builder()
          .putRequest(
            PutRequest
              .builder()
              .item(Map("id" -> AttributeValue.fromS(s"b-$i")).asJava)
              .build()
          )
          .build()
      )
      batchKeys.add(s"id=b-$i")
    }
    DynamoStreamReplication.flushBatch(targetAlternator(), tableName, batch, batchKeys)
    assertEquals(scanAll().size, DynamoStreamReplication.batchWriteItemLimit)
  }

  test("run() with 100 items correctly flushes multiple batches") {
    val tableDesc = createTable()
    val items = (1 to 100).map(i => makeItem(s"item-$i", isPut = true))

    DynamoStreamReplication.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 100)
  }

  test("run() handles rapid put-then-delete for the same key across batch boundaries") {
    val tableDesc = createTable()
    // Create items that force multiple flush cycles with duplicate keys
    val items = (1 to 30).flatMap { i =>
      Seq(
        makeItem(s"key-${i % 5}", isPut = true),
        makeItem(s"key-${i % 5}", isPut = false)
      )
    }

    DynamoStreamReplication.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    // All keys should be deleted since the last operation for each key is a delete
    val results = scanAll()
    assertEquals(results.size, 0)
  }
}
