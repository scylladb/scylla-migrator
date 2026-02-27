package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, TargetSettings }
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import scala.jdk.CollectionConverters._

/** Tests for the batch write logic in DynamoStreamReplication.run(), which exercises
  * the flushBatch retry path indirectly through successful batch writes.
  */
class FlushBatchRetryTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "FlushBatchRetryTest"
  private val operationTypeColumn = DynamoStreamReplication.operationTypeColumn
  private val putOperation = DynamoStreamReplication.putOperation
  private val deleteOperation = DynamoStreamReplication.deleteOperation

  private val targetSettings = TargetSettings.DynamoDB(
    table                       = tableName,
    region                      = Some("eu-central-1"),
    endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
    credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
    streamChanges               = false,
    skipInitialSnapshotTransfer = None,
    writeThroughput             = None,
    throughputWritePercent       = None
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
      item.put(operationTypeColumn, putOperation)
    } else {
      item.put(operationTypeColumn, deleteOperation)
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

  test("run() handles batch of exactly 25 items (one full batch)") {
    val tableDesc = createTable()
    val items = (1 to 25).map(i => makeItem(s"item-$i", isPut = true))

    DynamoStreamReplication.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 25)
  }

  test("run() handles batch exceeding 25 items (multiple flushes)") {
    val tableDesc = createTable()
    val items = (1 to 60).map(i => makeItem(s"item-$i", isPut = true))

    DynamoStreamReplication.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

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

    DynamoStreamReplication.run(items, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    assertEquals(results.size, 2) // dup-1 and dup-2
  }

  test("run() handles mixed puts and deletes") {
    val tableDesc = createTable()
    // First insert some items
    val inserts = (1 to 5).map(i => makeItem(s"item-$i", isPut = true))
    DynamoStreamReplication.run(inserts, targetSettings, Map.empty, tableDesc, targetAlternator())
    assertEquals(scanAll().size, 5)

    // Now delete some and add new ones
    val mixed = Seq(
      makeItem("item-2", isPut = false), // delete
      makeItem("item-4", isPut = false), // delete
      makeItem("item-6", isPut = true)   // insert
    )
    DynamoStreamReplication.run(mixed, targetSettings, Map.empty, tableDesc, targetAlternator())

    val results = scanAll()
    val ids = results.map(_("id").s()).toSet
    assertEquals(ids, Set("item-1", "item-3", "item-5", "item-6"))
  }

  test("run() with empty items is a no-op") {
    val tableDesc = createTable()
    // Should not throw
    DynamoStreamReplication.run(Seq.empty, targetSettings, Map.empty, tableDesc, targetAlternator())
    DynamoStreamReplication.run(Seq(None, None), targetSettings, Map.empty, tableDesc, targetAlternator())
    assertEquals(scanAll().size, 0)
  }
}
