package com.scylladb.migrator

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  OperationType,
  Record,
  StreamRecord
}

import scala.jdk.CollectionConverters._

class DynamoStreamPollerTest extends munit.FunSuite {

  private val operationTypeColumn = "_dynamo_op_type"
  private val putMarker = AttributeValue.fromBool(true)
  private val deleteMarker = AttributeValue.fromBool(false)

  private def makeRecord(
    eventName: OperationType,
    keys: Map[String, AttributeValue],
    newImage: Map[String, AttributeValue] = Map.empty
  ): Record = {
    val srBuilder = StreamRecord
      .builder()
      .keys(keys.asJava)
      .sequenceNumber("000001")
    if (newImage.nonEmpty)
      srBuilder.newImage(newImage.asJava)
    Record
      .builder()
      .eventName(eventName)
      .dynamodb(srBuilder.build())
      .build()
  }

  test("recordToItem: INSERT produces put marker with newImage") {
    val keys = Map("id" -> AttributeValue.fromS("k1"))
    val newImage = Map(
      "id"    -> AttributeValue.fromS("k1"),
      "value" -> AttributeValue.fromS("v1")
    )
    val record = makeRecord(OperationType.INSERT, keys, newImage)
    val result =
      DynamoStreamPoller.recordToItem(record, operationTypeColumn, putMarker, deleteMarker)
    assert(result.isDefined)
    val item = result.get.asScala.toMap
    assertEquals(item(operationTypeColumn), putMarker)
    assertEquals(item("value"), AttributeValue.fromS("v1"))
  }

  test("recordToItem: MODIFY produces put marker with newImage") {
    val keys = Map("id" -> AttributeValue.fromS("k1"))
    val newImage = Map(
      "id"    -> AttributeValue.fromS("k1"),
      "value" -> AttributeValue.fromS("v2")
    )
    val record = makeRecord(OperationType.MODIFY, keys, newImage)
    val result =
      DynamoStreamPoller.recordToItem(record, operationTypeColumn, putMarker, deleteMarker)
    assert(result.isDefined)
    val item = result.get.asScala.toMap
    assertEquals(item(operationTypeColumn), putMarker)
    assertEquals(item("value"), AttributeValue.fromS("v2"))
  }

  test("recordToItem: REMOVE produces delete marker with keys only") {
    val keys = Map("id" -> AttributeValue.fromS("k1"))
    val record = makeRecord(OperationType.REMOVE, keys)
    val result =
      DynamoStreamPoller.recordToItem(record, operationTypeColumn, putMarker, deleteMarker)
    assert(result.isDefined)
    val item = result.get.asScala.toMap
    assertEquals(item(operationTypeColumn), deleteMarker)
    assertEquals(item("id"), AttributeValue.fromS("k1"))
    assertEquals(item.size, 2) // key + op marker
  }

  test("recordToItem: unknown event returns None") {
    val record = Record
      .builder()
      .eventName("UNKNOWN_EVENT")
      .dynamodb(
        StreamRecord
          .builder()
          .keys(Map("id" -> AttributeValue.fromS("k1")).asJava)
          .sequenceNumber("000001")
          .build()
      )
      .build()
    val result =
      DynamoStreamPoller.recordToItem(record, operationTypeColumn, putMarker, deleteMarker)
    assert(result.isEmpty)
  }
}
