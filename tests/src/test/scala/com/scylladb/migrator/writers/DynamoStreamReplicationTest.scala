package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{AttributeValue => AttributeValueV1}

import java.util
import scala.jdk.CollectionConverters._

class DynamoStreamReplicationTest extends munit.FunSuite {

  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)

  private def createOperation(id: String, seqNum: String, isPut: Boolean, value: Option[String] = None): util.Map[String, AttributeValueV1] = {
    val map = new util.HashMap[String, AttributeValueV1]()
    map.put("id", new AttributeValueV1().withS(id))
    map.put("__seq_num", new AttributeValueV1().withS(seqNum))
    map.put(operationTypeColumn, if (isPut) putOperation else deleteOperation)
    value.foreach(v => map.put("value", new AttributeValueV1().withS(v)))
    map
  }

  test("groupConsecutiveOperations should handle empty operations") {
    val operations = Seq.empty[util.Map[String, AttributeValueV1]]
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    assertEquals(result, List.empty)
  }

  test("groupConsecutiveOperations should handle single PUT operation") {
    val operations = Seq(
      createOperation("item1", "1", isPut = true, Some("value1"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 1)
    assertEquals(result.head._1, "PUT")
    assertEquals(result.head._2.size, 1)
    assertEquals(result.head._2.head.get("id").getS, "item1")
  }

  test("groupConsecutiveOperations should handle single DELETE operation") {
    val operations = Seq(
      createOperation("item1", "1", isPut = false)
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 1)
    assertEquals(result.head._1, "DELETE")
    assertEquals(result.head._2.size, 1)
    assertEquals(result.head._2.head.get("id").getS, "item1")
  }

  test("groupConsecutiveOperations should group consecutive PUTs together") {
    val operations = Seq(
      createOperation("item1", "1", isPut = true, Some("value1")),
      createOperation("item2", "2", isPut = true, Some("value2")),
      createOperation("item3", "3", isPut = true, Some("value3"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 1)
    assertEquals(result.head._1, "PUT")
    assertEquals(result.head._2.size, 3)
    assertEquals(result.head._2.map(_.get("id").getS), List("item1", "item2", "item3"))
  }

  test("groupConsecutiveOperations should group consecutive DELETEs together") {
    val operations = Seq(
      createOperation("item1", "1", isPut = false),
      createOperation("item2", "2", isPut = false),
      createOperation("item3", "3", isPut = false)
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 1)
    assertEquals(result.head._1, "DELETE")
    assertEquals(result.head._2.size, 3)
    assertEquals(result.head._2.map(_.get("id").getS), List("item1", "item2", "item3"))
  }

  test("groupConsecutiveOperations should separate different operation types") {
    val operations = Seq(
      createOperation("item1", "1", isPut = true, Some("value1")),
      createOperation("item2", "2", isPut = false),
      createOperation("item3", "3", isPut = true, Some("value3"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 3)
    assertEquals(result(0)._1, "PUT")
    assertEquals(result(0)._2.map(_.get("id").getS), List("item1"))
    
    assertEquals(result(1)._1, "DELETE")
    assertEquals(result(1)._2.map(_.get("id").getS), List("item2"))
    
    assertEquals(result(2)._1, "PUT")
    assertEquals(result(2)._2.map(_.get("id").getS), List("item3"))
  }

  test("groupConsecutiveOperations should maintain sequence order within groups") {
    val operations = Seq(
      createOperation("item1", "1", isPut = true, Some("value1")),
      createOperation("item2", "2", isPut = true, Some("value2")),
      createOperation("item3", "5", isPut = false),
      createOperation("item4", "6", isPut = false),
      createOperation("item5", "10", isPut = true, Some("value5"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 3)
    
    // First PUT batch
    assertEquals(result(0)._1, "PUT")
    assertEquals(result(0)._2.map(_.get("__seq_num").getS), List("1", "2"))
    
    // DELETE batch
    assertEquals(result(1)._1, "DELETE")
    assertEquals(result(1)._2.map(_.get("__seq_num").getS), List("5", "6"))
    
    // Second PUT batch
    assertEquals(result(2)._1, "PUT")
    assertEquals(result(2)._2.map(_.get("__seq_num").getS), List("10"))
  }

  test("groupConsecutiveOperations should handle complex alternating pattern") {
    val operations = Seq(
      createOperation("item1", "1", isPut = true, Some("value1")),
      createOperation("item2", "2", isPut = false),
      createOperation("item3", "3", isPut = true, Some("value3")),
      createOperation("item4", "4", isPut = false),
      createOperation("item5", "5", isPut = false),
      createOperation("item6", "6", isPut = true, Some("value6"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 5)
    assertEquals(result(0)._1, "PUT")
    assertEquals(result(0)._2.map(_.get("id").getS), List("item1"))
    
    assertEquals(result(1)._1, "DELETE")
    assertEquals(result(1)._2.map(_.get("id").getS), List("item2"))
    
    assertEquals(result(2)._1, "PUT")
    assertEquals(result(2)._2.map(_.get("id").getS), List("item3"))
    
    assertEquals(result(3)._1, "DELETE")
    assertEquals(result(3)._2.map(_.get("id").getS), List("item4", "item5"))
    
    assertEquals(result(4)._1, "PUT")
    assertEquals(result(4)._2.map(_.get("id").getS), List("item6"))
  }

  test("groupConsecutiveOperations should handle real-world scenario: PUT->DELETE->PUT same item") {
    val operations = Seq(
      createOperation("same_item", "1", isPut = true, Some("initial_value")),
      createOperation("same_item", "2", isPut = false),
      createOperation("same_item", "3", isPut = true, Some("final_value"))
    )
    val result = DynamoStreamReplication.groupConsecutiveOperations(operations)
    
    assertEquals(result.size, 3)
    assertEquals(result(0)._1, "PUT")
    assertEquals(result(0)._2.head.get("value").getS, "initial_value")
    
    assertEquals(result(1)._1, "DELETE")
    assertEquals(result(1)._2.head.get("id").getS, "same_item")
    
    assertEquals(result(2)._1, "PUT")
    assertEquals(result(2)._2.head.get("value").getS, "final_value")
  }
}