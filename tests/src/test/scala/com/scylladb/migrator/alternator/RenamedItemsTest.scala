package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest}
import scala.collection.JavaConverters._
import scala.util.chaining._

class RenamedItemsTest extends MigratorSuite {

  withTable("RenamedItems").test("Rename items along the migration") { tableName =>
    // Insert several items
    val keys1 = Map("id" -> new AttributeValue().withS("12345"))
    val attrs1 = Map("foo" -> new AttributeValue().withS("bar"))
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> new AttributeValue().withS("67890"))
    val attrs2 = Map(
      "foo" -> new AttributeValue().withS("baz"),
      "baz" -> new AttributeValue().withBOOL(false)
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb.putItem(tableName, item1Data.asJava)
    sourceDDb.putItem(tableName, item2Data.asJava)

    // Perform the migration
    submitSparkJob("dynamodb-to-alternator-renames.yaml")

    val renamedItem1Data =
      item1Data + ("quux" -> item1Data("foo")) - "foo"

    targetAlternator
      .getItem(new GetItemRequest(tableName, keys1.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, renamedItem1Data)
      }

    val renamedItem2Data =
      item2Data + ("quux" -> item2Data("foo")) - "foo"

    targetAlternator
      .getItem(new GetItemRequest(tableName, keys2.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, renamedItem2Data)
      }
  }

}
