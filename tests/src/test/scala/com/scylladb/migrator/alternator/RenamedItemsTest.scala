package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.scylladb.migrator.AttributeValueUtils.{ boolValue, stringValue }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import scala.collection.JavaConverters._
import scala.util.chaining._

class RenamedItemsTest extends MigratorSuite {

  withTable("RenamedItems").test("Rename items along the migration") { tableName =>
    // Insert several items
    val keys1 = Map("id"   -> stringValue("12345"))
    val attrs1 = Map("foo" -> stringValue("bar"))
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> stringValue("67890"))
    val attrs2 = Map(
      "foo" -> stringValue("baz"),
      "baz" -> boolValue(false)
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb.putItem(tableName, item1Data.asJava)
    sourceDDb.putItem(tableName, item2Data.asJava)

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-renames.yaml")

    val renamedItem1Data =
      item1Data + ("quux" -> item1Data("foo")) - "foo"
    checkItemWasMigrated(tableName, keys1, renamedItem1Data)

    val renamedItem2Data =
      item2Data + ("quux" -> item2Data("foo")) - "foo"
    checkItemWasMigrated(tableName, keys2, renamedItem2Data)
  }

}
