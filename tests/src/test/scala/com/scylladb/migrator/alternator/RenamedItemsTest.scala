package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters._

class RenamedItemsTest extends MigratorSuite {

  withTable("RenamedItems").test("Rename items along the migration") { tableName =>
    // Insert several items
    val keys1 = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> AttributeValue.fromS("67890"))
    val attrs2 = Map(
      "foo" -> AttributeValue.fromS("baz"),
      "baz" -> AttributeValue.fromBool(false)
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb.putItem(PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build())
    sourceDDb.putItem(PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build())

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
