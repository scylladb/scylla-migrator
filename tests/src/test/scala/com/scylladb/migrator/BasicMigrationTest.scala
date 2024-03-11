package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest}

import scala.collection.JavaConverters._
import scala.util.chaining._

class BasicMigrationTest extends MigratorSuite {

  withTable("BasicTest").test("Read from source and write to target") { tableName =>
    val keys = Map("id"   -> new AttributeValue().withS("12345"))
    val attrs = Map("foo" -> new AttributeValue().withS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(tableName, itemData.asJava)

    // Perform the migration
    submitSparkJob("dynamodb-to-alternator-basic.yaml")

    // Check that the schema has been replicated to the target table
    val sourceTableDesc = sourceDDb.describeTable(tableName).getTable
    targetAlternator
      .describeTable(tableName)
      .getTable
      .tap { targetTableDesc =>
        assertEquals(targetTableDesc.getKeySchema, sourceTableDesc.getKeySchema)
        assertEquals(
          targetTableDesc.getAttributeDefinitions,
          sourceTableDesc.getAttributeDefinitions)
      }

    // Check that the items have been migrated to the target table
    targetAlternator
      .getItem(new GetItemRequest(tableName, keys.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, itemData)
      }
  }

}
