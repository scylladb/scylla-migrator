package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest}
import com.scylladb.migrator.SparkUtils.submitMigrationJob

import scala.collection.JavaConverters._
import scala.util.chaining._

// Reproduction of https://github.com/scylladb/scylla-migrator/issues/103
class Issue103Test extends MigratorSuite {

  withTable("Issue103Items").test("Issue #103 is fixed") { tableName =>
    // Insert two items
    val keys1 = Map("id" -> new AttributeValue().withS("4"))
    val attrs1 = Map(
      "AlbumTitle" -> new AttributeValue().withS("aaaaa"),
      "Awards" -> new AttributeValue().withN("4")
    )
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> new AttributeValue().withS("999"))
    val attrs2 = Map(
      "asdfg" -> new AttributeValue().withM(
        Map("fffff" -> new AttributeValue().withS("asdfasdfs")).asJava
      )
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb.putItem(tableName, item1Data.asJava)
    sourceDDb.putItem(tableName, item2Data.asJava)

    // Perform the migration
    submitMigrationJob("dynamodb-to-alternator-issue-103.yaml")

    // Check that both items have been correctly migrated to the target table
    targetAlternator
      .getItem(new GetItemRequest(tableName, keys1.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, item1Data)
      }
    targetAlternator
      .getItem(new GetItemRequest(tableName, keys2.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, item2Data)
      }
  }

}
