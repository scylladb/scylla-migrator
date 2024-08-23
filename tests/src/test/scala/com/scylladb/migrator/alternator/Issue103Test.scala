package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters._

// Reproduction of https://github.com/scylladb/scylla-migrator/issues/103
class Issue103Test extends MigratorSuiteWithDynamoDBLocal {

  withTable("Issue103Items").test("Issue #103 is fixed") { tableName =>
    // Insert two items
    val keys1 = Map("id" -> AttributeValue.fromS("4"))
    val attrs1 = Map(
      "AlbumTitle" -> AttributeValue.fromS("aaaaa"),
      "Awards"     -> AttributeValue.fromN("4")
    )
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> AttributeValue.fromS("999"))
    val attrs2 = Map(
      "asdfg" -> AttributeValue.fromM(
        Map(
        "fffff" -> AttributeValue.fromS("asdfasdfs")
        ).asJava
      )
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build())
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build())

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-issue-103.yaml")

    // Check that both items have been correctly migrated to the target table
    checkItemWasMigrated(tableName, keys1, item1Data)
    checkItemWasMigrated(tableName, keys2, item2Data)
  }

}
