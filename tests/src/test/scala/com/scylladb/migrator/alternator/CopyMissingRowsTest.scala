package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteItemRequest,
  PutItemRequest
}

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

class CopyMissingRowsTest extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 120.seconds

  withTable("BasicTest").test("copyMissingRows copies missing rows to target") { tableName =>
    val basicConfigFile = "dynamodb-to-alternator-basic.yaml"
    val copyMissingConfigFile = "dynamodb-to-alternator-copy-missing-rows.yaml"

    val keys = Map("id" -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    sourceDDb().putItem(
      PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build()
    )

    successfullyPerformMigration(basicConfigFile)
    assertEquals(performValidation(basicConfigFile), 0, "Initial validation failed")

    // Delete the row from the target to simulate a missing row
    targetAlternator().deleteItem(
      DeleteItemRequest.builder().tableName(tableName).key(keys.asJava).build()
    )

    // Validate with copyMissingRows enabled — detects the missing row and copies it
    assertEquals(performValidation(copyMissingConfigFile), 1, "Should detect missing row")

    // Validate again — the missing row should now be present in the target
    assertEquals(performValidation(basicConfigFile), 0, "Row should have been copied to target")
  }

}
