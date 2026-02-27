package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeAction,
  AttributeValue,
  AttributeValueUpdate,
  PutItemRequest,
  UpdateItemRequest
}

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

class ValidatorTest extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 120.seconds

  withTable("BasicTest").test("Validate migration") { tableName =>
    val configFile = "dynamodb-to-alternator-basic.yaml"

    val keys = Map("id" -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    // Perform the migration
    successfullyPerformMigration(configFile)

    // Perform the validation
    assertEquals(performValidation(configFile), 0, "Validation failed")

    // Change the value of an item
    targetAlternator().updateItem(
      UpdateItemRequest
        .builder()
        .tableName(tableName)
        .key(keys.asJava)
        .attributeUpdates(
          Map(
            "foo" -> AttributeValueUpdate
              .builder()
              .value(AttributeValue.fromS("baz"))
              .action(AttributeAction.PUT)
              .build()
          ).asJava
        )
        .build()
    )

    // Check that the validation failed because of the introduced change
    assertEquals(performValidation(configFile), 1)
  }

}
