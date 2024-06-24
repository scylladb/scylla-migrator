package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.{submitSparkJob, successfullyPerformMigration}
import software.amazon.awssdk.services.dynamodb.model.{AttributeAction, AttributeValue, AttributeValueUpdate, PutItemRequest, UpdateItemRequest}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ValidatorTest extends MigratorSuite {

  withTable("BasicTest").test("Validate migration") { tableName =>
    val configFile = "dynamodb-to-alternator-basic.yaml"

    val keys = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    // Perform the migration
    successfullyPerformMigration(configFile)

    // Perform the validation
    submitSparkJob(configFile, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 0, "Validation failed")
    }

    // Change the value of an item
    targetAlternator.updateItem(
      UpdateItemRequest
        .builder()
        .tableName(tableName)
        .key(keys.asJava)
        .attributeUpdates(
          Map(
            "foo" -> AttributeValueUpdate.builder()
              .value(AttributeValue.fromS("baz"))
              .action(AttributeAction.PUT).build()
          ).asJava
        )
        .build()
    )

    // Check that the validation failed because of the introduced change
    submitSparkJob(configFile, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 1)
    }
  }

}
