package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.{ AttributeAction, AttributeValue, AttributeValueUpdate }
import com.scylladb.migrator.SparkUtils.{ submitSparkJob, successfullyPerformMigration }

import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ValidatorTest extends MigratorSuite {

  withTable("BasicTest").test("Validate migration") { tableName =>
    val configFile = "dynamodb-to-alternator-basic.yaml"

    val keys = Map("id"   -> new AttributeValue().withS("12345"))
    val attrs = Map("foo" -> new AttributeValue().withS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(tableName, itemData.asJava)

    // Perform the migration
    successfullyPerformMigration(configFile)

    // Perform the validation
    submitSparkJob(configFile, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 0, "Validation failed")
    }

    // Change the value of an item
    targetAlternator.updateItem(
      tableName,
      keys.asJava,
      Map(
        "foo" -> new AttributeValueUpdate()
          .withValue(new AttributeValue().withS("baz"))
          .withAction(AttributeAction.PUT)).asJava)

    // Check that the validation failed because of the introduced change
    submitSparkJob(configFile, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 1)
    }
  }

}
