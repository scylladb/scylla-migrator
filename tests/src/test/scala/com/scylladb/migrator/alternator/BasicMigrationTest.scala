package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import scala.jdk.CollectionConverters._

class BasicMigrationTest extends MigratorSuiteWithDynamoDBLocal {

  withTable("BasicTest").test("Read from source and write to target (PAY_PER_REQUEST)") {
    tableName =>
      runBasicMigration(tableName, "dynamodb-to-alternator-basic.yaml")
  }

  withTable("BasicTest").test("Read from source and write to target (PROVISIONED)") { tableName =>
    runBasicMigration(tableName, "dynamodb-to-alternator-basic-provisioned.yaml")
  }

  private def runBasicMigration(tableName: String, configFile: String): Unit = {
    val keys = Map("id" -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    // Perform the migration
    successfullyPerformMigration(configFile)

    checkSchemaWasMigrated(tableName)

    checkItemWasMigrated(tableName, keys, itemData)
  }

}
