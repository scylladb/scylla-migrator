package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters._

/** Basic migration that uses the real AWS DynamoDB as a source and AssumeRole for authentication */
class AssumeRoleTest extends MigratorSuiteWithAWS {

  withTable("AssumeRoleTest").test("Read from source and write to target") { tableName =>
    val configFileName = "dynamodb-to-alternator-assume-role.yaml"

    setupConfigurationFile(configFileName)

    // Insert some items
    val keys = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    // Perform the migration
    successfullyPerformMigration(configFileName)

    checkSchemaWasMigrated(tableName)

    checkItemWasMigrated(tableName, keys, itemData)
  }

}
