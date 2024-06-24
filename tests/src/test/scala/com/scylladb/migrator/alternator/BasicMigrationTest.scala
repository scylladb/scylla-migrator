package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters._

class BasicMigrationTest extends MigratorSuite {

  withTable("BasicTest").test("Read from source and write to target") { tableName =>
    val keys = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-basic.yaml")

    checkSchemaWasMigrated(tableName)

    checkItemWasMigrated(tableName, keys, itemData)
  }

}
