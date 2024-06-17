package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import scala.jdk.CollectionConverters._

class BasicMigrationTest extends MigratorSuite {

  withTable("BasicTest").test("Read from source and write to target") { tableName =>
    val keys = Map("id"   -> new AttributeValue().withS("12345"))
    val attrs = Map("foo" -> new AttributeValue().withS("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(tableName, itemData.asJava)

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-basic.yaml")

    checkSchemaWasMigrated(tableName)

    checkItemWasMigrated(tableName, keys, itemData)
  }

}
