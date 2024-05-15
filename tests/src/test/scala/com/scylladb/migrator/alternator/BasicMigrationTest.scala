package com.scylladb.migrator.alternator

import com.scylladb.migrator.AttributeValueUtils.stringValue
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import scala.collection.JavaConverters._

class BasicMigrationTest extends MigratorSuite {

  withTable("BasicTest").test("Read from source and write to target") { tableName =>
    val keys = Map("id"   -> stringValue("12345"))
    val attrs = Map("foo" -> stringValue("bar"))
    val itemData = keys ++ attrs

    // Insert some items
    sourceDDb.putItem(tableName, itemData.asJava)

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-basic.yaml")

    checkSchemaWasMigrated(tableName)

    checkItemWasMigrated(tableName, keys, itemData)
  }

}
