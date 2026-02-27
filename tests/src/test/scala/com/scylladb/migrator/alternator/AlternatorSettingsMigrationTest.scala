package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import scala.jdk.CollectionConverters._

class AlternatorSettingsMigrationTest extends MigratorSuiteWithDynamoDBLocal {

  withTable("AlternatorSettingsTest").test(
    "Migration works with alternator settings in config"
  ) { tableName =>
    val keys = Map("id" -> AttributeValue.fromS("alt-settings-1"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    sourceDDb().putItem(
      PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build()
    )

    successfullyPerformMigration("dynamodb-to-alternator-with-alternator-settings.yaml")

    checkSchemaWasMigrated(tableName)
    checkItemWasMigrated(tableName, keys, itemData)
  }

}
