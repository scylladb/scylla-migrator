package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  PutItemRequest,
  TimeToLiveSpecification,
  UpdateTimeToLiveRequest
}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

class SkippedItemsTest extends MigratorSuiteWithDynamoDBLocal {

  withTable("TtlTable").test("Expired items should be filtered out from the source table") {
    tableName =>
      // Insert two items, one of them is expired
      val oneDay = 24 * 60 * 60 // seconds
      val now = System.currentTimeMillis() / 1000 // seconds
      val keys1 = Map("id" -> AttributeValue.fromS("12345"))
      val attrs1 = Map("foo" -> AttributeValue.fromN((now + oneDay).toString))
      val item1Data = keys1 ++ attrs1
      sourceDDb().putItem(
        PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build()
      )
      val keys2 = Map("id" -> AttributeValue.fromS("67890"))
      val attrs2 = Map("foo" -> AttributeValue.fromN((now - oneDay).toString))
      val item2Data = keys2 ++ attrs2
      sourceDDb().putItem(
        PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build()
      )

      // Enable TTL
      sourceDDb()
        .updateTimeToLive(
          UpdateTimeToLiveRequest
            .builder()
            .tableName(tableName)
            .timeToLiveSpecification(
              TimeToLiveSpecification
                .builder()
                .enabled(true)
                .attributeName("foo")
                .build()
            )
            .build()
        )
        .sdkHttpResponse()
        .statusCode()
        .tap { statusCode =>
          assertEquals(statusCode, 200)
        }

      // Check that expired item is still present in the source before the migration
      val getItem2Request =
        GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build()
      sourceDDb()
        .getItem(getItem2Request)
        .tap { itemResult =>
          assert(itemResult.hasItem)
          assertEquals(itemResult.item.asScala.toMap, item2Data)
        }

      successfullyPerformMigration("dynamodb-to-alternator-ttl.yaml")

      checkItemWasMigrated(tableName, keys1, item1Data)

      // Expired item has been skipped
      targetAlternator()
        .getItem(getItem2Request)
        .tap { itemResult =>
          assert(!itemResult.hasItem)
        }

  }

}
