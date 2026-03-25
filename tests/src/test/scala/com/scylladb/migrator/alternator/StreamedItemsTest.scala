package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  PutItemRequest
}

import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
import scala.jdk.CollectionConverters._
import scala.util.chaining.scalaUtilChainingOps

class StreamedItemsTest extends MigratorSuiteWithAWS {

  override val munitTimeout: Duration = 120.seconds

  withTable("StreamedItemsTest").test("Stream changes") { tableName =>
    val configFileName = "dynamodb-to-alternator-streaming.yaml"

    // Populate the source table
    val keys1 = Map("id" -> AttributeValue.fromS("12345"))
    val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
    val item1Data = keys1 ++ attrs1
    sourceDDb().putItem(
      PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build()
    )

    // Perform the migration in background (streaming runs indefinitely)
    val sparkJob = SparkUtils.submitMigrationInBackground(configFileName)

    // Check that the table initial snapshot has been successfully migrated
    awaitAtMost(60.seconds) {
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys1.asJava).build())
        .tap { itemResult =>
          assert(itemResult.hasItem, "First item not found in the target database")
          assertEquals(itemResult.item.asScala.toMap, item1Data)
        }
    }

    // Insert one more item
    val keys2 = Map("id" -> AttributeValue.fromS("67890"))
    val attrs2 = Map(
      "foo" -> AttributeValue.fromS("baz"),
      "baz" -> AttributeValue.fromBool(false)
    )
    val item2Data = keys2 ++ attrs2
    sourceDDb().putItem(
      PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build()
    )

    // Check that the added item has also been migrated
    awaitAtMost(60.seconds) {
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build())
        .tap { itemResult =>
          assert(itemResult.hasItem, "Second item not found in the target database")
          assertEquals(
            itemResult.item.asScala.toMap,
            item2Data + ("_dynamo_op_type" -> AttributeValue.fromBool(true))
          )
        }
    }

    // Stop the migration job
    sparkJob.stop()

    deleteStreamTable(tableName)
  }

  withTable("StreamedItemsSkipSnapshotTest").test("Stream changes but skip initial snapshot") {
    tableName =>
      val configFileName = "dynamodb-to-alternator-streaming-skip-snapshot.yaml"

      // Populate the source table
      val keys1 = Map("id" -> AttributeValue.fromS("12345"))
      val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
      val item1Data = keys1 ++ attrs1
      sourceDDb().putItem(
        PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build()
      )

      // Perform the migration in background
      val sparkJob = SparkUtils.submitMigrationInBackground(configFileName)

      // Wait for the streaming to start (give initial snapshot transfer time to be skipped)
      Thread.sleep(10000)

      // Insert one more item
      val keys2 = Map("id" -> AttributeValue.fromS("67890"))
      val attrs2 = Map(
        "foo" -> AttributeValue.fromS("baz"),
        "baz" -> AttributeValue.fromBool(false)
      )
      val item2Data = keys2 ++ attrs2
      sourceDDb().putItem(
        PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build()
      )

      // Check that only the second item has been migrated
      awaitAtMost(60.seconds) {
        targetAlternator()
          .getItem(GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build())
          .tap { itemResult =>
            assert(itemResult.hasItem, "Second item not found in the target database")
            assertEquals(
              itemResult.item.asScala.toMap,
              item2Data + ("_dynamo_op_type" -> AttributeValue.fromBool(true))
            )
          }
      }
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys1.asJava).build())
        .tap { itemResult =>
          assert(!itemResult.hasItem, "First item found in the target database")
        }

      // Stop the migration job
      sparkJob.stop()

      deleteStreamTable(tableName)
  }

  /** Continuously evaluate the provided `assertion` until it terminates (MUnit models failures with
    * exceptions of type `AssertionError`), or until the provided `delay` passed.
    */
  def awaitAtMost(delay: FiniteDuration)(assertion: => Unit): Unit = {
    val deadline = delay.fromNow
    var maybeFailure: Option[AssertionError] = Some(
      new AssertionError("Assertion has not been tested yet")
    )
    while (maybeFailure.isDefined && deadline.hasTimeLeft())
      try {
        assertion
        maybeFailure = None
      } catch {
        case failure: AssertionError =>
          maybeFailure = Some(failure)
          Thread.sleep(1000)
      }
    for (failure <- maybeFailure)
      fail(s"Assertion was not true after ${delay}", failure)
  }

  /** Delete the DynamoDB table automatically created by the Kinesis library
    * @param sourceTableName
    *   Source table that had streams enabled
    */
  private def deleteStreamTable(sourceTableName: String): Unit =
    sourceDDb()
      .listTablesPaginator()
      .tableNames()
      .stream()
      .filter(name => name.startsWith(s"migrator_${sourceTableName}_"))
      .forEach { streamTableName =>
        deleteTableIfExists(sourceDDb(), streamTableName)
      }

}
