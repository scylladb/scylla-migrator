package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  PutItemRequest,
  ResourceNotFoundException,
  ScanRequest
}

import java.util.UUID
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

class SkippedSegmentsTest extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 120.seconds

  withTable("SkippedSegments").test("Run partial migrations") { tableName =>
    // We rely on the fact that both config files have `scanSegments: 3` and
    // complementary `skipSegments` properties
    val configPart1 = "dynamodb-to-alternator-part1.yaml"
    val configPart2 = "dynamodb-to-alternator-part2.yaml"

    createRandomData(tableName)

    // Initially, the target table does not exist
    try {
      targetAlternator().describeTable(describeTableRequest(tableName))
      fail(s"The table ${tableName} should not exist yet")
    } catch {
      case _: ResourceNotFoundException =>
        () // OK
    }

    // Perform the first part of the migration
    successfullyPerformMigration(configPart1)

    // Verify that some items have been copied to the target database …
    val itemCount = targetAlternatorItemCount(tableName)
    assert(itemCount > 75L && itemCount < 125L, s"Unexpected item count: ${itemCount}")
    // … but not all of them, hence the validator fails
    assertEquals(performValidation(configPart2), 1)

    // Perform the other (complementary) part of the migration
    successfullyPerformMigration(configPart2)

    // Validate that all the data from the source have been migrated to the target database
    assertEquals(performValidation(configPart2), 0, "Validation failed")
  }

  def createRandomData(tableName: String): Unit =
    for (_ <- 1 to 300) {
      val itemData = Map(
        "id"  -> AttributeValue.fromS(UUID.randomUUID().toString),
        "foo" -> AttributeValue.fromS(UUID.randomUUID().toString),
        "bar" -> AttributeValue.fromS(UUID.randomUUID().toString),
        "baz" -> AttributeValue.fromS(UUID.randomUUID().toString)
      )
      sourceDDb().putItem(
        PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build()
      )
    }

  def targetAlternatorItemCount(tableName: String): Long =
    targetAlternator()
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .stream()
      .count()

}
