package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.{submitSparkJob, successfullyPerformMigration}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest, ResourceNotFoundException, ScanRequest}

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class SkippedSegmentsTest extends MigratorSuite {

  withTable("SkippedSegments").test("Run partial migrations") { tableName =>
    // We rely on the fact that both config files have `scanSegments: 3` and
    // complementary `skipSegments` properties
    val configPart1 = "dynamodb-to-alternator-part1.yaml"
    val configPart2 = "dynamodb-to-alternator-part2.yaml"

    createRandomData(tableName)

    // Initially, the target table does not exist
    try {
      targetAlternator.describeTable(describeTableRequest(tableName))
      fail(s"The table ${tableName} should not exist yet")
    } catch {
      case _: ResourceNotFoundException =>
        () // OK
    }

    // Perform the first part of the migration
    successfullyPerformMigration(configPart1)

    // Verify that some items have been copied to the target database …
    val itemCount = targetAlternatorItemCount(tableName)
    assert(itemCount > 90L && itemCount < 110L)
    // … but not all of them, hence the validator fails
    submitSparkJob(configPart2, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 1)
    }

    // Perform the other (complementary) part of the migration
    successfullyPerformMigration(configPart2)

    // Validate that all the data from the source have been migrated to the target database
    submitSparkJob(configPart2, "com.scylladb.migrator.Validator").exitValue().tap { statusCode =>
      assertEquals(statusCode, 0, "Validation failed")
    }
  }

  def createRandomData(tableName: String): Unit = {
    for (_ <- 1 to 300) {
      val itemData = Map(
        "id" -> AttributeValue.fromS(UUID.randomUUID().toString),
        "foo" -> AttributeValue.fromS(UUID.randomUUID().toString),
        "bar" -> AttributeValue.fromS(UUID.randomUUID().toString),
        "baz" -> AttributeValue.fromS(UUID.randomUUID().toString)
      )
      sourceDDb.putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())
    }
  }

  def targetAlternatorItemCount(tableName: String): Long =
    targetAlternator
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .stream()
      .count()

}
