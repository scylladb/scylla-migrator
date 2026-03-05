package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ SparkUtils, TestFileUtils }
import io.circe.parser.{ decode => jsonDecode }
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import java.io.File
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Integration test for DynamoDB -> S3 Export through Migrator.main.
  *
  * Exercises the (DynamoDB, DynamoDBS3Export) match case in Migrator with a small number of items.
  */
class DynamoDBToS3ExportMigrationTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "S3ExportBasicTest"
  private val exportDir = new File("docker/parquet/s3export_basic_test")

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (exportDir.exists()) TestFileUtils.deleteRecursive(exportDir)
  }

  override def afterAll(): Unit = {
    if (exportDir.exists()) TestFileUtils.deleteRecursive(exportDir)
    super.afterAll()
  }

  test("DynamoDB -> S3 Export produces valid export files") {
    deleteTableIfExists(sourceDDb(), tableName)

    sourceDDb().createTable(
      software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
        .builder()
        .tableName(tableName)
        .keySchema(
          software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
            .builder()
            .attributeName("id")
            .keyType(software.amazon.awssdk.services.dynamodb.model.KeyType.HASH)
            .build()
        )
        .attributeDefinitions(
          software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.S)
            .build()
        )
        .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
        .build()
    )
    sourceDDb()
      .waiter()
      .waitUntilTableExists(describeTableRequest(tableName))

    val itemCount = 5
    for (i <- 0 until itemCount)
      sourceDDb().putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "id"   -> AttributeValue.fromS(s"id-$i"),
              "col1" -> AttributeValue.fromS(s"value-$i"),
              "col2" -> AttributeValue.fromN(i.toString)
            ).asJava
          )
          .build()
      )

    SparkUtils.successfullyPerformMigration("dynamodb-to-s3export-basic.yaml")

    assert(exportDir.exists(), s"Export directory not created: $exportDir")
    val summaryFile = new File(exportDir, "manifest-summary.json")
    assert(summaryFile.exists(), "manifest-summary.json not created")
    assert(new File(exportDir, "data").exists(), "data directory not created")

    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val summary =
      jsonDecode[DynamoDBS3ExportE2EBenchmarkUtils.ManifestSummary](summaryJson)
        .fold(throw _, identity)
    assertEquals(summary.itemCount, itemCount.toLong, "Manifest item count mismatch")
  }

}
