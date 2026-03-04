package com.scylladb.migrator.alternator

import com.scylladb.migrator.config.MigratorConfig
import com.scylladb.migrator.{ SparkUtils, TestFileUtils }
import io.circe.parser.{ decode => jsonDecode }
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import java.io.File
import java.nio.file.{ Files, Path, Paths }
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Integration test for DynamoDB -> S3 Export with savepoint support.
  *
  * Verifies that the migration produces a savepoint file with skipSegments populated, confirming
  * that the DynamoDbSavepointsManager is wired in for this migration path.
  */
class DynamoDBToS3ExportSavepointsTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "S3ExportSavepointsTest"
  private val exportDir = new File("docker/parquet/s3export_savepoints_test")
  private val savepointsDir = Paths.get("docker/spark-master/s3export_savepoints")

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (exportDir.exists()) TestFileUtils.deleteRecursive(exportDir)
    ensureEmptyDirectory(savepointsDir)
  }

  override def afterAll(): Unit = {
    if (exportDir.exists()) TestFileUtils.deleteRecursive(exportDir)
    ensureEmptyDirectory(savepointsDir)
    super.afterAll()
  }

  test("DynamoDB -> S3 Export creates savepoint with skipSegments") {
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

    SparkUtils.successfullyPerformMigration("dynamodb-to-s3export-savepoints.yaml")

    assert(exportDir.exists(), s"Export directory not created: $exportDir")
    val summaryFile = new File(exportDir, "manifest-summary.json")
    assert(summaryFile.exists(), "manifest-summary.json not created")

    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val summary =
      jsonDecode[DynamoDBS3ExportE2EBenchmarkUtils.ManifestSummary](summaryJson)
        .fold(throw _, identity)
    assertEquals(summary.itemCount, itemCount.toLong, "Manifest item count mismatch")

    val savepoint = findLatestSavepoint(savepointsDir)
    assert(savepoint.isDefined, "No savepoint file was created")

    val savepointConfig = MigratorConfig.loadFrom(savepoint.get.toString)
    assert(
      savepointConfig.skipSegments.isDefined,
      "Savepoint should contain skipSegments"
    )
    assert(
      savepointConfig.skipSegments.get.nonEmpty,
      "skipSegments should not be empty after migration"
    )
  }

  private def findLatestSavepoint(directory: Path): Option[Path] =
    if (!Files.exists(directory)) None
    else
      Using
        .resource(Files.list(directory)) { stream =>
          stream
            .iterator()
            .asScala
            .filter(path => Files.isRegularFile(path))
            .filter(_.getFileName.toString.startsWith("savepoint_"))
            .toSeq
        }
        .sortBy(path => Files.getLastModifiedTime(path).toMillis)
        .lastOption

  private def ensureEmptyDirectory(directory: Path): Unit = {
    if (Files.exists(directory)) {
      Using.resource(Files.list(directory)) { stream =>
        stream.iterator().asScala.foreach(Files.deleteIfExists)
      }
    }
    Files.createDirectories(directory)
  }

}
