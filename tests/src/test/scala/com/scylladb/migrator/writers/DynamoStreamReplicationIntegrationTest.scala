package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, TargetSettings }
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import java.net.URI
import java.util
import scala.jdk.CollectionConverters._
import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal

class DynamoStreamReplicationIntegrationTest extends MigratorSuiteWithDynamoDBLocal {
  implicit val spark: SparkSession =
    SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  private val tableName = "DynamoStreamReplicationIntegrationTest"
  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)

  def scanAll(client: DynamoDbClient, tableName: String): List[Map[String, AttributeValue]] =
    client
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .asScala
      .map(_.asScala.toMap)
      .toList

  withTable(tableName).test("should correctly apply UPSERT and DELETE operations from a stream") {
    _ =>
      targetAlternator().createTable(
        CreateTableRequest
          .builder()
          .tableName(tableName)
          .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
          .attributeDefinitions(
            AttributeDefinition
              .builder()
              .attributeName("id")
              .attributeType(ScalarAttributeType.S)
              .build()
          )
          .provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
          )
          .build()
      )
      targetAlternator()
        .waiter()
        .waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build())

      targetAlternator().putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "id"    -> AttributeValue.builder.s("toDelete").build,
              "value" -> AttributeValue.builder.s("value1").build
            ).asJava
          )
          .build()
      )
      targetAlternator().putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "id"    -> AttributeValue.builder.s("toUpdate").build,
              "value" -> AttributeValue.builder.s("value2").build
            ).asJava
          )
          .build()
      )

      val streamEvents = Seq(
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("toDelete"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("toUpdate"),
            "value"             -> new AttributeValueV1().withS("value2-updated"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("toInsert"),
            "value"             -> new AttributeValueV1().withS("value3"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("keyPutDelete"),
            "value"             -> new AttributeValueV1().withS("value4"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("keyPutDelete"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("keyDeletePut"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> new AttributeValueV1().withS("keyDeletePut"),
            "value"             -> new AttributeValueV1().withS("value5"),
            operationTypeColumn -> putOperation
          ).asJava
        )
      )

      val rdd = spark.sparkContext
        .parallelize(streamEvents, 1)
        .asInstanceOf[RDD[Option[DynamoStreamReplication.DynamoItem]]]

      val targetSettings = TargetSettings.DynamoDB(
        table                       = tableName,
        region                      = Some("eu-central-1"),
        endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
        credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
        streamChanges               = false,
        skipInitialSnapshotTransfer = Some(true),
        writeThroughput             = None,
        throughputWritePercent      = None
      )

      val tableDesc = targetAlternator()
        .describeTable(
          software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
            .builder()
            .tableName(tableName)
            .build()
        )
        .table()

      DynamoStreamReplication.run(
        rdd,
        targetSettings,
        Map.empty[String, String],
        tableDesc
      )

      val finalItems = scanAll(targetAlternator(), tableName).sortBy(m => m("id").s)

      assertEquals(finalItems.size, 3)

      assert(!finalItems.exists(_("id").s == "toDelete"))

      val updatedItem = finalItems.find(_("id").s == "toUpdate").get
      assertEquals(updatedItem("value").s, "value2-updated")

      val insertedItem = finalItems.find(_("id").s == "toInsert").get
      assertEquals(insertedItem("value").s, "value3")

      assert(!finalItems.exists(_("id").s == "keyPutDelete"))

      val deletePutItem = finalItems.find(_("id").s == "keyDeletePut").get
      assertEquals(deletePutItem("value").s, "value5")

      finalItems.foreach { item =>
        assert(!item.contains(operationTypeColumn))
      }
  }
}
