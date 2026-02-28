package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, TargetSettings }
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._
import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal

class DynamoStreamReplicationIntegrationTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "DynamoStreamReplicationIntegrationTest"
  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = AttributeValue.fromBool(true)
  private val deleteOperation = AttributeValue.fromBool(false)

  def scanAll(
    client: DynamoDbClient,
    tableName: String
  ): List[Map[String, AttributeValue]] =
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

      val streamEvents: Seq[Option[BatchWriter.DynamoItem]] = Seq(
        Some(
          Map(
            "id"                -> AttributeValue.fromS("toDelete"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("toUpdate"),
            "value"             -> AttributeValue.fromS("value2-updated"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("toInsert"),
            "value"             -> AttributeValue.fromS("value3"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("keyPutDelete"),
            "value"             -> AttributeValue.fromS("value4"),
            operationTypeColumn -> putOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("keyPutDelete"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("keyDeletePut"),
            operationTypeColumn -> deleteOperation
          ).asJava
        ),
        Some(
          Map(
            "id"                -> AttributeValue.fromS("keyDeletePut"),
            "value"             -> AttributeValue.fromS("value5"),
            operationTypeColumn -> putOperation
          ).asJava
        )
      )

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

      BatchWriter.run(
        streamEvents,
        targetSettings,
        Map.empty[String, String],
        tableDesc,
        targetAlternator()
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

  withTable(tableName + "Renames").test(
    "should correctly apply renames when renamesMap is provided"
  ) { _ =>
    val renamedTable = tableName + "Renames"
    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(renamedTable)
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
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(renamedTable).build())

    val streamEvents: Seq[Option[BatchWriter.DynamoItem]] = Seq(
      Some(
        Map(
          "id"                -> AttributeValue.fromS("k1"),
          "oldName"           -> AttributeValue.fromS("val1"),
          operationTypeColumn -> putOperation
        ).asJava
      )
    )

    val targetSettings = TargetSettings.DynamoDB(
      table                       = renamedTable,
      region                      = Some("eu-central-1"),
      endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
      credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
      streamChanges               = false,
      skipInitialSnapshotTransfer = Some(true),
      writeThroughput             = None,
      throughputWritePercent      = None
    )

    val tableDesc = targetAlternator()
      .describeTable(DescribeTableRequest.builder().tableName(renamedTable).build())
      .table()

    BatchWriter.run(
      streamEvents,
      targetSettings,
      Map("oldName" -> "newName"),
      tableDesc,
      targetAlternator()
    )

    val finalItems = scanAll(targetAlternator(), renamedTable)
    assertEquals(finalItems.size, 1)
    val item = finalItems.head
    assertEquals(item("id").s, "k1")
    assertEquals(item("newName").s, "val1")
    assert(!item.contains("oldName"))
  }
}
