package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  StreamChangesSetting,
  TargetSettings
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import scala.jdk.CollectionConverters._
import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal

class DynamoStreamReplicationIntegrationTest extends MigratorSuiteWithDynamoDBLocal {
  implicit val spark: SparkSession =
    SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  private val tableName = "DynamoStreamReplicationIntegrationTest"

  /** Collision-proof op-type marker: operations are now tagged out-of-band on
    * [[DynamoStreamReplication.StreamChange]] rather than stamped into the item map under a
    * reserved attribute name. This test formerly asserted that the sentinel attribute
    * `_dynamo_op_type` never leaked into the final table rows — that assertion is now structural
    * (the marker isn't in the item map to begin with) and is preserved below as a presence check on
    * a user-controlled attribute name so any regression to the in-band marker shows up here.
    */
  private val formerMarkerAttributeName = "_dynamo_op_type"

  def scanAll(client: DynamoDbClient, tableName: String): List[Map[String, AttributeValue]] =
    client
      .scanPaginator(ScanRequest.builder().tableName(tableName).build())
      .items()
      .asScala
      .map(_.asScala.toMap)
      .toList

  /** Build a [[DynamoStreamReplication.StreamChange]] for a put. */
  private def put(
    attrs: (String, AttributeValueV1)*
  ): DynamoStreamReplication.StreamChange = {
    val map = new util.HashMap[String, AttributeValueV1]()
    attrs.foreach { case (k, v) => map.put(k, v) }
    DynamoStreamReplication.StreamChange(map, DynamoStreamReplication.OpType.Put)
  }

  /** Build a [[DynamoStreamReplication.StreamChange]] for a delete. */
  private def delete(
    attrs: (String, AttributeValueV1)*
  ): DynamoStreamReplication.StreamChange = {
    val map = new util.HashMap[String, AttributeValueV1]()
    attrs.foreach { case (k, v) => map.put(k, v) }
    DynamoStreamReplication.StreamChange(map, DynamoStreamReplication.OpType.Delete)
  }

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

      val streamEvents: Seq[Option[DynamoStreamReplication.StreamChange]] = Seq(
        Some(delete("id" -> new AttributeValueV1().withS("toDelete"))),
        Some(
          put(
            "id"    -> new AttributeValueV1().withS("toUpdate"),
            "value" -> new AttributeValueV1().withS("value2-updated")
          )
        ),
        Some(
          put(
            "id"    -> new AttributeValueV1().withS("toInsert"),
            "value" -> new AttributeValueV1().withS("value3")
          )
        ),
        Some(
          put(
            "id"    -> new AttributeValueV1().withS("keyPutDelete"),
            "value" -> new AttributeValueV1().withS("value4")
          )
        ),
        Some(delete("id" -> new AttributeValueV1().withS("keyPutDelete"))),
        Some(delete("id" -> new AttributeValueV1().withS("keyDeletePut"))),
        Some(
          put(
            "id"    -> new AttributeValueV1().withS("keyDeletePut"),
            "value" -> new AttributeValueV1().withS("value5")
          )
        )
      )

      val rdd: RDD[Option[DynamoStreamReplication.StreamChange]] =
        spark.sparkContext.parallelize(streamEvents, 1)

      val targetSettings = TargetSettings.DynamoDB(
        table                       = tableName,
        region                      = Some("eu-central-1"),
        endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
        credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
        streamChanges               = StreamChangesSetting.Disabled,
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
        tableDesc,
        new DynamoStreamReplication.Metrics(spark)
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

      // The legacy in-band marker is gone — but if a future regression silently reintroduces it
      // this assertion will fail visibly instead of flipping put/delete on real user data.
      finalItems.foreach { item =>
        assert(
          !item.contains(formerMarkerAttributeName),
          s"The legacy '_dynamo_op_type' marker must not appear in target rows under any " +
            "circumstance — its presence indicates a regression to the in-band marker that LOGIC-1 " +
            "escalated to CRITICAL."
        )
      }
  }

  /** Pin the LOGIC-1 regression test: a source attribute literally named `_dynamo_op_type` with a
    * boolean value must NOT flip the intended operation. Before the out-of-band refactor this row
    * was silently converted into a delete because `_dynamo_op_type == putOperation` was false
    * (putOperation is `BOOL=true`; but the check was equality against a shared sentinel, so a row
    * with `_dynamo_op_type: {BOOL: false}` would have been deleted under the old code).
    */
  withTable("DynamoStreamReplicationIntegrationTest_Collision").test(
    "source attribute named `_dynamo_op_type` no longer flips the operation (LOGIC-1)"
  ) { _ =>
    val collisionTable = "DynamoStreamReplicationIntegrationTest_Collision"
    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(collisionTable)
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
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(collisionTable).build())

    val events: Seq[Option[DynamoStreamReplication.StreamChange]] = Seq(
      Some(
        put(
          "id"    -> new AttributeValueV1().withS("userRow"),
          "value" -> new AttributeValueV1().withS("keep-me"),
          // Same name as the former in-band marker, deliberately with the "wrong" boolean to
          // catch any code path that still treats the column as a signal.
          formerMarkerAttributeName -> new AttributeValueV1().withBOOL(false)
        )
      )
    )

    val rdd: RDD[Option[DynamoStreamReplication.StreamChange]] =
      spark.sparkContext.parallelize(events, 1)

    val targetSettings = TargetSettings.DynamoDB(
      table                       = collisionTable,
      region                      = Some("eu-central-1"),
      endpoint                    = Some(DynamoDBEndpoint("http://localhost", 8000)),
      credentials                 = Some(AWSCredentials("dummy", "dummy", None)),
      streamChanges               = StreamChangesSetting.Disabled,
      skipInitialSnapshotTransfer = Some(true),
      writeThroughput             = None,
      throughputWritePercent      = None
    )

    val tableDesc = targetAlternator()
      .describeTable(
        DescribeTableRequest.builder().tableName(collisionTable).build()
      )
      .table()

    DynamoStreamReplication.run(
      rdd,
      targetSettings,
      Map.empty[String, String],
      tableDesc,
      new DynamoStreamReplication.Metrics(spark)
    )

    val finalItems = scanAll(targetAlternator(), collisionTable)
    assertEquals(finalItems.size, 1, "The put must NOT be flipped to a delete by a colliding name")
    val row = finalItems.head
    assertEquals(row("id").s, "userRow")
    assertEquals(row("value").s, "keep-me")
    assertEquals(
      row(formerMarkerAttributeName).bool.booleanValue(),
      false,
      "User-controlled attribute must round-trip unchanged — the migrator must NOT strip or " +
        "rewrite an attribute just because its name coincides with the legacy sentinel."
    )
  }
}
