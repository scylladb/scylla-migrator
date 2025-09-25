package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{AttributeValue => AttributeValueV1}
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{DynamoDBEndpoint, TargetSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import java.net.URI
import java.util
import scala.jdk.CollectionConverters._
import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal

class DynamoStreamReplicationIntegrationTest extends MigratorSuiteWithDynamoDBLocal {
  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

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

  withTable(tableName).test("should correctly apply UPSERT and DELETE operations from a stream") { _ =>
    sourceDDb().putItem(
      PutItemRequest
      .builder()
      .tableName(tableName)
      .item(Map("key" -> AttributeValue.builder.s("toDelete").build, "value" -> AttributeValue.builder.s("value1").build).asJava)
      .item(Map("key" -> AttributeValue.builder.s("toUpdate").build, "value" -> AttributeValue.builder.s("value2").build).asJava)
      .build()
      )

    val streamEvents = Seq(
      Some(Map(
        "key" -> new AttributeValueV1().withS("toDelete"),
        operationTypeColumn -> deleteOperation
      ).asJava),
      Some(Map(
        "key" -> new AttributeValueV1().withS("toUpdate"),
        "value" -> new AttributeValueV1().withS("value2-updated"),
        operationTypeColumn -> putOperation
      ).asJava),
      Some(Map(
        "key" -> new AttributeValueV1().withS("toInsert"),
        "value" -> new AttributeValueV1().withS("value3"),
        operationTypeColumn -> putOperation
      ).asJava)
    )

    val rdd = spark.sparkContext.parallelize(streamEvents)
      .asInstanceOf[RDD[Option[util.Map[String, AttributeValueV1]]]]

    val targetSettings = TargetSettings.DynamoDB(
      table = tableName,
      region = Some("eu-central-1"),
      endpoint = Some(DynamoDBEndpoint("localhost", 8001)),
      credentials = None,
      streamChanges = false,
      skipInitialSnapshotTransfer = Some(true),
      writeThroughput = None,
      throughputWritePercent = None
    )

    val tableDesc = sourceDDb().describeTable(
      software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
        .builder()
        .tableName(tableName)
        .build()
    ).table()

    DynamoStreamReplication.run(
      rdd,
      targetSettings,
      Map.empty[String, String],
      tableDesc,
      DynamoDB
    )

    val finalItems = scanAll(sourceDDb(), tableName).sortBy(m => m("key").s)

    assertEquals(finalItems.size, 2)

    assert(!finalItems.exists(_("key").s == "toDelete"))


    val key2Item = finalItems.find(_("key").s == "toUpdate").get
    assertEquals(key2Item("value").s, "value2-updated")


    val key3Item = finalItems.find(_("key").s == "toInsert").get
    assertEquals(key3Item("value").s, "value3")


    finalItems.foreach { item =>
      assert(!item.contains(operationTypeColumn))
    }
  }
}