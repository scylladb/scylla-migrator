package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisDynamoDBInputDStream,
  KinesisInitialPositions,
  SparkAWSCredentials
}
import com.scylladb.migrator.DynamoUtils
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AttributeValueV2,
  DeleteItemRequest,
  PutItemRequest,
  TableDescription
}

import java.util
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  // We enrich the table items with a column `operationTypeColumn` describing the type of change
  // applied to the item.
  // We have to deal with multiple representation of the data because `spark-kinesis-dynamodb`
  // uses the AWS SDK V1, whereas `emr-dynamodb-hadoop` uses the AWS SDK V2
  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)

  private[writers] def run(
    msgs: RDD[Option[util.HashMap[String, AttributeValueV1]]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {
    val rdd = msgs.flatMap(_.toSeq)

    val putCount = spark.sparkContext.longAccumulator("putCount")
    val deleteCount = spark.sparkContext.longAccumulator("deleteCount")
    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet

    rdd.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val client =
          DynamoUtils.buildDynamoClient(
            target.endpoint,
            target.finalCredentials.map(_.toProvider),
            target.region,
            Seq.empty
          )
        try {
          partition.foreach { item =>
            val isPut = item.get(operationTypeColumn) == putOperation

            val itemWithoutOp = item.asScala.collect {
              case (k, v) if k != operationTypeColumn => k -> AttributeValueUtils.fromV1(v)
            }.asJava

            if (isPut) {
              putCount.add(1)
              val finalItem = itemWithoutOp.asScala.map {
                case (key, value) => renamesMap.getOrElse(key, key) -> value
              }.asJava
              try {
                client.putItem(
                  PutItemRequest.builder().tableName(target.table).item(finalItem).build())
              } catch {
                case e: Exception =>
                  log.error(s"Failed to put item into ${target.table}", e)
              }
            } else {
              deleteCount.add(1)
              val keyToDelete = itemWithoutOp.asScala
                .filter { case (key, _) => keyAttributeNames.contains(key) }
                .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
                .asJava
              try {
                client.deleteItem(
                  DeleteItemRequest.builder().tableName(target.table).key(keyToDelete).build())
              } catch {
                case e: Exception =>
                  log.error(s"Failed to delete item from ${target.table}", e)
              }
            }
          }
        } finally {
          client.close()
        }
      }
    }

    if (putCount.value > 0 || deleteCount.value > 0) {
      log.info(s"""
                  |Changes to be applied:
                  |  - ${putCount.value} items to UPSERT
                  |  - ${deleteCount.value} items to DELETE
                  |""".stripMargin)
    } else {
      log.info("No changes to apply")
    }
  }

  def createDStream(spark: SparkSession,
                    streamingContext: StreamingContext,
                    src: SourceSettings.DynamoDB,
                    target: TargetSettings.DynamoDB,
                    targetTableDesc: TableDescription,
                    renamesMap: Map[String, String]): Unit =
    new KinesisDynamoDBInputDStream(
      streamingContext,
      streamName        = src.table,
      regionName        = src.region.orNull,
      initialPosition   = new KinesisInitialPositions.TrimHorizon,
      checkpointAppName = s"migrator_${src.table}_${System.currentTimeMillis()}",
      messageHandler = {
        case recAdapter: RecordAdapter =>
          val rec = recAdapter.getInternalObject
          val newMap = new util.HashMap[String, AttributeValueV1]()

          if (rec.getDynamodb.getNewImage ne null) {
            newMap.putAll(rec.getDynamodb.getNewImage)
          }

          newMap.putAll(rec.getDynamodb.getKeys)

          val operationType =
            rec.getEventName match {
              case "INSERT" | "MODIFY" => putOperation
              case "REMOVE"            => deleteOperation
            }
          newMap.put(operationTypeColumn, operationType)
          Some(newMap)

        case _ => None
      },
      kinesisCreds = src.credentials
        .map {
          case AWSCredentials(accessKey, secretKey, maybeAssumeRole) =>
            val builder =
              SparkAWSCredentials.builder
                .basicCredentials(accessKey, secretKey)
            for (assumeRole <- maybeAssumeRole) {
              builder.stsCredentials(assumeRole.arn, assumeRole.getSessionName)
            }
            builder.build()
        }
        .getOrElse(SparkAWSCredentials.builder.build())
    ).foreachRDD { msgs =>
      run(
        msgs, //.asInstanceOf[RDD[Option[util.Map[String, AttributeValueV1]]]],
        target,
        renamesMap,
        targetTableDesc)(spark)
    }

}
