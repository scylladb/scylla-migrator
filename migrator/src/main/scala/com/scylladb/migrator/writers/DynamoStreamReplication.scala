package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, SourceSettings, TargetSettings }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisDynamoDBInputDStream,
  KinesisInitialPositions,
  SparkAWSCredentials
}
import software.amazon.awssdk.services.dynamodb.model.TableDescription

import java.util
import java.util.stream.Collectors

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  // We enrich the table items with a column `operationTypeColumn` describing the type of change
  // applied to the item.
  // We have to deal with multiple representation of the data because `spark-kinesis-dynamodb`
  // uses the AWS SDK V1, whereas `emr-dynamodb-hadoop` uses the AWS SDK V2
  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)

  private[writers] def run(msgs: RDD[Option[util.Map[String, AttributeValueV1]]],
                           target: TargetSettings.DynamoDB,
                           renamesMap: Map[String, String],
                           targetTableDesc: TableDescription,
                           dynamoDB: DynamoDB.type)(implicit spark: SparkSession): Unit =
    if (!msgs.isEmpty()) {
      val rdd = msgs
        .collect { case Some(item) => item: util.Map[String, AttributeValueV1] }
        .repartition(Runtime.getRuntime.availableProcessors() * 2)

      val puts = rdd.filter(item => item.get(operationTypeColumn) == putOperation)
      val deletes =
        rdd.filter(item => item.get(operationTypeColumn) == deleteOperation)

      val putCount = puts.count()
      val deleteCount = deletes.count()

      if (putCount > 0 || deleteCount > 0) {
        log.info(s"""
                    |Changes to be applied:
                    |  - ${putCount} items to UPSERT
                    |  - ${deleteCount} items to DELETE
                    |""".stripMargin)
      } else {
        log.info("No changes to apply")
      }

      if (deleteCount > 0) {
        val deletableItems =
          deletes.map { item =>
            item
              .entrySet()
              .stream()
              .filter(e => e.getKey != operationTypeColumn)
              .collect(
                Collectors.toMap(
                  (e: util.Map.Entry[String, AttributeValueV1]) => e.getKey,
                  (e: util.Map.Entry[String, AttributeValueV1]) =>
                    AttributeValueUtils.fromV1(e.getValue)
                )
              )
          }
        dynamoDB.deleteRDD(target, targetTableDesc, deletableItems)(spark)
      }

      if (putCount > 0) {
        val writablePuts =
          puts.map { item =>
            (
              new Text,
              new DynamoDBItemWritable(
                item
                  .entrySet()
                  .stream()
                  .filter(e => e.getKey != operationTypeColumn)
                  .collect(
                    Collectors.toMap(
                      (e: util.Map.Entry[String, AttributeValueV1]) => e.getKey,
                      (e: util.Map.Entry[String, AttributeValueV1]) =>
                        AttributeValueUtils.fromV1(e.getValue)
                    )
                  )
              )
            )
          }
        dynamoDB.writeRDD(target, renamesMap, writablePuts, targetTableDesc)(spark)
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
        msgs.asInstanceOf[RDD[Option[util.Map[String, AttributeValueV1]]]],
        target,
        renamesMap,
        targetTableDesc,
        DynamoDB)(spark)
    }

}
