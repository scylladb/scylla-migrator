package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, SourceSettings, TargetSettings }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
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
      val rdd = msgs
        .collect { case Some(item) => item: util.Map[String, AttributeValueV1] }
        .repartition(Runtime.getRuntime.availableProcessors() * 2)

      val changes =
        rdd
          .groupBy { item =>
            item.get(operationTypeColumn) match {
              case `putOperation`    => "UPSERT"
              case `deleteOperation` => "DELETE"
              case _                 => "UNKNOWN"
            }
          }
          .mapValues(_.size)
          .collect()
      if (changes.nonEmpty) {
        log.info("Changes to be applied:")
        for ((operation, count) <- changes) {
          log.info(s"${operation}: ${count}")
        }
      } else {
        log.info("No changes to apply")
      }

      val writableRdd =
        rdd.map { item =>
          (
            new Text,
            new DynamoDBItemWritable(
              item
                .entrySet()
                .stream()
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

      DynamoDB.writeRDD(target, renamesMap, writableRdd, targetTableDesc)(spark)
    }

}
