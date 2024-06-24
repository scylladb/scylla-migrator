package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, Rename, SourceSettings, TargetSettings }
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = AttributeValueUtils.boolValue(true)
  private val deleteOperation = AttributeValueUtils.boolValue(false)

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
          val newMap = new util.HashMap[String, AttributeValue]()

          if (rec.getDynamodb.getNewImage ne null) {
            rec.getDynamodb.getNewImage.forEach { (key, value) =>
              newMap.put(key, AttributeValueUtils.fromV1(value))
            }
          }

          rec.getDynamodb.getKeys.forEach { (key, value) =>
            newMap.put(key, AttributeValueUtils.fromV1(value))
          }

          val operationType =
            rec.getEventName match {
              case "INSERT" | "MODIFY" => putOperation
              case "REMOVE"            => deleteOperation
            }
          newMap.put(operationTypeColumn, operationType)
          Some(newMap)

        case _ => None
      },
      kinesisCreds = src.credentials.map {
        case AWSCredentials(accessKey, secretKey, maybeAssumeRole) =>
          val builder =
            SparkAWSCredentials.builder
              .basicCredentials(accessKey, secretKey)
          for (assumeRole <- maybeAssumeRole) {
            builder.stsCredentials(assumeRole.arn, assumeRole.getSessionName)
          }
          builder.build()
      }.orNull
    ).foreachRDD { msgs =>
      val rdd = msgs
        .collect { case Some(item) => new DynamoDBItemWritable(item) }
        .repartition(Runtime.getRuntime.availableProcessors() * 2)
        .map(item => (new Text, item)) // Create the key after repartitioning to avoid Serialization issues

      val changes =
        rdd
          .map(_._2) // Remove keys because they are not serializable
          .groupBy { itemWritable =>
            itemWritable.getItem.get(operationTypeColumn) match {
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

      DynamoDB.writeRDD(target, renamesMap, rdd, targetTableDesc)(spark)
    }

}
