package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, TableDescription }
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.scylladb.migrator.config.{ AWSCredentials, Rename, SourceSettings, TargetSettings }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisInitialPositions,
  KinesisInputDStream,
  SparkAWSCredentials
}

import java.util

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = new AttributeValue().withBOOL(true)
  private val deleteOperation = new AttributeValue().withBOOL(false)

  def createDStream(spark: SparkSession,
                    streamingContext: StreamingContext,
                    src: SourceSettings.DynamoDB,
                    target: TargetSettings.DynamoDB,
                    targetTableDesc: TableDescription,
                    renames: List[Rename]): Unit =
    KinesisInputDStream.builder
      .streamingContext(streamingContext)
      .streamName(src.table)
      .dynamoStream(true)
      .kinesisCredentials(
        src.credentials.map {
          case AWSCredentials(accessKey, secretKey) =>
            SparkAWSCredentials.builder
              .basicCredentials(accessKey, secretKey)
              .build
        }.orNull
      )
      .regionName(src.region.orNull)
      .checkpointAppName(s"migrator_${src.table}_${System.currentTimeMillis()}")
      .initialPosition(new KinesisInitialPositions.TrimHorizon)
      .buildWithMessageHandler {
        case recAdapter: RecordAdapter =>
          val rec = recAdapter.getInternalObject
          val newMap = new util.HashMap[String, AttributeValue]()

          if (rec.getDynamodb.getNewImage ne null)
            newMap.putAll(rec.getDynamodb.getNewImage)

          newMap.putAll(rec.getDynamodb.getKeys)

          val operationType =
            rec.getEventName match {
              case "INSERT" | "MODIFY" => putOperation
              case "REMOVE"            => deleteOperation
            }
          newMap.put(operationTypeColumn, operationType)
          Some(newMap)

        case _ => None
      }
      .foreachRDD { msgs =>
        val rdd = msgs
          .collect {
            case Some(item) => (new Text(), new DynamoDBItemWritable(item))
          }
          .repartition(Runtime.getRuntime.availableProcessors() * 2)

        log.info("Changes to be applied:")
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
          .foreach {
            case (operation, count) =>
              log.info(s"${operation}: ${count}")
          }

        DynamoDB.writeRDD(target, renames, rdd, Some(targetTableDesc))(spark)
      }

}
