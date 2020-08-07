package com.scylladb.migrator.writers

import java.util

import com.amazonaws.services.dynamodbv2.document.ItemUtils
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, TableDescription }
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.audienceproject.spark.dynamodb.connector.ColumnSchema
import com.audienceproject.spark.dynamodb.datasource.TypeConverter
import com.scylladb.migrator.config.{ AWSCredentials, Rename, SourceSettings, TargetSettings }
import org.apache.spark.sql.types.{ BooleanType, StructType }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{ KinesisInputDStream, SparkAWSCredentials }
import org.apache.spark.unsafe.types.UTF8String

object DynamoStreamReplication {
  def createDStream(spark: SparkSession,
                    streamingContext: StreamingContext,
                    src: SourceSettings.DynamoDB,
                    target: TargetSettings.DynamoDB,
                    inferredSchema: StructType,
                    targetTableDesc: TableDescription,
                    renames: List[Rename]) =
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
      .buildWithMessageHandler {
        case rec: RecordAdapter => Some(rec.getInternalObject)
        case _                  => None
      }
      .foreachRDD { msgs =>
        val typeConverter = TypeConverter.fromStructType(inferredSchema)
        val df = spark
          .createDataFrame(
            msgs
              .collect {
                case Some(rec) =>
                  val newMap = new util.HashMap[String, AttributeValue]()

                  if (rec.getDynamodb.getNewImage ne null)
                    newMap.putAll(rec.getDynamodb.getNewImage)

                  newMap.putAll(rec.getDynamodb.getKeys)

                  Row.fromSeq {
                    typeConverter
                      .readInternalRow(ItemUtils.toItem(newMap))
                      .toSeq(inferredSchema)
                      .map {
                        case s: UTF8String => s.toString
                        case x             => x
                      } ++
                      Seq(
                        rec.getEventName match {
                          case "INSERT" | "MODIFY" => ColumnSchema.PutOperation
                          case "REMOVE"            => ColumnSchema.DeleteOperation
                        }
                      )
                  }
              },
            inferredSchema.add(ColumnSchema.OperationTypeColumn, BooleanType)
          )

        DynamoDB.writeDataframe(target, renames, df, targetTableDesc)(spark)
      }
}
