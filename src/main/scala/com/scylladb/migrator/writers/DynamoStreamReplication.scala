package com.scylladb.migrator.writers

import java.util

import com.amazonaws.services.dynamodbv2.document.ItemUtils
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, TableDescription }
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.audienceproject.spark.dynamodb.connector.ColumnSchema
import com.audienceproject.spark.dynamodb.datasource.TypeConverter
import com.scylladb.migrator.config.{ AWSCredentials, Rename, SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{ BooleanType, StructType }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisInitialPositions,
  KinesisInputDStream,
  SparkAWSCredentials
}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.{ functions => f }

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  def createDStream(spark: SparkSession,
                    streamingContext: StreamingContext,
                    src: SourceSettings.DynamoDB,
                    target: TargetSettings.DynamoDB,
                    inferredSchema: StructType,
                    targetTableDesc: TableDescription,
                    renames: List[Rename]) = {
    val typeConverter = TypeConverter.fromStructType(inferredSchema)

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

          val row =
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

          Some(row)

        case _ => None
      }
      .foreachRDD { msgs =>
        val df = spark
          .createDataFrame(
            msgs
              .collect {
                case Some(row) => row
              },
            inferredSchema.add(ColumnSchema.OperationTypeColumn, BooleanType)
          )

        log.info("Changes to be applied:")
        df.select(
            f.when(
                f.col(ColumnSchema.OperationTypeColumn) === f.lit(ColumnSchema.PutOperation),
                f.lit("UPSERT"))
              .when(
                f.col(ColumnSchema.OperationTypeColumn) === f.lit(ColumnSchema.DeleteOperation),
                f.lit("DELETE"))
              .otherwise(f.lit("UNKNOWN"))
              .as(ColumnSchema.OperationTypeColumn)
          )
          .groupBy(ColumnSchema.OperationTypeColumn)
          .count()
          .show()

        DynamoDB.writeDataframe(target, renames, df, targetTableDesc)(spark)
      }
  }
}
