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
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AttributeValueV2,
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
  private val sequenceNumberColumn = "_seq_num"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)
  private val PUT_OPERATION_TYPE = "PUT"
  private val DELETE_OPERATION_TYPE = "DELETE"

  /**
    * Groups consecutive operations of the same type into batches for efficient processing
    * while maintaining sequence order.
    *
    * @param operations List of operations sorted by sequence number
    * @return List of (operation_type, batch) tuples in sequence order
    */
  private[writers] def groupConsecutiveOperations(
    operations: Seq[util.Map[String, AttributeValueV1]])
    : List[(String, List[util.Map[String, AttributeValueV1]])] = {
    val getOperationType = (item: util.Map[String, AttributeValueV1]) =>
      if (item.get(operationTypeColumn) == putOperation) PUT_OPERATION_TYPE
      else DELETE_OPERATION_TYPE

    operations
      .foldLeft(List.empty[(String, List[util.Map[String, AttributeValueV1]])]) {
        case (Nil, item) =>
          List((getOperationType(item), List(item)))
        case ((lastType, lastBatch) :: rest, item) =>
          val currentType = getOperationType(item)
          if (currentType == lastType) {
            (lastType, lastBatch :+ item) :: rest
          } else {
            (currentType, List(item)) :: (lastType, lastBatch) :: rest
          }
      }
      .reverse
  }

  private[writers] def run(msgs: RDD[Option[util.Map[String, AttributeValueV1]]],
                           target: TargetSettings.DynamoDB,
                           renamesMap: Map[String, String],
                           targetTableDesc: TableDescription,
                           dynamoDB: DynamoDB.type)(implicit spark: SparkSession): Unit =
    if (!msgs.isEmpty()) {
      val rdd = msgs
        .collect { case Some(item) => item: util.Map[String, AttributeValueV1] }
        .repartition(Runtime.getRuntime.availableProcessors() * 2)

      val allOperationsSorted = rdd.collect().sortBy(_.get(sequenceNumberColumn).getS)

      if (allOperationsSorted.nonEmpty) {
        val grouped = groupConsecutiveOperations(allOperationsSorted.toIndexedSeq)

        val (totalPuts, totalDeletes) = grouped.foldLeft((0L, 0L)) {
          case ((putAcc, deleteAcc), (operationType, batch)) =>
            if (operationType == DELETE_OPERATION_TYPE) {
              val deletableItems = spark.sparkContext.parallelize(batch).map { item =>
                item
                  .entrySet()
                  .stream()
                  .filter(e => e.getKey != operationTypeColumn && e.getKey != sequenceNumberColumn)
                  .collect(
                    Collectors.toMap(
                      (e: util.Map.Entry[String, AttributeValueV1]) => e.getKey,
                      (e: util.Map.Entry[String, AttributeValueV1]) =>
                        AttributeValueUtils.fromV1(e.getValue)
                    ))
              }
              dynamoDB.deleteRDD(target, targetTableDesc, deletableItems)(spark)
              (putAcc, deleteAcc + batch.length)
            } else {
              val writablePuts = spark.sparkContext.parallelize(batch).map { item =>
                (
                  new Text,
                  new DynamoDBItemWritable(
                    item
                      .entrySet()
                      .stream()
                      .filter(e =>
                        e.getKey != operationTypeColumn && e.getKey != sequenceNumberColumn)
                      .collect(
                        Collectors.toMap(
                          (e: util.Map.Entry[String, AttributeValueV1]) => e.getKey,
                          (e: util.Map.Entry[String, AttributeValueV1]) =>
                            AttributeValueUtils.fromV1(e.getValue)
                        ))
                  ))
              }
              dynamoDB.writeRDD(target, renamesMap, writablePuts, targetTableDesc)(spark)
              (putAcc + batch.length, deleteAcc)
            }
        }

        if (totalPuts > 0 || totalDeletes > 0) {
          log.info(s"""
                      |Changes applied in sequence order:
                      |  - ${totalPuts} items UPSERTED
                      |  - ${totalDeletes} items DELETED
                      |""".stripMargin)
        } else {
          log.info("No changes to apply")
        }
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
          newMap.put(
            sequenceNumberColumn,
            new AttributeValueV1().withS(rec.getDynamodb.getSequenceNumber))
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
