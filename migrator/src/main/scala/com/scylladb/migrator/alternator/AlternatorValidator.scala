package com.scylladb.migrator.alternator

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchWriteItemRequest,
  PutRequest,
  WriteRequest
}

import scala.jdk.CollectionConverters._

object AlternatorValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.alternator")
  private val MaxBatchWriteSize = 25
  private val MaxBatchRetries = 3
  private val RetryBaseDelayMs = 100

  /** Writes a batch of items via BatchWriteItem, retrying unprocessed items up to
    * [[MaxBatchRetries]] times with linear back-off.
    */
  private def batchWriteWithRetry(
    dynamoDB: software.amazon.awssdk.services.dynamodb.DynamoDbClient,
    tableName: String,
    items: java.util.List[WriteRequest]
  ): Unit = {
    var unprocessed: java.util.List[WriteRequest] = items
    var attempt = 0
    while (!unprocessed.isEmpty && attempt < MaxBatchRetries) {
      val response = dynamoDB.batchWriteItem(
        BatchWriteItemRequest
          .builder()
          .requestItems(java.util.Collections.singletonMap(tableName, unprocessed))
          .build()
      )
      val remaining = response.unprocessedItems.get(tableName)
      unprocessed =
        if (remaining != null) remaining
        else java.util.Collections.emptyList[WriteRequest]()
      if (!unprocessed.isEmpty) {
        attempt += 1
        if (attempt < MaxBatchRetries)
          Thread.sleep(RetryBaseDelayMs.toLong * attempt)
      }
    }
    if (!unprocessed.isEmpty)
      throw new RuntimeException(
        s"BatchWriteItem still has ${unprocessed.size()} unprocessed items " +
          s"for table $tableName after $MaxBatchRetries attempts"
      )
  }

  /** Checks that the target Alternator database contains the same data as the source DynamoDB
    * database.
    *
    * When `copyMissingRows` is enabled in the validation config, rows that exist in the source but
    * are missing in the target are copied to the target. Note that the returned failure list is a
    * snapshot taken ''before'' the copy, so it may contain `MissingTargetRow` entries for rows that
    * have since been written. A subsequent re-validation can be used to confirm convergence.
    *
    * @return
    *   A list of comparison failures (which is empty if the data are the same in both databases).
    */
  def runValidation(
    sourceSettings: SourceSettings.DynamoDB,
    targetSettings: TargetSettings.DynamoDB,
    config: MigratorConfig
  )(implicit spark: SparkSession): List[RowComparisonFailure] = {

    val (source, sourceTableDesc) = readers.DynamoDB.readRDD(spark, sourceSettings, None)
    val sourceTableKeys = sourceTableDesc.keySchema.asScala.toList

    val sourceByKey: RDD[(List[DdbValue], collection.Map[String, DdbValue])] =
      source
        .map { case (_, item) =>
          val key = sourceTableKeys
            .map(keySchemaElement =>
              DdbValue.from(item.getItem.get(keySchemaElement.attributeName))
            )
          (key, item.getItem.asScala.map { case (k, v) => (k, DdbValue.from(v)) })
        }

    val (target, _) = readers.DynamoDB.readRDD(
      spark,
      targetSettings.endpoint,
      targetSettings.finalCredentials,
      targetSettings.region,
      targetSettings.table,
      sourceSettings.scanSegments, // Reuse same settings as source table
      sourceSettings.maxMapTasks,
      sourceSettings.readThroughput,
      sourceSettings.throughputReadPercent,
      skipSegments           = None,
      removeConsumedCapacity = targetSettings.removeConsumedCapacity.getOrElse(false)
    )

    // Define some aliases to prevent the Spark engine to try to serialize the whole object graph
    val renamedColumn = config.renamesMap
    val configValidation = config.validation.getOrElse(
      sys.error("Missing required property 'validation' in the configuration file.")
    )

    val targetByKey: RDD[(List[DdbValue], collection.Map[String, DdbValue])] =
      target
        .map { case (_, item) =>
          val key = sourceTableKeys
            .map { keySchemaElement =>
              val columnName = keySchemaElement.attributeName
              DdbValue.from(item.getItem.get(renamedColumn(columnName)))
            }
          (key, item.getItem.asScala.map { case (k, v) => (k, DdbValue.from(v)) })
        }

    val joinedRdd = sourceByKey.leftOuterJoin(targetByKey)

    val cachedJoined =
      if (configValidation.copyMissingRows) joinedRdd.persist(StorageLevel.MEMORY_AND_DISK)
      else joinedRdd

    try {
      val failures = cachedJoined
        .flatMap { case (_, (l, r)) =>
          RowComparisonFailure.compareDynamoDBRows(
            l,
            r,
            renamedColumn,
            configValidation.floatingPointTolerance
          )
        }
        .take(configValidation.failuresToFetch)
        .toList

      if (configValidation.copyMissingRows) {
        log.info("Copying missing rows from source to target")

        val targetEndpoint = targetSettings.endpoint
        val targetCredentials = targetSettings.finalCredentials
        val targetRegion = targetSettings.region
        val targetTable = targetSettings.table
        val targetAlternator = targetSettings.alternator

        cachedJoined
          .filter { case (_, (_, r)) => r.isEmpty }
          .map { case (_, (sourceItem, _)) => sourceItem }
          .foreachPartition { partition =>
            if (partition.nonEmpty) {
              val dynamoDB = DynamoUtils.buildDynamoClient(
                targetEndpoint,
                targetCredentials.map(_.toProvider),
                targetRegion,
                Seq.empty,
                targetAlternator
              )
              try
                partition.grouped(MaxBatchWriteSize).foreach { batch =>
                  val writeRequests = batch
                    .map { sourceItem =>
                      val item = new java.util.HashMap[String, AttributeValue]()
                      sourceItem.foreach { case (k, v) =>
                        item.put(renamedColumn(k), DdbValue.toAttributeValue(v))
                      }
                      WriteRequest
                        .builder()
                        .putRequest(PutRequest.builder().item(item).build())
                        .build()
                    }
                    .toList
                    .asJava

                  batchWriteWithRetry(dynamoDB, targetTable, writeRequests)
                }
              finally
                dynamoDB.close()
            }
          }

        log.info("Finished copying missing rows to target")
      }

      failures
    } finally
      if (configValidation.copyMissingRows) cachedJoined.unpersist()
  }

}
