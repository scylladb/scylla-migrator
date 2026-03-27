package com.scylladb.migrator.alternator

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import com.scylladb.migrator.readers
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import scala.jdk.CollectionConverters._

object AlternatorValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.alternator")

  /** Checks that the target Alternator database contains the same data as the source DynamoDB
    * database.
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

    val failures = joinedRdd
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
      val missingRowCount = failures.count(
        _.items.contains(RowComparisonFailure.Item.MissingTargetRow)
      )
      if (missingRowCount > 0) {
        log.info(s"Detected ${missingRowCount} missing rows, copying from source to target")

        // Alias fields needed in the closure to avoid serializing the whole settings objects
        val targetEndpoint    = targetSettings.endpoint
        val targetCredentials = targetSettings.finalCredentials
        val targetRegion      = targetSettings.region
        val targetTable       = targetSettings.table
        val targetAlternator  = targetSettings.alternator

        joinedRdd
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
                partition.foreach { sourceItem =>
                  val item = new java.util.HashMap[String, AttributeValue]()
                  sourceItem.foreach { case (k, v) =>
                    item.put(renamedColumn(k), DdbValue.toAttributeValue(v))
                  }
                  dynamoDB.putItem(
                    PutItemRequest.builder()
                      .tableName(targetTable)
                      .item(item)
                      .build()
                  )
                }
              finally
                dynamoDB.close()
            }
          }

        log.info(s"Finished copying missing rows to target")
      }
    }

    failures
  }

}
