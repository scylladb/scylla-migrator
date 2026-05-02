package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ writers, DynamoUtils }
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, DescribeTableRequest }

import scala.jdk.CollectionConverters._

object AlternatorValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.alternator.AlternatorValidator")

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
    sourceSettings: SourceSettings.DynamoDBLike,
    targetSettings: TargetSettings.DynamoDBLike,
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
      removeConsumedCapacity = targetSettings.removeConsumedCapacity,
      alternatorSettings     = targetSettings.alternatorSettings
    )

    // Define some aliases to prevent the Spark engine to try to serialize the whole object graph
    val renamedColumn = config.renamesMap
    val configValidation = config.validation.getOrElse(
      sys.error("Missing required property 'validation' in the configuration file.")
    )

    configValidation.hashColumns.foreach { _ =>
      log.warn(
        "hashColumns is only supported for MySQL-to-ScyllaDB validation and will be ignored."
      )
    }

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

        val missingRowsRdd =
          cachedJoined.filter { case (_, (_, r)) => r.isEmpty }
        val missingRowCount = missingRowsRdd.count()

        val missingAsHadoop = missingRowsRdd.map { case (_, (sourceItem, _)) =>
          val item = new java.util.HashMap[String, AttributeValue]()
          sourceItem.foreach { case (k, v) =>
            item.put(k, DdbValue.toAttributeValue(v))
          }
          (new Text(), new DynamoDBItemWritable(item))
        }

        val targetTableDesc = {
          val client = DynamoUtils.buildDynamoClient(
            targetSettings.endpoint,
            targetSettings.finalCredentials.map(_.toProvider),
            targetSettings.region,
            Seq.empty,
            targetSettings.alternatorSettings
          )
          try
            client
              .describeTable(
                DescribeTableRequest.builder().tableName(targetSettings.table).build()
              )
              .table()
          finally
            client.close()
        }

        writers.DynamoDB.writeRDD(
          targetSettings,
          renamedColumn,
          missingAsHadoop,
          targetTableDesc
        )

        log.info(
          s"Finished copying missing rows to target: $missingRowCount missing row(s) copied"
        )
      }

      failures
    } finally
      if (configValidation.copyMissingRows) cachedJoined.unpersist()
  }

}
