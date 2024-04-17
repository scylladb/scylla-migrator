package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf, tableThroughput }
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import com.scylladb.migrator.readers
import org.apache.hadoop.dynamodb.DynamoDBConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

object AlternatorValidator {

  /**
    * Checks that the target Alternator database contains the same data
    * as the source DynamoDB database.
    *
    * @return A list of comparison failures (which is empty if the data
    *         are the same in both databases).
    */
  def runValidation(
    sourceSettings: SourceSettings.DynamoDB,
    targetSettings: TargetSettings.DynamoDB,
    config: MigratorConfig)(implicit spark: SparkSession): List[RowComparisonFailure] = {

    val (source, sourceTableDesc) = readers.DynamoDB.readRDD(spark, sourceSettings)
    val sourceTableKeys = sourceTableDesc.getKeySchema.asScala.toList

    val sourceByKey: RDD[(List[AttributeValue], util.Map[String, AttributeValue])] =
      source
        .map {
          case (_, item) =>
            val key = sourceTableKeys
              .map(keySchemaElement => item.getItem.get(keySchemaElement.getAttributeName))
            (key, item.getItem)
        }

    val (target, _) = readers.DynamoDB.readRDD(
      spark,
      targetSettings.endpoint,
      targetSettings.credentials,
      targetSettings.region,
      targetSettings.table
    ) { (jobConf, targetTableDesc) =>
      setDynamoDBJobConf(
        jobConf,
        targetSettings.region,
        targetSettings.endpoint,
        targetSettings.scanSegments,
        targetSettings.maxMapTasks,
        targetSettings.credentials)
      jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, targetSettings.table)
      setOptionalConf(
        jobConf,
        DynamoDBConstants.READ_THROUGHPUT,
        tableThroughput(Option(targetTableDesc)))
    }

    // Define some aliases to prevent the Spark engine to try to serialize the whole object graph
    val renamedColumn = config.renamesMap
    val configValidation = config.validation

    val targetByKey: RDD[(List[AttributeValue], util.Map[String, AttributeValue])] =
      target
        .map {
          case (_, item) =>
            val key = sourceTableKeys
              .map { keySchemaElement =>
                val columnName = keySchemaElement.getAttributeName
                item.getItem.get(renamedColumn(columnName))
              }
            (key, item.getItem)
        }

    sourceByKey
      .leftOuterJoin(targetByKey)
      .flatMap {
        case (_, (l, r)) =>
          RowComparisonFailure.compareDynamoDBRows(
            l.asScala.toMap,
            r.map(_.asScala.toMap),
            renamedColumn,
            configValidation.floatingPointTolerance
          )
      }
      .take(config.validation.failuresToFetch)
      .toList
  }

}
