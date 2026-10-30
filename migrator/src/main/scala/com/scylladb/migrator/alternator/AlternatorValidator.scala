package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ writers, DynamoUtils }
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers
import com.scylladb.migrator.validation.{
  PartitionKeyedAccumulator,
  PartitionRepairSummary,
  RowComparisonFailure
}
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.logging.log4j.LogManager
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, DescribeTableRequest }

import scala.collection.mutable.ListBuffer
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

    val failures: List[RowComparisonFailure] =
      if (!configValidation.copyMissingRows)
        joinedRdd
          .flatMap { case (_, (l, r)) =>
            RowComparisonFailure.compareDynamoDBRows(
              l,
              r,
              renamedColumn,
              configValidation.floatingPointTolerance,
              configValidation.numericTypePolicy
            )
          }
          .take(configValidation.failuresToFetch)
          .toList
      else {
        log.info("Copying missing rows from source to target")

        val repairAccumulator = new PartitionKeyedAccumulator[PartitionRepairSummary]()
        spark.sparkContext.register(repairAccumulator, "AlternatorValidator repair summary")

        // Cheap AWS API lookup, not a Spark action, so it's fine to always perform it rather
        // than guard it behind a row count that would otherwise require a separate action.
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

        // The comparison and the repair item are both derived from a single pass over
        // `joinedRdd`, so there is no second action that would require persisting it. See
        // ScyllaValidator for the detailed rationale: the per-partition failure sample and
        // repaired-row count are accumulated locally and reported exactly once per task
        // completion, keyed by (partitionId, stageAttemptNumber), to be resilient to the
        // SPARK-9809/SPARK-10042 stage-resubmission double-count mechanism. This matters more
        // here than for ScyllaValidator because `joinedRdd` is produced by a shuffle-based
        // `leftOuterJoin`, which is exactly the kind of upstream stage whose fetch failures
        // trigger the result stage (this one) to be resubmitted.
        val missingAsHadoop: RDD[(Text, DynamoDBItemWritable)] = joinedRdd.mapPartitions {
          partitionIter =>
            val tc = TaskContext.get()
            val partitionId = tc.partitionId()
            val stageAttemptNumber = tc.stageAttemptNumber()

            var missingCount = 0L
            val localFailures = ListBuffer.empty[RowComparisonFailure]

            val result = partitionIter.flatMap { case (_, (sourceItem, targetItem)) =>
              RowComparisonFailure
                .compareDynamoDBRows(
                  sourceItem,
                  targetItem,
                  renamedColumn,
                  configValidation.floatingPointTolerance,
                  configValidation.numericTypePolicy
                )
                .foreach { failure =>
                  if (localFailures.size < configValidation.failuresToFetch)
                    localFailures += failure
                }

              if (targetItem.isEmpty) {
                missingCount += 1
                val item = new java.util.HashMap[String, AttributeValue]()
                sourceItem.foreach { case (k, v) =>
                  item.put(k, DdbValue.toAttributeValue(v))
                }
                Some((new Text(), new DynamoDBItemWritable(item)))
              } else None
            }

            // Keep the listener body trivial and non-throwing: it fires on success, failure, or
            // cancellation alike, but a failed/cancelled attempt's update is discarded by
            // Spark's scheduler regardless (never merged into the driver-side accumulator).
            tc.addTaskCompletionListener[Unit] { _ =>
              repairAccumulator.add(
                (
                  partitionId,
                  stageAttemptNumber,
                  PartitionRepairSummary(missingCount, localFailures.toList)
                )
              )
            }

            result
        }

        writers.DynamoDB.writeRDD(
          targetSettings,
          renamedColumn,
          missingAsHadoop,
          targetTableDesc
        )

        val repairSummary =
          PartitionRepairSummary.combine(repairAccumulator.values, configValidation.failuresToFetch)

        log.info(
          s"Finished copying missing rows to target: ${repairSummary.missingCount} missing row(s) copied"
        )

        repairSummary.failureSample
      }

    failures
  }

}
