package com.scylladb.migrator.alternator

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.config.{ Rename, SourceSettings, TargetSettings }
import com.scylladb.migrator.{ readers, writers }
import com.scylladb.migrator.writers.DynamoStreamReplication
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import scala.util.control.NonFatal

object AlternatorMigrator {
  private val log = LogManager.getLogger("com.scylladb.migrator.alternator")

  def migrate(source: SourceSettings.DynamoDB,
              target: TargetSettings.DynamoDB,
              renames: List[Rename])(implicit spark: SparkSession): Unit = {

    val (sourceRDD, sourceTableDesc) = readers.DynamoDB.readRDD(spark, source)

    log.info("We need to transfer: " + sourceRDD.getNumPartitions + " partitions in total")

    log.info("Starting write...")

    try {
      val targetTableDesc = {
        if (target.streamChanges) {
          log.info(
            "Source is a Dynamo table and change streaming requested; enabling Dynamo Stream")
          DynamoUtils.enableDynamoStream(source)
        }

        DynamoUtils.replicateTableDefinition(
          sourceTableDesc,
          target
        )
      }

      if (target.streamChanges && target.skipInitialSnapshotTransfer.contains(true)) {
        log.info("Skip transferring table snapshot")
      } else {
        writers.DynamoDB.writeRDD(target, renames, sourceRDD, Some(targetTableDesc))
        log.info("Done transferring table snapshot")
      }

      if (target.streamChanges) {
        log.info("Starting to transfer changes")
        val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        DynamoStreamReplication.createDStream(
          spark,
          streamingContext,
          source,
          target,
          targetTableDesc,
          renames)

        streamingContext.start()
        streamingContext.awaitTermination()
      }
    } catch {
      case NonFatal(e) =>
        log.error("Caught error while writing the RDD.", e)
    }

  }

}
