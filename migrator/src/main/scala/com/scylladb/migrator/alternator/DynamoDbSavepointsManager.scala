package com.scylladb.migrator.alternator

import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.split.DynamoDBSplit
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.InputSplit
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListener, SparkListenerTaskEnd }
import org.apache.spark.{ Partition, SerializableWritable, SparkContext, Success => TaskEndSuccess }

import scala.util.{ Failure, Success, Try }

/**
  * Manage DynamoDB-based migrations by tracking the migrated scan segments.
  */
class DynamoDbSavepointsManager(migratorConfig: MigratorConfig,
                                segmentsAccumulator: IntSetAccumulator)
    extends SavepointsManager(migratorConfig) {

  def describeMigrationState(): String =
    s"Segments to skip: ${segmentsAccumulator.value}"

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(skipSegments = Some(segmentsAccumulator.value))

}

object DynamoDbSavepointsManager {

  private val log = LogManager.getLogger(classOf[DynamoDbSavepointsManager])

  def apply(migratorConfig: MigratorConfig,
            segmentsAccumulator: IntSetAccumulator): DynamoDbSavepointsManager =
    new DynamoDbSavepointsManager(migratorConfig, segmentsAccumulator)

  /**
    * Set up a savepoints manager that tracks the scan segments migrated from the source RDD.
    */
  def setup(migratorConfig: MigratorConfig,
            sourceRDD: RDD[(Text, DynamoDBItemWritable)],
            spark: SparkContext): DynamoDbSavepointsManager = {
    val segmentsAccumulator =
      IntSetAccumulator(migratorConfig.skipSegments.getOrElse(Set.empty))
    spark.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val partitionId = taskEnd.taskInfo.partitionId
        log.debug(s"Migration of partition ${partitionId} ended: ${taskEnd.reason}.")
        if (taskEnd.reason == TaskEndSuccess) {
          scanSegments(sourceRDD, partitionId) match {
            case Success(segments) =>
              segments.forEach(segment => segmentsAccumulator.add(segment))
              log.info(s"Marked segments ${segments} as migrated.")
            case Failure(error) =>
              log.error(
                s"Unable to collect the segments scanned in partition ${partitionId}. The next savepoint will not include them.",
                error)
          }
        }
      }
    })
    DynamoDbSavepointsManager(migratorConfig, segmentsAccumulator)
  }

  /**
    * @return The scan segments processed in partition `partitionId` of `rdd`.
    */
  private def scanSegments(rdd: RDD[(Text, DynamoDBItemWritable)],
                           partitionId: Int): Try[java.util.List[Integer]] =
    if (partitionId >= 0 && partitionId < rdd.getNumPartitions) {
      val partition = rdd.partitions(partitionId)
      inputSplit(partition).map(_.getSegments)
    } else {
      Failure(new Exception(s"Partition ${partitionId} not found in the RDD."))
    }

  /**
    * @return The `DynamoDBSplit` wrapped by the `partition`.
    *         Fails if the `partition` is not a `HadoopPartition` containing a `DynamoDBSplit`.
    */
  private def inputSplit(partition: Partition): Try[DynamoDBSplit] = Try {
    // Unfortunately, class `HadoopPartition` is private, so we can’t simply
    // pattern match on it. We use reflection to access its `inputSplit` member.
    if (partition.getClass.getName != "org.apache.spark.rdd.HadoopPartition") {
      throw new Exception(s"Unexpected partition type: ${partition.getClass.getName}.")
    }
    val inputSplitMember = partition.getClass.getMethod("inputSplit")
    val inputSplitResult =
      inputSplitMember.invoke(partition).asInstanceOf[SerializableWritable[InputSplit]]
    inputSplitResult.value match {
      case dynamoDbSplit: DynamoDBSplit => dynamoDbSplit
      case other                        => throw new Exception(s"Unexpected InputSplit type: ${other.getClass.getName}.")
    }
  }

}
