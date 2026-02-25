package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, DynamoUtils }
import com.scylladb.migrator.config.{ SourceSettings, TargetSettings }
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteItemRequest,
  PutItemRequest,
  ShardIteratorType,
  TableDescription
}

import java.util
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  type DynamoItem = util.Map[String, AttributeValue]

  private val operationTypeColumn = "_dynamo_op_type"
  private val putOperation = AttributeValue.fromBool(true)
  private val deleteOperation = AttributeValue.fromBool(false)

  private[writers] def run(
    msgs: RDD[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription
  )(implicit spark: SparkSession): Unit = {
    val rdd = msgs.flatMap(_.toSeq)

    val putCount = spark.sparkContext.longAccumulator("putCount")
    val deleteCount = spark.sparkContext.longAccumulator("deleteCount")
    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet

    rdd.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val client =
          DynamoUtils.buildDynamoClient(
            target.endpoint,
            target.finalCredentials.map(_.toProvider),
            target.region,
            Seq.empty
          )
        try
          partition.foreach { item =>
            val isPut = item.get(operationTypeColumn) == putOperation

            val itemWithoutOp = item.asScala.collect {
              case (k, v) if k != operationTypeColumn => k -> v
            }.asJava

            if (isPut) {
              putCount.add(1)
              val finalItem = itemWithoutOp.asScala.map { case (key, value) =>
                renamesMap.getOrElse(key, key) -> value
              }.asJava
              try
                client.putItem(
                  PutItemRequest.builder().tableName(target.table).item(finalItem).build()
                )
              catch {
                case e: Exception =>
                  log.error(s"Failed to put item into ${target.table}", e)
              }
            } else {
              deleteCount.add(1)
              val keyToDelete = itemWithoutOp.asScala
                .filter { case (key, _) => keyAttributeNames.contains(key) }
                .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
                .asJava
              try
                client.deleteItem(
                  DeleteItemRequest.builder().tableName(target.table).key(keyToDelete).build()
                )
              catch {
                case e: Exception =>
                  log.error(s"Failed to delete item from ${target.table}", e)
              }
            }
          }
        finally
          client.close()
      }
    }

    if (putCount.value > 0 || deleteCount.value > 0) {
      log.info(s"""
                  |Changes to be applied:
                  |  - ${putCount.value} items to UPSERT
                  |  - ${deleteCount.value} items to DELETE
                  |""".stripMargin)
    } else {
      log.info("No changes to apply")
    }
  }

  def createDStream(
    spark: SparkSession,
    streamingContext: StreamingContext,
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String]
  ): Unit = {
    val sourceClient =
      DynamoUtils.buildDynamoClient(
        src.endpoint,
        src.finalCredentials.map(_.toProvider),
        src.region,
        Seq.empty
      )
    val streamsClient =
      DynamoUtils.buildDynamoStreamsClient(
        src.endpoint,
        src.finalCredentials.map(_.toProvider),
        src.region
      )

    val streamArn = DynamoStreamPoller.getStreamArn(sourceClient, src.table)
    log.info(s"Stream ARN: $streamArn")

    // Track shard iterators across batches
    var shardIterators = Map.empty[String, String]
    var initialized = false

    streamingContext.queueStream(
      {
        val queue =
          new scala.collection.mutable.Queue[RDD[Option[DynamoItem]]]()

        streamingContext.sparkContext.addSparkListener(
          new org.apache.spark.scheduler.SparkListener {}
        )

        queue
      },
      oneAtATime = true
    )

    // Use a receiver-less approach: poll in the driver on each batch interval
    streamingContext.addStreamingListener(
      new org.apache.spark.streaming.scheduler.StreamingListener {
        override def onBatchCompleted(
          event: org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
        ): Unit =
          pollAndProcess()
      }
    )

    def pollAndProcess(): Unit =
      try {
        val shards = DynamoStreamPoller.listShards(streamsClient, streamArn)
        val shardIds = shards.map(_.shardId()).toSet

        // Initialize iterators for new shards
        for {
          shard <- shards
          if !shardIterators.contains(shard.shardId())
        } {
          val iteratorType =
            if (initialized) ShardIteratorType.LATEST else ShardIteratorType.TRIM_HORIZON
          val iterator = DynamoStreamPoller.getShardIterator(
            streamsClient,
            streamArn,
            shard.shardId(),
            iteratorType
          )
          shardIterators = shardIterators + (shard.shardId() -> iterator)
        }
        initialized = true

        // Remove closed shards that no longer appear
        shardIterators = shardIterators.filter { case (id, _) => shardIds.contains(id) }

        // Poll all shards
        val allItems = scala.collection.mutable.Buffer.empty[Option[DynamoItem]]
        val updatedIterators =
          scala.collection.mutable.Map.empty[String, String]

        for ((shardId, iterator) <- shardIterators) {
          val (records, nextIterator) =
            DynamoStreamPoller.getRecords(streamsClient, iterator)

          for (record <- records) {
            val maybeItem = DynamoStreamPoller.recordToItem(
              record,
              operationTypeColumn,
              putOperation,
              deleteOperation
            )
            allItems += maybeItem
          }

          nextIterator match {
            case Some(next) => updatedIterators(shardId) = next
            case None       => // shard is closed, don't track it anymore
          }
        }
        shardIterators = updatedIterators.toMap

        if (allItems.nonEmpty) {
          val rdd = spark.sparkContext.parallelize(allItems.toSeq)
          run(rdd, target, renamesMap, targetTableDesc)(spark)
        }
      } catch {
        case e: Exception =>
          log.error("Error polling DynamoDB stream", e)
      }

    // Do an initial poll
    pollAndProcess()
  }
}
