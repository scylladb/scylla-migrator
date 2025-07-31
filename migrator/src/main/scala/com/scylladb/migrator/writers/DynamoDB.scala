package com.scylladb.migrator.writers

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteItemRequest,
  TableDescription
}

import java.util
import java.util.stream.Collectors

object DynamoDB {

  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDB")

  def deleteRDD(target: TargetSettings.DynamoDB,
                targetTableDesc: TableDescription,
                rdd: RDD[util.Map[String, AttributeValue]])(implicit spark: SparkSession): Unit = {

    val keySchema = targetTableDesc.keySchema()

    rdd.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val dynamoDB = DynamoUtils.buildDynamoClient(
          target.endpoint,
          target.finalCredentials.map(_.toProvider),
          target.region
        )

        try {
          partition.foreach { item =>
            val keyToDelete =
              new util.HashMap[String, AttributeValue]()

            keySchema.forEach { keyElement =>
              val keyName = keyElement.attributeName()
              if (item.containsKey(keyName)) {
                keyToDelete.put(keyName, item.get(keyName))
              }
            }

            if (!keyToDelete.isEmpty) {
              try {
                dynamoDB.deleteItem(
                  DeleteItemRequest
                    .builder()
                    .tableName(target.table)
                    .key(keyToDelete)
                    .build()
                )
              } catch {
                case e: Exception =>
                  log.error(
                    s"Failed to delete item with key ${keyToDelete} from table ${target.table}",
                    e)
              }
            }
          }
        } finally {
          dynamoDB.close()
        }
      }
    }
  }

  def writeRDD(target: TargetSettings.DynamoDB,
               renamesMap: Map[String, String],
               rdd: RDD[(Text, DynamoDBItemWritable)],
               targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    setDynamoDBJobConf(
      jobConf,
      target.region,
      target.endpoint,
      maybeScanSegments = None,
      maybeMaxMapTasks  = None,
      target.finalCredentials)
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, target.table)
    val writeThroughput =
      target.writeThroughput match {
        case Some(value) =>
          log.info(
            s"Setting up Hadoop job to write table using a configured throughput of ${value} WCU(s)")
          value
        case None =>
          val value = DynamoUtils.tableWriteThroughput(targetTableDesc)
          log.info(
            s"Setting up Hadoop job to write table using a calculated throughput of ${value} WCU(s)")
          value
      }
    jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT, writeThroughput.toString)
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
      target.throughputWritePercent.map(_.toString))

    val finalRdd =
      if (renamesMap.isEmpty) rdd
      else
        rdd.mapValues { itemWritable =>
          val item = new util.HashMap[String, AttributeValue]()
          itemWritable.getItem.forEach((key, value) => item.put(renamesMap(key), value))
          itemWritable.setItem(item)
          itemWritable
        }
    finalRdd.saveAsHadoopDataset(jobConf)
  }
}
