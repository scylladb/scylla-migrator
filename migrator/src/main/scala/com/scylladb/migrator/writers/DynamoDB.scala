package com.scylladb.migrator.writers

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.{
  DynamoDBConstants,
  DynamoDBItemWritable,
  LoadBalancedDynamoDBClient
}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, TableDescription }

import java.util

object DynamoDB {

  def writeRDD(target: TargetSettings.DynamoDB,
               renamesMap: Map[String, String],
               rdd: RDD[(Text, DynamoDBItemWritable)],
               targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    setDynamoDBJobConf(
      jobConf,
      target.region,
      target.endpoint,
      target.scanSegments,
      target.maxMapTasks,
      target.finalCredentials)
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, target.table)
    val writeThroughput =
      target.writeThroughput.getOrElse(DynamoUtils.tableWriteThroughput(targetTableDesc))
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
    finalRdd
      .mapPartitions { partitions =>
        // Adjust the log level of the DynamoDBClient logger in the executors, see https://github.com/scylladb/scylla-migrator/issues/167
        LogManager
          .getLogger(classOf[LoadBalancedDynamoDBClient])
          .setLevel(Level.WARN)
        partitions
      }
      .saveAsHadoopDataset(jobConf)
  }
}
