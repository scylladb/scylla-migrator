package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf, tableThroughput }
import com.scylladb.migrator.config.{ Rename, TargetSettings }
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DynamoDB {

  def writeRDD(target: TargetSettings.DynamoDB,
               renames: List[Rename],
               rdd: RDD[(Text, DynamoDBItemWritable)],
               targetTableDesc: Option[TableDescription])(implicit spark: SparkSession): Unit = {

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    setDynamoDBJobConf(
      jobConf,
      target.region,
      target.endpoint,
      target.scanSegments,
      target.maxMapTasks,
      target.credentials)
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, target.table)
    setOptionalConf(jobConf, DynamoDBConstants.WRITE_THROUGHPUT, tableThroughput(targetTableDesc))
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
      target.throughputWritePercent.map(_.toString))

    rdd
      .mapValues { itemWritable =>
        val item = itemWritable.getItem
        for (rename <- renames) {
          item.put(rename.to, item.get(rename.from))
          item.remove(rename.from)
        }
        itemWritable
      }
      .saveAsHadoopDataset(jobConf)

  }
}
