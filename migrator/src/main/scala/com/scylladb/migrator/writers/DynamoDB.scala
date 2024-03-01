package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.config.{ Rename, TargetSettings }
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DynamoDB {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDB")

  def writeRDD(target: TargetSettings.DynamoDB,
               renames: List[Rename],
               rdd: RDD[(Text, DynamoDBItemWritable)],
               targetTableDesc: Option[TableDescription])(implicit spark: SparkSession): Unit = {
    val provisionedThroughput = targetTableDesc.flatMap(p => Option(p.getProvisionedThroughput))
    val readThroughput = provisionedThroughput.flatMap(p => Option(p.getReadCapacityUnits))
    val writeThroughput = provisionedThroughput.flatMap(p => Option(p.getWriteCapacityUnits))

    val throughput =
      if (readThroughput.isEmpty || writeThroughput.isEmpty) Some("25")
      else None

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    def setOptionalConf(name: String, maybeValue: Option[String]): Unit =
      for (value <- maybeValue) {
        jobConf.set(name, value)
      }

    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, target.table)
    setOptionalConf(DynamoDBConstants.REGION, target.region)
    setOptionalConf(DynamoDBConstants.ENDPOINT, target.endpoint.map(_.renderEndpoint))
    setOptionalConf(DynamoDBConstants.READ_THROUGHPUT, throughput)
    setOptionalConf(
      DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
      target.throughputWritePercent.map(_.toString))
    setOptionalConf(DynamoDBConstants.SCAN_SEGMENTS, target.scanSegments.map(_.toString))
    setOptionalConf(DynamoDBConstants.MAX_MAP_TASKS, target.maxMapTasks.map(_.toString))
    setOptionalConf(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, target.credentials.map(_.accessKey))
    setOptionalConf(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, target.credentials.map(_.secretKey))
    jobConf.set(
      "mapred.output.format.class",
      "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

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
