package com.scylladb.migrator.readers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.config.SourceSettings
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DynamoDB {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDB")

  def readRDD(spark: SparkSession,
              source: SourceSettings.DynamoDB,
              description: TableDescription): RDD[(Text, DynamoDBItemWritable)] = {
    val provisionedThroughput = Option(description.getProvisionedThroughput)
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
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, source.table)
    setOptionalConf(DynamoDBConstants.REGION, source.region)
    setOptionalConf(DynamoDBConstants.ENDPOINT, source.endpoint.map(_.renderEndpoint))
    setOptionalConf(DynamoDBConstants.READ_THROUGHPUT, throughput)
    setOptionalConf(
      DynamoDBConstants.THROUGHPUT_READ_PERCENT,
      source.throughputReadPercent.map(_.toString))
    setOptionalConf(DynamoDBConstants.SCAN_SEGMENTS, source.scanSegments.map(_.toString))
    setOptionalConf(DynamoDBConstants.MAX_MAP_TASKS, source.maxMapTasks.map(_.toString))
    setOptionalConf(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, source.credentials.map(_.accessKey))
    setOptionalConf(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, source.credentials.map(_.secretKey))
    jobConf.set(
      "mapred.output.format.class",
      "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

    spark.sparkContext.hadoopRDD(
      jobConf,
      classOf[DynamoDBInputFormat],
      classOf[Text],
      classOf[DynamoDBItemWritable])
  }

}
