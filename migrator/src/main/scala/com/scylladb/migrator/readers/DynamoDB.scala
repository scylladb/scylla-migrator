package com.scylladb.migrator.readers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
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
    setDynamoDBJobConf(
      jobConf,
      source.region,
      source.endpoint,
      source.scanSegments,
      source.maxMapTasks,
      source.credentials)
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, source.table)
    setOptionalConf(jobConf, DynamoDBConstants.READ_THROUGHPUT, throughput)
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_READ_PERCENT,
      source.throughputReadPercent.map(_.toString))

    spark.sparkContext.hadoopRDD(
      jobConf,
      classOf[DynamoDBInputFormat],
      classOf[Text],
      classOf[DynamoDBItemWritable])
  }

}
