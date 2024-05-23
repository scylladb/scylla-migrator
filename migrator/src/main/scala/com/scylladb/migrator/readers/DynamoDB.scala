package com.scylladb.migrator.readers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.alternator.DynamoDBInputFormat
import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, SourceSettings }
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DynamoDB {

  def readRDD(
    spark: SparkSession,
    source: SourceSettings.DynamoDB): (RDD[(Text, DynamoDBItemWritable)], TableDescription) =
    readRDD(
      spark,
      source.endpoint,
      source.credentials,
      source.region,
      source.table,
      source.scanSegments,
      source.maxMapTasks,
      source.readThroughput,
      source.throughputReadPercent
    )

  /**
    * Overload of `readRDD` that does not depend on `SourceSettings.DynamoDB`
    */
  def readRDD(
    spark: SparkSession,
    endpoint: Option[DynamoDBEndpoint],
    credentials: Option[AWSCredentials],
    region: Option[String],
    table: String,
    scanSegments: Option[Int],
    maxMapTasks: Option[Int],
    readThroughput: Option[Int],
    throughputReadPercent: Option[Float]): (RDD[(Text, DynamoDBItemWritable)], TableDescription) = {

    val tableDescription = DynamoUtils
      .buildDynamoClient(endpoint, credentials.map(_.toAWSCredentialsProvider), region)
      .describeTable(table)
      .getTable

    val jobConf =
      makeJobConf(
        spark,
        endpoint,
        credentials,
        region,
        table,
        scanSegments,
        maxMapTasks,
        readThroughput,
        throughputReadPercent,
        tableDescription)

    val rdd =
      spark.sparkContext.hadoopRDD(
        jobConf,
        classOf[DynamoDBInputFormat],
        classOf[Text],
        classOf[DynamoDBItemWritable])
    (rdd, tableDescription)
  }

  private[migrator] def makeJobConf(
    spark: SparkSession,
    endpoint: Option[DynamoDBEndpoint],
    credentials: Option[AWSCredentials],
    region: Option[String],
    table: String,
    scanSegments: Option[Int],
    maxMapTasks: Option[Int],
    readThroughput: Option[Int],
    throughputReadPercent: Option[Float],
    description: TableDescription
  ): JobConf = {
    val maybeItemCount = Option(description.getItemCount).map(_.toLong)
    val maybeAvgItemSize =
      for {
        itemCount <- maybeItemCount
        if itemCount != 0L
        tableSize <- Option(description.getTableSizeBytes)
      } yield tableSize / itemCount

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    setDynamoDBJobConf(
      jobConf,
      region,
      endpoint,
      scanSegments,
      maxMapTasks,
      credentials
    )
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, table)
    setOptionalConf(jobConf, DynamoDBConstants.ITEM_COUNT, maybeItemCount.map(_.toString))
    setOptionalConf(jobConf, DynamoDBConstants.AVG_ITEM_SIZE, maybeAvgItemSize.map(_.toString))
    setOptionalConf(jobConf, DynamoDBConstants.TABLE_SIZE_BYTES, Option(description.getTableSizeBytes).map(_.toString))
    jobConf.set(
      DynamoDBConstants.READ_THROUGHPUT,
      readThroughput
        .getOrElse(DynamoUtils.tableReadThroughput(description))
        .toString)
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_READ_PERCENT,
      throughputReadPercent.map(_.toString))

    jobConf
  }

}
