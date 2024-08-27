package com.scylladb.migrator.readers

import com.scylladb.migrator.{ AWSCredentials, DynamoUtils }
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.{ DynamoDBEndpoint, SourceSettings }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{
  DescribeTableRequest,
  DescribeTimeToLiveRequest,
  TableDescription,
  TimeToLiveDescription,
  TimeToLiveStatus
}

object DynamoDB {

  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDB")

  def readRDD(
    spark: SparkSession,
    source: SourceSettings.DynamoDB,
    skipSegments: Option[Set[Int]]): (RDD[(Text, DynamoDBItemWritable)], TableDescription) =
    readRDD(
      spark,
      source.endpoint,
      source.finalCredentials,
      source.region,
      source.table,
      source.scanSegments,
      source.maxMapTasks,
      source.readThroughput,
      source.throughputReadPercent,
      skipSegments
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
    throughputReadPercent: Option[Float],
    skipSegments: Option[Set[Int]]): (RDD[(Text, DynamoDBItemWritable)], TableDescription) = {

    val dynamoDbClient =
      DynamoUtils.buildDynamoClient(endpoint, credentials.map(_.toProvider), region)

    val tableDescription =
      dynamoDbClient
        .describeTable(DescribeTableRequest.builder().tableName(table).build())
        .table

    val maybeTtlDescription =
      Option(
        dynamoDbClient
          .describeTimeToLive(DescribeTimeToLiveRequest.builder().tableName(table).build())
          .timeToLiveDescription
      )

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
        tableDescription,
        maybeTtlDescription,
        skipSegments)

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
    description: TableDescription,
    maybeTtlDescription: Option[TimeToLiveDescription],
    skipSegments: Option[Set[Int]]
  ): JobConf = {
    val maybeItemCount = Option(description.itemCount).map(_.toLong)
    val maybeAvgItemSize =
      for {
        itemCount <- maybeItemCount
        if itemCount != 0L
        tableSize <- Option(description.tableSizeBytes)
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
    setOptionalConf(
      jobConf,
      DynamoDBConstants.TABLE_SIZE_BYTES,
      Option(description.tableSizeBytes).map(_.toString))
    val throughput = readThroughput match {
      case Some(value) =>
        log.info(
          s"Setting up Hadoop job to read the table using a configured throughput of ${value} RCU(s)")
        value
      case None =>
        val value = DynamoUtils.tableReadThroughput(description)
        log.info(
          s"Setting up Hadoop job to read the table using a computed throughput of ${value} RCU(s)")
        value
    }
    jobConf.set(DynamoDBConstants.READ_THROUGHPUT, throughput.toString)
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_READ_PERCENT,
      throughputReadPercent.map(_.toString))
    setOptionalConf(
      jobConf,
      DynamoDBConstants.EXCLUDED_SCAN_SEGMENTS,
      skipSegments.map(_.mkString(","))
    )
    val maybeTtlAttributeName =
      maybeTtlDescription
        .filter(_.timeToLiveStatus == TimeToLiveStatus.ENABLED)
        .map(_.attributeName())
    setOptionalConf(jobConf, DynamoDBConstants.TTL_ATTRIBUTE_NAME, maybeTtlAttributeName)

    jobConf
  }

}
