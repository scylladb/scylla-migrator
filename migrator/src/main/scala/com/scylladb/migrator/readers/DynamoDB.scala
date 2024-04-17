package com.scylladb.migrator.readers

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.{ AWSCredentials, DynamoDBEndpoint, SourceSettings }
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DynamoDB {

  def readRDD(
    spark: SparkSession,
    source: SourceSettings.DynamoDB): (RDD[(Text, DynamoDBItemWritable)], TableDescription) =
    readRDD(spark, source.endpoint, source.credentials, source.region, source.table) {
      (jobConf, description) =>
        setDynamoDBJobConf(
          jobConf,
          source.region,
          source.endpoint,
          source.scanSegments,
          source.maxMapTasks,
          source.credentials)
        jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, source.table)
        setOptionalConf(
          jobConf,
          DynamoDBConstants.READ_THROUGHPUT,
          DynamoUtils.tableThroughput(Option(description)))
        setOptionalConf(
          jobConf,
          DynamoDBConstants.THROUGHPUT_READ_PERCENT,
          source.throughputReadPercent.map(_.toString))
    }

  /**
    * A lower-level overload of `readRDD` that expects the caller to fully configure the Hadoop JobConf
    * with the provided `configureJobConf` parameter.
    */
  def readRDD(spark: SparkSession,
              endpoint: Option[DynamoDBEndpoint],
              credentials: Option[AWSCredentials],
              region: Option[String],
              table: String)(
    configureJobConf: (JobConf, TableDescription) => Unit
  ): (RDD[(Text, DynamoDBItemWritable)], TableDescription) = {
    val description = DynamoUtils
      .buildDynamoClient(endpoint, credentials, region)
      .describeTable(table)
      .getTable

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    configureJobConf(jobConf, description)
    val rdd =
      spark.sparkContext.hadoopRDD(
        jobConf,
        classOf[DynamoDBInputFormat],
        classOf[Text],
        classOf[DynamoDBItemWritable])
    (rdd, description)
  }

}
