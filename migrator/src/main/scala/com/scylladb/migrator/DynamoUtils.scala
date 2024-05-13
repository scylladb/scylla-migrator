package com.scylladb.migrator

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClientBuilder,
  AmazonDynamoDBStreams,
  AmazonDynamoDBStreamsClientBuilder
}
import com.amazonaws.services.dynamodbv2.model.{
  BillingMode,
  CreateTableRequest,
  DescribeStreamRequest,
  ProvisionedThroughput,
  ProvisionedThroughputDescription,
  ResourceNotFoundException,
  StreamSpecification,
  StreamViewType,
  TableDescription,
  UpdateTableRequest
}
import com.scylladb.migrator.config.{
  AWSCredentials,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import org.apache.hadoop.dynamodb.DynamoDBConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager

import scala.util.{ Failure, Success, Try }

object DynamoUtils {
  val log = LogManager.getLogger("com.scylladb.migrator.DynamoUtils")

  def replicateTableDefinition(sourceDescription: TableDescription,
                               target: TargetSettings.DynamoDB): TableDescription = {
    // If non-existent, replicate
    val targetClient = buildDynamoClient(
      target.endpoint,
      target.credentials.map(_.toAWSCredentialsProvider),
      target.region)

    log.info("Checking for table existence at destination")
    val targetDescription = Try(targetClient.describeTable(target.table))
    targetDescription match {
      case Success(desc) =>
        log.info(s"Table ${target.table} exists at destination")
        desc.getTable

      case Failure(e: ResourceNotFoundException) =>
        val request = new CreateTableRequest()
          .withTableName(target.table)
          .withKeySchema(sourceDescription.getKeySchema)
          .withAttributeDefinitions(sourceDescription.getAttributeDefinitions)
          .withProvisionedThroughput(
            new ProvisionedThroughput(
              sourceDescription.getProvisionedThroughput.getReadCapacityUnits,
              sourceDescription.getProvisionedThroughput.getWriteCapacityUnits
            )
          )

        log.info(
          s"Table ${target.table} does not exist at destination - creating it according to definition:")
        log.info(sourceDescription.toString)
        targetClient.createTable(request)
        log.info(s"Table ${target.table} created.")

        targetClient.describeTable(target.table).getTable

      case Failure(otherwise) =>
        throw new RuntimeException("Failed to check for table existence", otherwise)
    }
  }

  def enableDynamoStream(source: SourceSettings.DynamoDB): Unit = {
    val sourceClient = buildDynamoClient(
      source.endpoint,
      source.credentials.map(_.toAWSCredentialsProvider),
      source.region)
    val sourceStreamsClient =
      buildDynamoStreamsClient(
        source.endpoint,
        source.credentials.map(_.toAWSCredentialsProvider),
        source.region)

    sourceClient
      .updateTable(
        new UpdateTableRequest()
          .withTableName(source.table)
          .withStreamSpecification(
            new StreamSpecification()
              .withStreamEnabled(true)
              .withStreamViewType(StreamViewType.NEW_IMAGE)
          )
      )

    var done = false
    while (!done) {
      val tableDesc = sourceClient.describeTable(source.table)
      val latestStreamArn = tableDesc.getTable.getLatestStreamArn
      val describeStream = sourceStreamsClient.describeStream(
        new DescribeStreamRequest().withStreamArn(latestStreamArn))

      val streamStatus = describeStream.getStreamDescription.getStreamStatus
      if (streamStatus == "ENABLED") {
        log.info("Stream enabled successfully")
        done = true
      } else {
        log.info(
          s"Stream not yet enabled (status ${streamStatus}); waiting for 5 seconds and retrying")
        Thread.sleep(5000)
      }
    }
  }

  def buildDynamoClient(endpoint: Option[DynamoDBEndpoint],
                        creds: Option[AWSCredentialsProvider],
                        region: Option[String]): AmazonDynamoDB = {
    val builder = AmazonDynamoDBClientBuilder.standard()
    AwsUtils
      .configureClientBuilder(builder, endpoint, region, creds)
      .build()
  }

  def buildDynamoStreamsClient(endpoint: Option[DynamoDBEndpoint],
                               creds: Option[AWSCredentialsProvider],
                               region: Option[String]): AmazonDynamoDBStreams = {
    val builder = AmazonDynamoDBStreamsClientBuilder.standard()
    AwsUtils.configureClientBuilder(builder, endpoint, region, creds).build()
  }

  /**
    * Optionally set a configuration. If `maybeValue` is empty, nothing is done. Otherwise,
    * its value is set to the `name` property on the `jobConf`.
    *
    * @param jobConf    Target JobConf to configure
    * @param name       Name of the Hadoop configuration key
    * @param maybeValue Optional value to set.
    */
  def setOptionalConf(jobConf: JobConf, name: String, maybeValue: Option[String]): Unit =
    for (value <- maybeValue) {
      jobConf.set(name, value)
    }

  /**
    * Set the common configuration of both read and write DynamoDB jobs.
    * @param jobConf             Target JobConf to configure
    * @param maybeRegion         AWS region
    * @param maybeEndpoint       AWS endpoint
    * @param maybeScanSegments   Scan segments
    * @param maybeMaxMapTasks    Max map tasks
    * @param maybeAwsCredentials AWS credentials
    */
  def setDynamoDBJobConf(jobConf: JobConf,
                         maybeRegion: Option[String],
                         maybeEndpoint: Option[DynamoDBEndpoint],
                         maybeScanSegments: Option[Int],
                         maybeMaxMapTasks: Option[Int],
                         maybeAwsCredentials: Option[AWSCredentials]): Unit = {
    setOptionalConf(jobConf, DynamoDBConstants.REGION, maybeRegion)
    setOptionalConf(jobConf, DynamoDBConstants.ENDPOINT, maybeEndpoint.map(_.renderEndpoint))
    setOptionalConf(jobConf, DynamoDBConstants.SCAN_SEGMENTS, maybeScanSegments.map(_.toString))
    setOptionalConf(jobConf, DynamoDBConstants.MAX_MAP_TASKS, maybeMaxMapTasks.map(_.toString))
    for (credentials <- maybeAwsCredentials) {
      jobConf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, credentials.accessKey)
      jobConf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, credentials.secretKey)
    }
    jobConf.set(
      DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF,
      "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    jobConf.set(
      "mapred.output.format.class",
      "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
  }

  /**
    * @return The read throughput (in bytes per second) of the
    *         provided table description.
    *         If the table billing mode is PROVISIONED, it returns the
    *         table RCU multiplied by the number of bytes per read
    *         capacity unit. Otherwise (e.g. ,in case of on-demand
    *         billing mode), it returns
    *         [[DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND]].
    */
  def tableReadThroughput(description: TableDescription): Long =
    tableThroughput(
      description,
      DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT.longValue(),
      _.getReadCapacityUnits)

  /**
    * @return The write throughput (in bytes per second) of the
    *         provided table description.
    *         If the table billing mode is PROVISIONED, it returns the
    *         table WCU multiplied by the number of bytes per write
    *         capacity unit. Otherwise (e.g. ,in case of on-demand
    *         billing mode), it returns
    *         [[DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND]].
    */
  def tableWriteThroughput(description: TableDescription): Long =
    tableThroughput(
      description,
      DynamoDBConstants.BYTES_PER_WRITE_CAPACITY_UNIT.longValue(),
      _.getWriteCapacityUnits)

  private def tableThroughput(description: TableDescription,
                              bytesPerCapacityUnit: Long,
                              capacityUnits: ProvisionedThroughputDescription => Long): Long =
    if (description.getBillingModeSummary == null || description.getBillingModeSummary.getBillingMode == BillingMode.PROVISIONED.toString) {
      capacityUnits(description.getProvisionedThroughput) * bytesPerCapacityUnit
    } else {
      DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND * bytesPerCapacityUnit
    }

}
