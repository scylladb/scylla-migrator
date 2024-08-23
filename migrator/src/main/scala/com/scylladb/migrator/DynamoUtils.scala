package com.scylladb.migrator

import com.scylladb.alternator.AlternatorEndpointProvider
import com.scylladb.migrator.config.{ DynamoDBEndpoint, SourceSettings, TargetSettings }
import org.apache.hadoop.conf.{ Configurable, Configuration }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDbClientBuilderTransformer }
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import software.amazon.awssdk.auth.credentials.{
  AwsCredentials,
  AwsCredentialsProvider,
  ProfileCredentialsProvider
}
import software.amazon.awssdk.services.dynamodb.{ DynamoDbClient, DynamoDbClientBuilder }
import software.amazon.awssdk.services.dynamodb.model.{
  BillingMode,
  CreateTableRequest,
  DescribeStreamRequest,
  DescribeTableRequest,
  GlobalSecondaryIndex,
  LocalSecondaryIndex,
  ProvisionedThroughput,
  ProvisionedThroughputDescription,
  ResourceNotFoundException,
  StreamSpecification,
  StreamStatus,
  StreamViewType,
  TableDescription,
  UpdateTableRequest
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util.stream.Collectors
import java.net.URI
import scala.util.{ Failure, Success, Try }
import scala.jdk.OptionConverters._

object DynamoUtils {
  val log = LogManager.getLogger("com.scylladb.migrator.DynamoUtils")

  def replicateTableDefinition(sourceDescription: TableDescription,
                               target: TargetSettings.DynamoDB): TableDescription = {
    // If non-existent, replicate
    val targetClient =
      buildDynamoClient(target.endpoint, target.finalCredentials.map(_.toProvider), target.region)

    log.info("Checking for table existence at destination")
    val describeTargetTableRequest =
      DescribeTableRequest.builder().tableName(target.table).build()
    val targetDescription = Try(targetClient.describeTable(describeTargetTableRequest))
    targetDescription match {
      case Success(desc) =>
        log.info(s"Table ${target.table} exists at destination")
        desc.table()

      case Failure(e: ResourceNotFoundException) =>
        val request = CreateTableRequest
          .builder()
          .tableName(target.table)
          .keySchema(sourceDescription.keySchema)
          .attributeDefinitions(sourceDescription.attributeDefinitions)
        if (sourceDescription.provisionedThroughput.readCapacityUnits != 0L && sourceDescription.provisionedThroughput.writeCapacityUnits != 0) {
          request
            .provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(sourceDescription.provisionedThroughput.readCapacityUnits)
                .writeCapacityUnits(sourceDescription.provisionedThroughput.writeCapacityUnits)
                .build()
            )
        } else {
          request.billingMode(BillingMode.PAY_PER_REQUEST)
        }
        if (sourceDescription.hasLocalSecondaryIndexes) {
          request.localSecondaryIndexes(
            sourceDescription.localSecondaryIndexes.stream
              .map(
                index =>
                  LocalSecondaryIndex
                    .builder()
                    .indexName(index.indexName())
                    .keySchema(index.keySchema())
                    .projection(index.projection())
                    .build())
              .collect(Collectors.toList[LocalSecondaryIndex])
          )
        }
        if (sourceDescription.hasGlobalSecondaryIndexes) {
          request.globalSecondaryIndexes(
            sourceDescription.globalSecondaryIndexes.stream
              .map(
                index =>
                  GlobalSecondaryIndex
                    .builder()
                    .indexName(index.indexName())
                    .keySchema(index.keySchema())
                    .projection(index.projection())
                    .provisionedThroughput(
                      ProvisionedThroughput
                        .builder()
                        .readCapacityUnits(index.provisionedThroughput.readCapacityUnits)
                        .writeCapacityUnits(index.provisionedThroughput.writeCapacityUnits)
                        .build()
                    )
                    .build())
              .collect(Collectors.toList[GlobalSecondaryIndex])
          )
        }

        log.info(
          s"Table ${target.table} does not exist at destination - creating it according to definition:")
        log.info(sourceDescription.toString)
        targetClient.createTable(request.build())
        log.info(s"Table ${target.table} created.")

        val waiterResponse =
          targetClient.waiter().waitUntilTableExists(describeTargetTableRequest).matched
        waiterResponse.response.toScala match {
          case Some(describeTableResponse) => describeTableResponse.table
          case None =>
            throw new RuntimeException(
              "Unable to replicate table definition",
              waiterResponse.exception.get)
        }

      case Failure(otherwise) =>
        throw new RuntimeException("Failed to check for table existence", otherwise)
    }
  }

  def enableDynamoStream(source: SourceSettings.DynamoDB): Unit = {
    val sourceClient =
      buildDynamoClient(source.endpoint, source.finalCredentials.map(_.toProvider), source.region)
    val sourceStreamsClient =
      buildDynamoStreamsClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region)

    sourceClient
      .updateTable(
        UpdateTableRequest
          .builder()
          .tableName(source.table)
          .streamSpecification(
            StreamSpecification
              .builder()
              .streamEnabled(true)
              .streamViewType(StreamViewType.NEW_IMAGE)
              .build()
          )
          .build()
      )

    var done = false
    while (!done) {
      val tableDesc =
        sourceClient.describeTable(DescribeTableRequest.builder().tableName(source.table).build())
      val latestStreamArn = tableDesc.table.latestStreamArn
      val describeStream =
        sourceStreamsClient.describeStream(
          DescribeStreamRequest.builder().streamArn(latestStreamArn).build())

      val streamStatus = describeStream.streamDescription.streamStatus
      if (streamStatus == StreamStatus.ENABLED) {
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
                        creds: Option[AwsCredentialsProvider],
                        region: Option[String]): DynamoDbClient =
    AwsUtils
      .configureClientBuilder(DynamoDbClient.builder(), endpoint, region, creds)
      .build()

  def buildDynamoStreamsClient(endpoint: Option[DynamoDBEndpoint],
                               creds: Option[AwsCredentialsProvider],
                               region: Option[String]): DynamoDbStreamsClient =
    AwsUtils
      .configureClientBuilder(DynamoDbStreamsClient.builder(), endpoint, region, creds)
      .build()

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
      for (sessionToken <- credentials.maybeSessionToken) {
        jobConf.set(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF, sessionToken)
      }
    }

    // The default way to compute the available resources (memory/cpu per worker) requires a
    // YARN environment. We disable it to be agnostic to the type of cluster.
    jobConf.set(DynamoDBConstants.YARN_RESOURCE_MANAGER_ENABLED, "false")

    jobConf.set(
      DynamoDBConstants.CUSTOM_CLIENT_BUILDER_TRANSFORMER,
      classOf[AlternatorLoadBalancingEnabler].getName)

    jobConf.set(
      DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF,
      classOf[ProfileCredentialsProvider].getName)
    jobConf.set("mapred.output.format.class", classOf[DynamoDBOutputFormat].getName)
    jobConf.set("mapred.input.format.class", classOf[DynamoDBInputFormat].getName)
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
      _.readCapacityUnits)

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
      _.writeCapacityUnits)

  private def tableThroughput(description: TableDescription,
                              bytesPerCapacityUnit: Long,
                              capacityUnits: ProvisionedThroughputDescription => Long): Long =
    if (description.billingModeSummary == null || description.billingModeSummary.billingMode == BillingMode.PROVISIONED) {
      capacityUnits(description.provisionedThroughput) * bytesPerCapacityUnit
    } else {
      DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND * bytesPerCapacityUnit
    }

  /** Reflection-friendly credentials provider used by the EMR DynamoDB connector */
  class ProfileCredentialsProvider
      extends software.amazon.awssdk.auth.credentials.AwsCredentialsProvider {
    private lazy val delegate = ProfileCredentialsProvider.create()
    def resolveCredentials(): AwsCredentials = delegate.resolveCredentials()
  }

  class AlternatorLoadBalancingEnabler extends DynamoDbClientBuilderTransformer with Configurable {
    private var conf: Configuration = null

    override def apply(builder: DynamoDbClientBuilder): DynamoDbClientBuilder = {
      for (customEndpoint <- Option(conf.get(DynamoDBConstants.ENDPOINT))) {
        builder.endpointProvider(
          new AlternatorEndpointProvider(URI.create(customEndpoint))
        )
      }
      builder
    }

    override def setConf(configuration: Configuration): Unit =
      conf = configuration
    override def getConf: Configuration = conf
  }

}
