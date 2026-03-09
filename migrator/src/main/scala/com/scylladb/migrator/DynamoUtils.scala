package com.scylladb.migrator

import com.scylladb.alternator.AlternatorEndpointProvider
import com.scylladb.migrator.config.{ DynamoDBEndpoint, SourceSettings, TargetSettings }
import org.apache.hadoop.conf.{ Configurable, Configuration }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDbClientBuilderTransformer }
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.dynamodb.{ DynamoDbClient, DynamoDbClientBuilder }
import software.amazon.awssdk.core.SdkRequest
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.{
  Context,
  ExecutionAttributes,
  ExecutionInterceptor
}
import software.amazon.awssdk.services.dynamodb.model.{
  BatchWriteItemRequest,
  BillingMode,
  CreateTableRequest,
  DeleteItemRequest,
  DescribeStreamRequest,
  DescribeTableRequest,
  GlobalSecondaryIndex,
  LocalSecondaryIndex,
  ProvisionedThroughput,
  ProvisionedThroughputDescription,
  PutItemRequest,
  QueryRequest,
  ResourceNotFoundException,
  ReturnConsumedCapacity,
  ScanRequest,
  StreamSpecification,
  StreamStatus,
  StreamViewType,
  TableDescription,
  UpdateItemRequest,
  UpdateTableRequest
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.net.URI
import java.util.stream.Collectors
import scala.util.{ Failure, Success, Try }
import scala.jdk.OptionConverters._

object DynamoUtils {
  val log = LogManager.getLogger("com.scylladb.migrator.DynamoUtils")
  private val RemoveConsumedCapacityConfig = "scylla.migrator.remove_consumed_capacity"

  class RemoveConsumedCapacityInterceptor extends ExecutionInterceptor {
    override def modifyRequest(ctx: Context.ModifyRequest, attrs: ExecutionAttributes): SdkRequest =
      ctx.request() match {
        case r: BatchWriteItemRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case r: PutItemRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case r: DeleteItemRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case r: UpdateItemRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case r: ScanRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case r: QueryRequest =>
          r.toBuilder.returnConsumedCapacity(null: ReturnConsumedCapacity).build()
        case other => other
      }
  }

  def replicateTableDefinition(
    sourceDescription: TableDescription,
    target: TargetSettings.DynamoDB
  ): TableDescription = {
    // If non-existent, replicate
    val targetClient =
      buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        if (target.removeConsumedCapacity.getOrElse(false))
          Seq(new RemoveConsumedCapacityInterceptor)
        else Nil
      )

    log.info("Checking for table existence at destination")
    val describeTargetTableRequest =
      DescribeTableRequest.builder().tableName(target.table).build()
    val targetDescription = Try(targetClient.describeTable(describeTargetTableRequest))
    targetDescription match {
      case Success(desc) =>
        log.info(s"Table ${target.table} exists at destination")
        desc.table()

      case Failure(e: ResourceNotFoundException) =>
        val requestBuilder = CreateTableRequest
          .builder()
          .tableName(target.table)
          .keySchema(sourceDescription.keySchema)
          .attributeDefinitions(sourceDescription.attributeDefinitions)

        target.billingMode match {
          case Some(BillingMode.PROVISIONED)
              if (sourceDescription.provisionedThroughput.readCapacityUnits == 0L ||
                sourceDescription.provisionedThroughput.writeCapacityUnits == 0) =>
            throw new RuntimeException(
              "readCapacityUnits and writeCapacityUnits must be set for PROVISIONED billing mode"
            )

          case Some(BillingMode.PROVISIONED) | None
              if (sourceDescription.provisionedThroughput.readCapacityUnits != 0L &&
                sourceDescription.provisionedThroughput.writeCapacityUnits != 0) =>
            log.info(
              "BillingMode PROVISIONED will be used since writeCapacityUnits and readCapacityUnits are set"
            )
            requestBuilder.billingMode(BillingMode.PROVISIONED)
            requestBuilder.provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(sourceDescription.provisionedThroughput.readCapacityUnits)
                .writeCapacityUnits(sourceDescription.provisionedThroughput.writeCapacityUnits)
                .build()
            )

          // billing mode = PAY_PER_REQUEST or empty ( for backward compatibility )
          case _ => requestBuilder.billingMode(BillingMode.PAY_PER_REQUEST)
        }
        if (sourceDescription.hasLocalSecondaryIndexes) {
          requestBuilder.localSecondaryIndexes(
            sourceDescription.localSecondaryIndexes.stream
              .map(index =>
                LocalSecondaryIndex
                  .builder()
                  .indexName(index.indexName())
                  .keySchema(index.keySchema())
                  .projection(index.projection())
                  .build()
              )
              .collect(Collectors.toList[LocalSecondaryIndex])
          )
        }
        if (sourceDescription.hasGlobalSecondaryIndexes) {
          requestBuilder.globalSecondaryIndexes(
            sourceDescription.globalSecondaryIndexes.stream
              .map { index =>
                val builder =
                  GlobalSecondaryIndex
                    .builder()
                    .indexName(index.indexName())
                    .keySchema(index.keySchema())
                    .projection(index.projection())
                if (target.billingMode.forall(_ == BillingMode.PROVISIONED))
                  builder.provisionedThroughput(
                    ProvisionedThroughput
                      .builder()
                      .readCapacityUnits(index.provisionedThroughput.readCapacityUnits)
                      .writeCapacityUnits(index.provisionedThroughput.writeCapacityUnits)
                      .build()
                  )
                builder.build()
              }
              .collect(Collectors.toList[GlobalSecondaryIndex])
          )
        }

        log.info(
          s"Table ${target.table} does not exist at destination - creating it according to definition:"
        )
        log.info(sourceDescription.toString)
        targetClient.createTable(requestBuilder.build())
        log.info(s"Table ${target.table} created.")

        val waiterResponse =
          targetClient.waiter().waitUntilTableExists(describeTargetTableRequest).matched
        waiterResponse.response.toScala match {
          case Some(describeTableResponse) => describeTableResponse.table
          case None =>
            throw new RuntimeException(
              "Unable to replicate table definition",
              waiterResponse.exception.get
            )
        }

      case Failure(otherwise) =>
        throw new RuntimeException("Failed to check for table existence", otherwise)
    }
  }

  def enableDynamoStream(source: SourceSettings.DynamoDB): Unit = {
    val sourceClient =
      buildDynamoClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region,
        if (source.removeConsumedCapacity.getOrElse(false))
          Seq(new RemoveConsumedCapacityInterceptor)
        else Nil
      )
    val sourceStreamsClient =
      buildDynamoStreamsClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region
      )

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
          DescribeStreamRequest.builder().streamArn(latestStreamArn).build()
        )

      val streamStatus = describeStream.streamDescription.streamStatus
      if (streamStatus == StreamStatus.ENABLED) {
        log.info("Stream enabled successfully")
        done = true
      } else {
        log.info(
          s"Stream not yet enabled (status ${streamStatus}); waiting for 5 seconds and retrying"
        )
        Thread.sleep(5000)
      }
    }
  }

  def buildDynamoClient(
    endpoint: Option[DynamoDBEndpoint],
    creds: Option[AwsCredentialsProvider],
    region: Option[String],
    interceptors: Seq[ExecutionInterceptor]
  ): DynamoDbClient = {
    val builder =
      AwsUtils.configureClientBuilder(DynamoDbClient.builder(), endpoint, region, creds)
    val conf = ClientOverrideConfiguration.builder()
    interceptors.foreach(conf.addExecutionInterceptor)
    builder.overrideConfiguration(conf.build()).build()
  }

  def buildDynamoStreamsClient(
    endpoint: Option[DynamoDBEndpoint],
    creds: Option[AwsCredentialsProvider],
    region: Option[String]
  ): DynamoDbStreamsClient =
    AwsUtils
      .configureClientBuilder(DynamoDbStreamsClient.builder(), endpoint, region, creds)
      .build()

  /** Optionally set a configuration. If `maybeValue` is empty, nothing is done. Otherwise, its
    * value is set to the `name` property on the `jobConf`.
    *
    * @param jobConf
    *   Target JobConf to configure
    * @param name
    *   Name of the Hadoop configuration key
    * @param maybeValue
    *   Optional value to set.
    */
  def setOptionalConf(jobConf: JobConf, name: String, maybeValue: Option[String]): Unit =
    for (value <- maybeValue)
      jobConf.set(name, value)

  /** Set the common configuration of both read and write DynamoDB jobs.
    * @param jobConf
    *   Target JobConf to configure
    * @param maybeRegion
    *   AWS region
    * @param maybeEndpoint
    *   AWS endpoint
    * @param maybeScanSegments
    *   Scan segments
    * @param maybeMaxMapTasks
    *   Max map tasks
    * @param maybeAwsCredentials
    *   AWS credentials
    */
  def setDynamoDBJobConf(
    jobConf: JobConf,
    maybeRegion: Option[String],
    maybeEndpoint: Option[DynamoDBEndpoint],
    maybeScanSegments: Option[Int],
    maybeMaxMapTasks: Option[Int],
    maybeAwsCredentials: Option[AWSCredentials],
    removeConsumedCapacity: Boolean = false
  ): Unit = {
    for (region <- maybeRegion) {
      log.info(s"Using AWS region: ${region}")
      jobConf.set(DynamoDBConstants.REGION, region)
    }
    for (endpoint <- maybeEndpoint) {
      log.info(s"Using AWS endpoint: ${endpoint.renderEndpoint}")
      jobConf.set(DynamoDBConstants.ENDPOINT, endpoint.renderEndpoint)
    }
    setOptionalConf(jobConf, DynamoDBConstants.SCAN_SEGMENTS, maybeScanSegments.map(_.toString))
    setOptionalConf(jobConf, DynamoDBConstants.MAX_MAP_TASKS, maybeMaxMapTasks.map(_.toString))
    for (credentials <- maybeAwsCredentials) {
      jobConf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, credentials.accessKey)
      jobConf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, credentials.secretKey)
      for (sessionToken <- credentials.maybeSessionToken)
        jobConf.set(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF, sessionToken)
    }

    // The default way to compute the available resources (memory/cpu per worker) requires a
    // YARN environment. We disable it to be agnostic to the type of cluster.
    jobConf.set(DynamoDBConstants.YARN_RESOURCE_MANAGER_ENABLED, "false")

    jobConf.set(
      DynamoDBConstants.CUSTOM_CLIENT_BUILDER_TRANSFORMER,
      classOf[AlternatorLoadBalancingEnabler].getName
    )

    jobConf.set(RemoveConsumedCapacityConfig, removeConsumedCapacity.toString)

    jobConf.set("mapred.output.format.class", classOf[DynamoDBOutputFormat].getName)
    jobConf.set("mapred.input.format.class", classOf[DynamoDBInputFormat].getName)
  }

  /** @return
    *   The read throughput (in RCU) of the provided table description. If the table billing mode is
    *   PROVISIONED, it returns the table RCU. Otherwise (e.g., in case of on-demand billing mode),
    *   it returns [[DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND]].
    */
  def tableReadThroughput(description: TableDescription): Long =
    tableThroughput(description, _.readCapacityUnits)

  /** @return
    *   The write throughput (in WCU) of the provided table description. If the table billing mode
    *   is PROVISIONED, it returns the table WCU. Otherwise (e.g., in case of on-demand billing
    *   mode), it returns [[DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND]].
    */
  def tableWriteThroughput(description: TableDescription): Long =
    tableThroughput(description, _.writeCapacityUnits)

  private def tableThroughput(
    description: TableDescription,
    capacityUnits: ProvisionedThroughputDescription => Long
  ): Long =
    if (
      description.billingModeSummary == null || description.billingModeSummary.billingMode == BillingMode.PROVISIONED
    ) {
      capacityUnits(description.provisionedThroughput)
    } else {
      DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND
    }

  class AlternatorLoadBalancingEnabler extends DynamoDbClientBuilderTransformer with Configurable {
    private var conf: Configuration = null

    override def apply(builder: DynamoDbClientBuilder): DynamoDbClientBuilder = {
      for (customEndpoint <- Option(conf.get(DynamoDBConstants.ENDPOINT)))
        builder.endpointProvider(
          new AlternatorEndpointProvider(URI.create(customEndpoint))
        )
      val overrideConf = ClientOverrideConfiguration.builder()
      if (conf.get(RemoveConsumedCapacityConfig, "false").toBoolean)
        overrideConf.addExecutionInterceptor(new RemoveConsumedCapacityInterceptor)
      builder.overrideConfiguration(overrideConf.build())
    }

    override def setConf(configuration: Configuration): Unit =
      conf = configuration
    override def getConf: Configuration = conf
  }

}
