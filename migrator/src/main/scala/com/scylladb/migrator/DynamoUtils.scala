package com.scylladb.migrator

import com.scylladb.alternator.AlternatorDynamoDbClient
import com.scylladb.alternator.routing.{ ClusterScope, DatacenterScope, RackScope, RoutingScope }
import com.scylladb.alternator.RequestCompressionAlgorithm
import com.scylladb.migrator.config.{
  AlternatorSettings,
  DynamoDBEndpoint,
  SourceSettings,
  TargetSettings
}
import org.apache.hadoop.conf.{ Configurable, Configuration }
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDbClientBuilderTransformer }
import org.apache.hadoop.mapred.JobConf
import org.apache.logging.log4j.LogManager
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
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
  DescribeKinesisStreamingDestinationRequest,
  DescribeKinesisStreamingDestinationResponse,
  DescribeStreamRequest,
  DescribeTableRequest,
  DestinationStatus,
  DynamoDbException,
  EnableKinesisStreamingDestinationRequest,
  GlobalSecondaryIndex,
  KinesisDataStreamDestination,
  LocalSecondaryIndex,
  ProvisionedThroughput,
  ProvisionedThroughputDescription,
  PutItemRequest,
  QueryRequest,
  ResourceInUseException,
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
import java.time.{ Duration => JDuration, Instant }
import java.util.concurrent.TimeoutException
import java.util.stream.Collectors
import scala.util.{ Failure, Success, Try, Using }
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object DynamoUtils {
  val log = LogManager.getLogger("com.scylladb.migrator.DynamoUtils")
  private val RemoveConsumedCapacityConfig = "scylla.migrator.remove_consumed_capacity"
  private val AlternatorDatacenterConfig = "scylla.migrator.alternator.datacenter"
  private val AlternatorRackConfig = "scylla.migrator.alternator.rack"
  private val AlternatorActiveRefreshConfig =
    "scylla.migrator.alternator.active_refresh_interval_ms"
  private val AlternatorIdleRefreshConfig = "scylla.migrator.alternator.idle_refresh_interval_ms"
  private val AlternatorCompressionConfig = "scylla.migrator.alternator.compression"
  private val AlternatorOptimizeHeadersConfig = "scylla.migrator.alternator.optimize_headers"
  private val AlternatorMaxConnectionsConfig = "scylla.migrator.alternator.max_connections"
  private val AlternatorConnectionMaxIdleTimeMsConfig =
    "scylla.migrator.alternator.connection_max_idle_time_ms"
  private val AlternatorConnectionTimeToLiveMsConfig =
    "scylla.migrator.alternator.connection_time_to_live_ms"
  private val AlternatorConnectionAcquisitionTimeoutMsConfig =
    "scylla.migrator.alternator.connection_acquisition_timeout_ms"
  private val AlternatorConnectionTimeoutMsConfig =
    "scylla.migrator.alternator.connection_timeout_ms"
  private val AlternatorMaxItemsPerBatchConfig =
    "scylla.migrator.alternator.max_items_per_batch"
  private val UseAlternatorClientConfig = "scylla.migrator.use_alternator_client"

  /** Write all [[AlternatorSettings]] fields into a Hadoop configuration. */
  private[migrator] def writeAlternatorSettingsToConf(
    jobConf: JobConf,
    settings: AlternatorSettings
  ): Unit = {
    setOptionalConf(jobConf, AlternatorDatacenterConfig, settings.datacenter)
    setOptionalConf(jobConf, AlternatorRackConfig, settings.rack)
    setOptionalConf(
      jobConf,
      AlternatorActiveRefreshConfig,
      settings.activeRefreshIntervalMs.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorIdleRefreshConfig,
      settings.idleRefreshIntervalMs.map(_.toString)
    )
    setOptionalConf(jobConf, AlternatorCompressionConfig, settings.compression.map(_.toString))
    setOptionalConf(
      jobConf,
      AlternatorOptimizeHeadersConfig,
      settings.optimizeHeaders.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorMaxConnectionsConfig,
      settings.maxConnections.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorConnectionMaxIdleTimeMsConfig,
      settings.connectionMaxIdleTimeMs.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorConnectionTimeToLiveMsConfig,
      settings.connectionTimeToLiveMs.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorConnectionAcquisitionTimeoutMsConfig,
      settings.connectionAcquisitionTimeoutMs.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorConnectionTimeoutMsConfig,
      settings.connectionTimeoutMs.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      AlternatorMaxItemsPerBatchConfig,
      settings.maxItemsPerBatch.map(_.toString)
    )
    setOptionalConf(
      jobConf,
      DynamoDBConstants.MAX_ITEMS_PER_BATCH,
      settings.maxItemsPerBatch.map(_.toString)
    )
  }

  /** Read [[AlternatorSettings]] fields from a Hadoop configuration.
    *
    * These settings were already validated at config-parse time, so a parse failure here indicates
    * corruption in the Hadoop config round-trip and should be treated as a fatal error.
    */
  private[migrator] def readAlternatorSettingsFromConf(conf: Configuration): AlternatorSettings = {
    def parseLong(key: String): Option[Long] =
      Option(conf.get(key)).map { s =>
        Try(s.toLong).getOrElse(
          throw new IllegalStateException(
            s"Failed to parse Hadoop config '$key' value '$s' as Long"
          )
        )
      }
    def parseInt(key: String): Option[Int] =
      Option(conf.get(key)).map { s =>
        Try(s.toInt).getOrElse(
          throw new IllegalStateException(
            s"Failed to parse Hadoop config '$key' value '$s' as Int"
          )
        )
      }
    def parseBoolean(key: String): Option[Boolean] =
      Option(conf.get(key)).map { s =>
        Try(s.toBoolean).getOrElse(
          throw new IllegalStateException(
            s"Failed to parse Hadoop config '$key' value '$s' as Boolean"
          )
        )
      }
    AlternatorSettings(
      datacenter                     = Option(conf.get(AlternatorDatacenterConfig)),
      rack                           = Option(conf.get(AlternatorRackConfig)),
      activeRefreshIntervalMs        = parseLong(AlternatorActiveRefreshConfig),
      idleRefreshIntervalMs          = parseLong(AlternatorIdleRefreshConfig),
      compression                    = parseBoolean(AlternatorCompressionConfig),
      optimizeHeaders                = parseBoolean(AlternatorOptimizeHeadersConfig),
      maxConnections                 = parseInt(AlternatorMaxConnectionsConfig),
      connectionMaxIdleTimeMs        = parseLong(AlternatorConnectionMaxIdleTimeMsConfig),
      connectionTimeToLiveMs         = parseLong(AlternatorConnectionTimeToLiveMsConfig),
      connectionAcquisitionTimeoutMs = parseLong(AlternatorConnectionAcquisitionTimeoutMsConfig),
      connectionTimeoutMs            = parseLong(AlternatorConnectionTimeoutMsConfig),
      maxItemsPerBatch               = parseInt(AlternatorMaxItemsPerBatchConfig)
    )
  }

  /** Build the interceptor list for removing `ConsumedCapacity` from DynamoDB responses. */
  def removeConsumedCapacityInterceptors(remove: Boolean): Seq[ExecutionInterceptor] =
    if (remove) Seq(new RemoveConsumedCapacityInterceptor)
    else Nil

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
    target: TargetSettings.DynamoDBLike
  ): TableDescription =
    Using.resource(
      buildDynamoClient(
        target.endpoint,
        target.finalCredentials.map(_.toProvider),
        target.region,
        removeConsumedCapacityInterceptors(target.removeConsumedCapacity),
        target.alternatorSettings
      )
    ) { targetClient =>
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
                  sourceDescription.provisionedThroughput.writeCapacityUnits == 0L) =>
              throw new RuntimeException(
                "readCapacityUnits and writeCapacityUnits must be set for PROVISIONED billing mode"
              )

            case Some(BillingMode.PROVISIONED) | None
                if (sourceDescription.provisionedThroughput.readCapacityUnits != 0L &&
                  sourceDescription.provisionedThroughput.writeCapacityUnits != 0L) =>
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
              waiterResponse.exception.toScala match {
                case Some(ex) =>
                  throw new RuntimeException("Unable to replicate table definition", ex)
                case None =>
                  throw new RuntimeException(
                    "Unable to replicate table definition: waiter returned neither response nor exception"
                  )
              }
          }

        case Failure(otherwise) =>
          throw new RuntimeException("Failed to check for table existence", otherwise)
      }
    }

  def enableDynamoStream(source: SourceSettings.DynamoDB): Unit =
    Using.resource(
      buildDynamoClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region,
        removeConsumedCapacityInterceptors(source.removeConsumedCapacity),
        source.alternatorSettings
      )
    ) { sourceClient =>
      Using.resource(
        buildDynamoStreamsClient(
          source.endpoint,
          source.finalCredentials.map(_.toProvider),
          source.region
        )
      ) { sourceStreamsClient =>
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

        val maxRetries = 60 // 60 * 5s = 5 minutes
        var retries = 0
        var done = false
        while (!done) {
          val tableDesc =
            sourceClient.describeTable(
              DescribeTableRequest.builder().tableName(source.table).build()
            )
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
            retries += 1
            if (retries >= maxRetries)
              throw new RuntimeException(
                s"Timed out waiting for stream on table '${source.table}' to become ENABLED " +
                  s"(last status: ${streamStatus}). Gave up after ${maxRetries} retries (${maxRetries * 5} seconds)."
              )
            log.info(
              s"Stream not yet enabled (status ${streamStatus}); waiting for 5 seconds and retrying (attempt ${retries}/${maxRetries})"
            )
            Thread.sleep(5000)
          }
        }
      }
    }

  /** Enable the Kinesis Data Streams destination for the source DynamoDB table so that its change
    * records start being published to `streamArn`. Idempotent across three paths:
    *
    *   1. The describe-then-enable fast path — if the destination is already `ACTIVE` / `ENABLING`,
    *      skip.
    *   2. The TOCTOU race between describe and enable — if the enable call races another migrator
    *      and returns [[software.amazon.awssdk.services.dynamodb.model.ResourceInUseException]]
    *      ("destination already enabled"), treat that as success.
    *   3. Transient `DynamoDbException`s (throttling, 5xx, socket hiccups) — retry up to 5 times
    *      with exponential backoff + jitter before giving up. Without the retry a single 500 from
    *      DDB would fail the migration at the last step of setup.
    *
    * The user is responsible for having pre-created the Kinesis stream — shard count, retention
    * (24h-1y), and KMS encryption are intentionally not the migrator's business. On a freshly
    * enabled destination the caller must still wait for the `ACTIVE` status via
    * [[waitForKinesisStreamingActive]] before any records will flow;
    * `EnableKinesisStreamingDestination` returns as soon as the request is accepted, not when
    * replication is live.
    */
  def enableKinesisStreamingDestination(
    source: SourceSettings.DynamoDB,
    streamArn: String
  ): Unit = {
    val sourceClient =
      buildDynamoClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region,
        if (source.removeConsumedCapacity)
          Seq(new RemoveConsumedCapacityInterceptor)
        else Nil,
        source.alternatorSettings
      )

    val currentStatus: Option[DestinationStatus] =
      Try(
        sourceClient.describeKinesisStreamingDestination(
          DescribeKinesisStreamingDestinationRequest.builder().tableName(source.table).build()
        )
      ).toOption
        .flatMap(findKinesisDestinationByArn(_, streamArn))
        .map(_.destinationStatus())

    currentStatus match {
      case Some(DestinationStatus.ACTIVE) =>
        log.info(
          s"Kinesis streaming destination '$streamArn' is already ACTIVE on table " +
            s"'${source.table}'; skipping enable"
        )
      case Some(DestinationStatus.ENABLING) =>
        log.info(
          s"Kinesis streaming destination '$streamArn' is already ENABLING on table " +
            s"'${source.table}'; skipping enable"
        )
      case _ =>
        log.info(
          s"Enabling Kinesis streaming destination '$streamArn' for DynamoDB table '${source.table}'"
        )
        callEnableWithRetry(sourceClient, source.table, streamArn)
    }
  }

  /** Call `EnableKinesisStreamingDestination` with a bounded retry loop.
    *
    *   - [[software.amazon.awssdk.services.dynamodb.model.ResourceInUseException]] is treated as
    *     idempotent success (the destination was either already enabled or got enabled between our
    *     describe and our enable — either way the end-state is what we want).
    *   - Any other [[DynamoDbException]] is retried with exponential backoff + jitter up to
    *     [[EnableMaxTransientAttempts]] attempts.
    *   - Non-DynamoDbException errors propagate immediately because they are almost certainly
    *     configuration bugs (misconfigured endpoint, bad credentials) that retrying cannot fix.
    */
  private val EnableMaxTransientAttempts: Int = 5

  private def callEnableWithRetry(
    client: DynamoDbClient,
    tableName: String,
    streamArn: String
  ): Unit = {
    val rng = new scala.util.Random()
    var attempt = 0
    while (true) {
      attempt += 1
      try {
        client.enableKinesisStreamingDestination(
          EnableKinesisStreamingDestinationRequest
            .builder()
            .tableName(tableName)
            .streamArn(streamArn)
            .build()
        )
        return
      } catch {
        case _: ResourceInUseException =>
          log.info(
            s"Kinesis streaming destination '$streamArn' was already enabled on table " +
              s"'$tableName' (race with another migrator); treating as success"
          )
          return
        case e: DynamoDbException if attempt < EnableMaxTransientAttempts =>
          val backoffMs = (500L * (1L << Math.min(attempt, 6))) + rng.nextInt(250)
          log.warn(
            s"EnableKinesisStreamingDestination transient error (attempt " +
              s"$attempt/$EnableMaxTransientAttempts): ${e.getClass.getSimpleName}; " +
              s"retrying in ${backoffMs}ms",
            e
          )
          Thread.sleep(backoffMs)
      }
    }
  }

  /** Look up the Kinesis streaming destination on a `DescribeKinesisStreamingDestination` response
    * whose ARN matches `streamArn`. Used by [[enableKinesisStreamingDestination]] and
    * [[waitForKinesisStreamingActive]] — prior to this helper the two sites inlined the same lookup
    * independently and risked drifting apart.
    *
    * Both sides are `.trim`-ed before comparison as defense in depth. The user-supplied `streamArn`
    * has already been validated and canonicalised at config load by
    * `StreamChangesSetting.validateArn` (trimmed, strict regex, lowercase partition + region,
    * 12-digit account), so the trim on the AWS-returned side is purely to guard against a future
    * AWS API quirk returning extraneous whitespace. Stream names are intentionally case-sensitive
    * per the Kinesis API contract — `MyStream` and `mystream` are genuinely different streams — so
    * we do NOT `toLowerCase` any part of the ARN. (Finding LOGIC-3.)
    */
  private def findKinesisDestinationByArn(
    resp: DescribeKinesisStreamingDestinationResponse,
    streamArn: String
  ): Option[KinesisDataStreamDestination] = {
    val target = streamArn.trim
    resp.kinesisDataStreamDestinations().asScala.find(_.streamArn().trim == target)
  }

  /** Block until the Kinesis Data Streams destination for the given `(source.table, streamArn)`
    * pair reaches `ACTIVE`.
    *
    *   - Throws `RuntimeException` immediately if the destination transitions to `ENABLE_FAILED`
    *     (terminal error — the user must investigate AWS-side).
    *   - Throws `RuntimeException` immediately if the destination is not found on the table
    *     (indicates the ARN is wrong or points to a different table's destination).
    *   - Throws `TimeoutException` if the destination does not reach `ACTIVE` within `maxWait`
    *     (default 15 minutes). This bounds pathological AWS outages so the migrator does not block
    *     forever.
    *   - Applies exponential backoff with jitter on transient `DynamoDbException`s (capped at 5
    *     attempts) before letting the exception propagate. Steady-state polling uses a jittered 5s
    *     interval so concurrent migrators do not thundering-herd AWS.
    *
    * @param maxWait
    *   Overall wall-clock deadline for the polling loop. Exposed so integration tests can set a
    *   small value; production callers should accept the default.
    */
  def waitForKinesisStreamingActive(
    source: SourceSettings.DynamoDB,
    streamArn: String,
    maxWait: JDuration = JDuration.ofMinutes(15)
  ): Unit = {
    val sourceClient =
      buildDynamoClient(
        source.endpoint,
        source.finalCredentials.map(_.toProvider),
        source.region,
        if (source.removeConsumedCapacity)
          Seq(new RemoveConsumedCapacityInterceptor)
        else Nil,
        source.alternatorSettings
      )

    val deadline = Instant.now().plus(maxWait)
    val rng = new scala.util.Random()
    var transientAttempts = 0
    val maxTransient = 5

    while (true) {
      val now = Instant.now()
      if (now.isAfter(deadline)) {
        throw new TimeoutException(
          s"Kinesis streaming destination for '${source.table}' -> '$streamArn' did not " +
            s"reach ACTIVE within $maxWait"
        )
      }

      val describeResult =
        Try(
          sourceClient.describeKinesisStreamingDestination(
            DescribeKinesisStreamingDestinationRequest
              .builder()
              .tableName(source.table)
              .build()
          )
        )

      describeResult match {
        case Success(resp) =>
          val destination = findKinesisDestinationByArn(resp, streamArn)
          destination match {
            case Some(d) if d.destinationStatus() == DestinationStatus.ACTIVE =>
              log.info(s"Kinesis streaming destination '$streamArn' is ACTIVE")
              return
            case Some(d) if d.destinationStatus() == DestinationStatus.ENABLE_FAILED =>
              throw new RuntimeException(
                s"Kinesis streaming destination enable failed for '$streamArn': " +
                  s"${Option(d.destinationStatusDescription()).getOrElse("<no description>")}"
              )
            case Some(d) =>
              val sleepMs = 5000L + rng.nextInt(1000)
              log.info(
                s"Kinesis streaming destination '$streamArn' status = ${d.destinationStatus()}; " +
                  s"waiting ${sleepMs}ms (deadline: $deadline)"
              )
              Thread.sleep(sleepMs)
              transientAttempts = 0
            case None =>
              // LOGIC-3: surface the ARNs AWS did return so operators can eyeball a typo
              // (case of the stream name, wrong account, wrong partition, wrong region). The
              // ARN itself is non-PII infrastructure identifier; safe to log.
              val registered = resp.kinesisDataStreamDestinations().asScala.toList
              val detail =
                if (registered.isEmpty)
                  "the table currently has no Kinesis destinations registered"
                else {
                  val summary = registered
                    .map(d => s"'${d.streamArn()}' (${d.destinationStatus()})")
                    .mkString(", ")
                  s"the table currently has: $summary"
                }
              throw new RuntimeException(
                s"Kinesis streaming destination for '${source.table}' -> '$streamArn' " +
                  s"was not found in DescribeKinesisStreamingDestination ($detail). " +
                  "Matching is strict and case-sensitive on the stream-name portion of the ARN; " +
                  "check for typos and confirm the destination is registered on the same AWS " +
                  "account and region as the source."
              )
          }
        case Failure(e: DynamoDbException) if transientAttempts < maxTransient =>
          val backoffMs = (500L * (1L << transientAttempts.min(6))) + rng.nextInt(250)
          log.warn(
            s"DescribeKinesisStreamingDestination transient error (attempt " +
              s"${transientAttempts + 1}/$maxTransient): ${e.getClass.getSimpleName}; " +
              s"retrying in ${backoffMs}ms",
            e
          )
          transientAttempts += 1
          Thread.sleep(backoffMs)
        case Failure(e) =>
          throw e
      }
    }
  }

  def buildDynamoClient(
    endpoint: Option[DynamoDBEndpoint],
    creds: Option[AwsCredentialsProvider],
    region: Option[String],
    interceptors: Seq[ExecutionInterceptor],
    alternatorSettings: Option[AlternatorSettings] = None
  ): DynamoDbClient = {
    val baseBuilder: DynamoDbClientBuilder =
      alternatorSettings match {
        case Some(settings) =>
          val altBuilder = AlternatorDynamoDbClient.builder()
          applyAlternatorSettings(altBuilder, settings)
          altBuilder
        case None => DynamoDbClient.builder()
      }
    val builder =
      AwsUtils.configureClientBuilder(baseBuilder, endpoint, region, creds)
    val conf = ClientOverrideConfiguration.builder()
    interceptors.foreach(conf.addExecutionInterceptor)
    builder.overrideConfiguration(conf.build()).build()
  }

  private def applyAlternatorSettings(
    altBuilder: AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder,
    settings: AlternatorSettings
  ): Unit = {
    val routingScope: Option[RoutingScope] =
      (settings.datacenter, settings.rack) match {
        case (Some(dc), Some(rack)) =>
          Some(RackScope.of(dc, rack, DatacenterScope.of(dc, ClusterScope.create())))
        case (Some(dc), None) =>
          Some(DatacenterScope.of(dc, ClusterScope.create()))
        case _ => None
      }
    for (scope <- routingScope)
      altBuilder.withRoutingScope(scope)
    for (interval <- settings.activeRefreshIntervalMs)
      altBuilder.withActiveRefreshIntervalMs(interval)
    for (interval <- settings.idleRefreshIntervalMs)
      altBuilder.withIdleRefreshIntervalMs(interval)
    settings.compression.foreach { enabled =>
      if (enabled) altBuilder.withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    }
    settings.optimizeHeaders.foreach(altBuilder.withOptimizeHeaders)
    for (maxConns <- settings.maxConnections)
      altBuilder.withMaxConnections(maxConns)
    for (idleTime <- settings.connectionMaxIdleTimeMs)
      altBuilder.withConnectionMaxIdleTimeMs(idleTime)
    for (ttl <- settings.connectionTimeToLiveMs)
      altBuilder.withConnectionTimeToLiveMs(ttl)
    for (timeout <- settings.connectionAcquisitionTimeoutMs)
      altBuilder.withConnectionAcquisitionTimeoutMs(timeout)
    for (timeout <- settings.connectionTimeoutMs)
      altBuilder.withConnectionTimeoutMs(timeout)
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
    removeConsumedCapacity: Boolean = false,
    alternatorSettings: Option[AlternatorSettings] = None
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

    if (alternatorSettings.isDefined)
      jobConf.set(
        DynamoDBConstants.CUSTOM_CLIENT_BUILDER_TRANSFORMER,
        classOf[AlternatorLoadBalancingEnabler].getName
      )

    jobConf.set(RemoveConsumedCapacityConfig, removeConsumedCapacity.toString)
    jobConf.set(UseAlternatorClientConfig, alternatorSettings.isDefined.toString)

    jobConf.set("mapred.output.format.class", classOf[DynamoDBOutputFormat].getName)
    jobConf.set("mapred.input.format.class", classOf[DynamoDBInputFormat].getName)

    for (settings <- alternatorSettings)
      writeAlternatorSettingsToConf(jobConf, settings)
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
      val useAlternator = conf.get(UseAlternatorClientConfig, "false").toBoolean
      val effectiveBuilder: DynamoDbClientBuilder =
        if (useAlternator) {
          val altBuilder = AlternatorDynamoDbClient.builder()
          for (customEndpoint <- Option(conf.get(DynamoDBConstants.ENDPOINT)))
            altBuilder.endpointOverride(URI.create(customEndpoint))
          for (region <- Option(conf.get(DynamoDBConstants.REGION)))
            altBuilder.region(Region.of(region))
          (
            Option(conf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF)),
            Option(conf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF))
          ) match {
            case (Some(accessKey), Some(secretKey)) =>
              val awsCreds =
                Option(conf.get(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF)) match {
                  case Some(token) =>
                    AwsSessionCredentials.create(accessKey, secretKey, token)
                  case None =>
                    AwsBasicCredentials.create(accessKey, secretKey)
                }
              altBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            case (Some(_), None) | (None, Some(_)) =>
              throw new IllegalStateException(
                "Incomplete DynamoDB credentials in Hadoop config: both accessKey and secretKey must be set, or neither."
              )
            case _ => // No credentials configured - use anonymous (Alternator default)
          }
          val settings = readAlternatorSettingsFromConf(conf)
          val validationErrors = AlternatorSettings.validate(settings)
          if (validationErrors.nonEmpty)
            throw new IllegalStateException(
              s"AlternatorSettings validation failed after Hadoop config round-trip: ${validationErrors.mkString("; ")}"
            )
          applyAlternatorSettings(altBuilder, settings)
          altBuilder
        } else builder
      val overrideConf = ClientOverrideConfiguration.builder()
      if (conf.get(RemoveConsumedCapacityConfig, "false").toBoolean)
        overrideConf.addExecutionInterceptor(new RemoveConsumedCapacityInterceptor)
      effectiveBuilder.overrideConfiguration(overrideConf.build())
    }

    override def setConf(configuration: Configuration): Unit =
      conf = configuration
    override def getConf: Configuration = conf
  }

}
