package com.scylladb.migrator.writers

import org.apache.log4j.LogManager
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.{
  Dimension,
  MetricDatum,
  PutMetricDataRequest,
  StandardUnit
}

import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

/** Metrics for DynamoDB stream replication, optionally published to CloudWatch.
  *
  * Counters are always maintained as AtomicLong fields for thread-safe access and periodic logging.
  * CloudWatch publishing is gated by the `enableCloudWatch` flag and requires valid AWS credentials
  * in the source region.
  */
class StreamMetrics(
  tableName: String,
  region: Option[String],
  enableCloudWatch: Boolean,
  credentialsProvider: Option[software.amazon.awssdk.auth.credentials.AwsCredentialsProvider] =
    None,
  cloudWatchNamespace: String = "ScyllaMigrator/StreamReplication"
) extends AutoCloseable {

  private val log = LogManager.getLogger("com.scylladb.migrator.writers.StreamMetrics")

  @volatile private var closed = false

  val recordsProcessed = new AtomicLong(0L)
  val pollCycles = new AtomicLong(0L)
  val activeShards = new AtomicLong(0L)
  val maxIteratorAgeMs = new AtomicLong(0L)
  val lastPollDurationMs = new AtomicLong(0L)
  val writeFailures = new AtomicLong(0L)
  val checkpointFailures = new AtomicLong(0L)
  val deadLetterItems = new AtomicLong(0L)

  // --- CloudWatch (optional, explicitly initialized on first use) ---

  private var cloudWatchClient: Option[CloudWatchClient] = None
  private var cloudWatchInitialized = false

  private def getOrInitCloudWatchClient(): Option[CloudWatchClient] = {
    if (!cloudWatchInitialized) {
      cloudWatchInitialized = true
      if (enableCloudWatch) {
        cloudWatchClient =
          try {
            val builder = CloudWatchClient.builder()
            region.foreach(r => builder.region(software.amazon.awssdk.regions.Region.of(r)))
            credentialsProvider.foreach(builder.credentialsProvider)
            val client = builder.build()
            log.info("CloudWatch metrics publishing enabled")
            Some(client)
          } catch {
            case e: Exception =>
              log.warn("Failed to create CloudWatch client, metrics will not be published", e)
              None
          }
      }
    }
    cloudWatchClient
  }

  private val namespace = cloudWatchNamespace

  private val tableDimension =
    Dimension.builder().name("TableName").value(tableName).build()

  // Track last published values to compute deltas for CloudWatch.
  // These vars are only accessed from publishToCloudWatch(), which is called
  // exclusively from the single scheduler thread (dynamo-stream-poller).
  private var lastPublishedRecordsProcessed = 0L
  private var lastPublishedWriteFailures = 0L
  private var lastPublishedCheckpointFailures = 0L
  private var lastPublishedDeadLetterItems = 0L

  /** Lock object guarding the CloudWatch client lifecycle. Ensures `publishToCloudWatch()` and
    * `close()` do not race (TOCTOU on the `closed` flag vs client usage).
    */
  private val cloudWatchLock = new AnyRef

  /** Publish current metric values to CloudWatch. Call periodically (e.g., every 60 cycles). */
  def publishToCloudWatch(): Unit = cloudWatchLock.synchronized {
    if (closed) return
    getOrInitCloudWatchClient().foreach { client =>
      try {
        val currentRecordsProcessed = recordsProcessed.get()
        val recordsDelta = currentRecordsProcessed - lastPublishedRecordsProcessed
        lastPublishedRecordsProcessed = currentRecordsProcessed

        val currentWriteFailures = writeFailures.get()
        val writeFailuresDelta = currentWriteFailures - lastPublishedWriteFailures
        lastPublishedWriteFailures = currentWriteFailures

        val currentCheckpointFailures = checkpointFailures.get()
        val checkpointFailuresDelta = currentCheckpointFailures - lastPublishedCheckpointFailures
        lastPublishedCheckpointFailures = currentCheckpointFailures

        val currentDeadLetterItems = deadLetterItems.get()
        val deadLetterItemsDelta = currentDeadLetterItems - lastPublishedDeadLetterItems
        lastPublishedDeadLetterItems = currentDeadLetterItems

        val data = Seq(
          MetricDatum
            .builder()
            .metricName("RecordsProcessed")
            .value(recordsDelta.toDouble)
            .unit(StandardUnit.COUNT)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("ActiveShards")
            .value(activeShards.get().toDouble)
            .unit(StandardUnit.COUNT)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("MaxIteratorAgeMs")
            .value(maxIteratorAgeMs.get().toDouble)
            .unit(StandardUnit.MILLISECONDS)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("PollDurationMs")
            .value(lastPollDurationMs.get().toDouble)
            .unit(StandardUnit.MILLISECONDS)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("WriteFailures")
            .value(writeFailuresDelta.toDouble)
            .unit(StandardUnit.COUNT)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("CheckpointFailures")
            .value(checkpointFailuresDelta.toDouble)
            .unit(StandardUnit.COUNT)
            .dimensions(tableDimension)
            .build(),
          MetricDatum
            .builder()
            .metricName("DeadLetterItems")
            .value(deadLetterItemsDelta.toDouble)
            .unit(StandardUnit.COUNT)
            .dimensions(tableDimension)
            .build()
        )
        client.putMetricData(
          PutMetricDataRequest
            .builder()
            .namespace(namespace)
            .metricData(data.asJava)
            .build()
        )
      } catch {
        case e: Exception =>
          log.warn("Failed to publish CloudWatch metrics", e)
      }
    }
  }

  override def close(): Unit = cloudWatchLock.synchronized {
    closed = true
    // Only close the client if it was actually initialized. Setting closed=true
    // inside the lock prevents publishToCloudWatch() from initializing it after
    // this point, so there is no race.
    cloudWatchClient.foreach { client =>
      try client.close()
      catch { case e: Exception => log.warn("Error closing CloudWatch client", e) }
    }
    cloudWatchClient = None
  }
}
