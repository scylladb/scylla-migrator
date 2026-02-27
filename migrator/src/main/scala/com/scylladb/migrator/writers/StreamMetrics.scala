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
  enableCloudWatch: Boolean
) extends AutoCloseable {

  private val log = LogManager.getLogger("com.scylladb.migrator.writers.StreamMetrics")

  val recordsProcessed = new AtomicLong(0L)
  val pollCycles = new AtomicLong(0L)
  val activeShards = new AtomicLong(0L)
  val maxIteratorAgeMs = new AtomicLong(0L)
  val lastPollDurationMs = new AtomicLong(0L)

  // --- CloudWatch (optional, lazily initialized) ---

  private lazy val cloudWatchClient: Option[CloudWatchClient] =
    if (enableCloudWatch)
      try {
        val builder = CloudWatchClient.builder()
        region.foreach(r => builder.region(software.amazon.awssdk.regions.Region.of(r)))
        val client = builder.build()
        log.info("CloudWatch metrics publishing enabled")
        Some(client)
      } catch {
        case e: Exception =>
          log.warn("Failed to create CloudWatch client, metrics will not be published", e)
          None
      }
    else None

  private val namespace = "ScyllaMigrator/StreamReplication"

  private val tableDimension =
    Dimension.builder().name("TableName").value(tableName).build()

  /** Publish current metric values to CloudWatch. Call periodically (e.g., every 60 cycles). */
  def publishToCloudWatch(): Unit =
    cloudWatchClient.foreach { client =>
      try {
        val data = Seq(
          MetricDatum
            .builder()
            .metricName("RecordsProcessed")
            .value(recordsProcessed.get().toDouble)
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

  override def close(): Unit =
    cloudWatchClient.foreach { client =>
      try client.close()
      catch { case e: Exception => log.warn("Error closing CloudWatch client", e) }
    }
}
