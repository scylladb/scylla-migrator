package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{
  AWSCredentials,
  SourceSettings,
  StreamChangesSetting,
  TargetSettings
}
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisDynamoDBInputDStream,
  KinesisInitialPositions,
  KinesisInputDStream,
  SparkAWSCredentials
}
import org.apache.spark.util.LongAccumulator
import com.scylladb.migrator.DynamoUtils
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AttributeValueV2,
  DeleteItemRequest,
  PutItemRequest,
  TableDescription
}

import java.security.MessageDigest
import java.time.Instant
import java.util
import java.util.Date
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  type DynamoItem = util.Map[String, AttributeValueV1]

  /** Tag on a [[DynamoItem]] telling [[run]] how to apply it against the target.
    *
    * Replaces the pre-existing in-band marker `_dynamo_op_type` column whose collision surface was
    * silent: any source row containing an attribute literally named `_dynamo_op_type` with a
    * boolean value had its operation silently flipped (put <-> delete) on replay. An out-of-band
    * tag eliminates that class of bug entirely and is the defining structural change of this review
    * cycle (cross-model review finding LOGIC-1, escalated to CRITICAL).
    */
  sealed trait OpType
  object OpType {
    case object Put extends OpType
    case object Delete extends OpType
  }

  /** One element of the streaming RDD flowing into [[run]]: a decoded source item tagged with the
    * DynamoDB operation that produced it.
    *
    *   - `item` is the post-image for `Put` and the key-only image for `Delete`. The DDB-streams
    *     path emits `NewImage ++ Keys` for puts and `Keys` for deletes; the Kinesis JSON path does
    *     the same. Neither ever contains an out-of-band marker attribute anymore.
    *   - `op` is inspected exactly once inside [[run]] to decide `PutItemRequest` vs
    *     `DeleteItemRequest`; no further branching downstream.
    *
    * Represented as a `case class` (not a bare tuple) so that pattern matches are self-documenting
    * (`case StreamChange(item, OpType.Put) =>` reads as English) and future fields (e.g. source
    * sequence number, approximateCreationDateTime) have an obvious place to live.
    */
  final case class StreamChange(item: DynamoItem, op: OpType)

  /** Per-migrator-run counters.
    *
    * Instantiate ONCE and reuse across every streaming micro-batch — `LongAccumulator`s are
    * registered with the `SparkContext` in their constructor, and the context retains those
    * registrations for the life of the driver. Creating a new accumulator inside `foreachRDD` would
    * silently leak metadata (OOM after days of streaming). This class makes the correct
    * "construct-once" pattern the only ergonomic option for `createDStream` callers.
    */
  private[writers] final class Metrics(spark: SparkSession) {
    val putCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.putCount")
    val deleteCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.deleteCount")
    val droppedRecordsCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.droppedRecords")
  }

  /** Apply a stream of decoded source changes to the target DynamoDB table within a Spark
    * micro-batch. Invoked from [[createDStream]]'s `foreachRDD` on every batch.
    *
    * The `metrics` parameter is pre-constructed by the caller (not allocated here) by design.
    * [[Metrics]]'s constructor registers three [[org.apache.spark.util.LongAccumulator]]s with the
    * `SparkContext`, and the context retains those registrations for the life of the driver.
    * Constructing a fresh [[Metrics]] inside every micro-batch would silently leak accumulator
    * metadata — on the default 5-second batch interval that is ~17k leaked accumulators per day,
    * eventually OOM-ing the driver. Requiring callers to supply a [[Metrics]] they constructed once
    * per [[createDStream]] invocation enforces the "construct-once, reuse-every-batch" pattern
    * structurally: there is no convenience overload that accepts fewer arguments and silently leaks
    * on every call. (Finding ARCH-4 of the cross-model review — the previous 4-arg overload encoded
    * this invariant only in prose, which is fragile to future test or refactor changes.)
    */
  private[writers] def run(
    msgs: RDD[Option[StreamChange]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription,
    metrics: Metrics
  )(implicit spark: SparkSession): Unit = {
    val rdd = msgs.flatMap(_.toSeq)
    val putCount = metrics.putCount
    val deleteCount = metrics.deleteCount
    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet

    rdd.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val client =
          DynamoUtils.buildDynamoClient(
            target.endpoint,
            target.finalCredentials.map(_.toProvider),
            target.region,
            if (target.removeConsumedCapacity.getOrElse(true))
              Seq(new DynamoUtils.RemoveConsumedCapacityInterceptor)
            else Nil,
            target.alternator
          )
        try
          partition.foreach { case StreamChange(item, op) =>
            val itemConverted = item.asScala.map { case (k, v) =>
              k -> AttributeValueUtils.fromV1(v)
            }.asJava

            op match {
              case OpType.Put =>
                putCount.add(1)
                val finalItem = itemConverted.asScala.map { case (key, value) =>
                  renamesMap.getOrElse(key, key) -> value
                }.asJava
                try
                  client.putItem(
                    PutItemRequest.builder().tableName(target.table).item(finalItem).build()
                  )
                catch {
                  case e: Exception =>
                    log.error(s"Failed to put item into ${target.table}", e)
                }
              case OpType.Delete =>
                deleteCount.add(1)
                val keyToDelete = itemConverted.asScala
                  .filter { case (key, _) => keyAttributeNames.contains(key) }
                  .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
                  .asJava
                try
                  client.deleteItem(
                    DeleteItemRequest.builder().tableName(target.table).key(keyToDelete).build()
                  )
                catch {
                  case e: Exception =>
                    log.error(s"Failed to delete item from ${target.table}", e)
                }
            }
          }
        finally
          client.close()
      }
    }

    if (putCount.value > 0 || deleteCount.value > 0) {
      log.info(
        s"""
           |Changes to be applied:
           |  - ${putCount.value} items to UPSERT
           |  - ${deleteCount.value} items to DELETE
           |  - ${metrics.droppedRecordsCount.value} Kinesis records dropped by deserialization (lifetime total)
           |""".stripMargin
      )
    } else {
      // Demoted from INFO to DEBUG because with a 5s batch interval a quiet stream would otherwise
      // print this line thousands of times per day, drowning out real signal.
      log.debug("No changes to apply")
    }
  }

  /** Build the streaming DStream that replays source changes into `target` once the initial
    * snapshot has completed. Dispatches on [[TargetSettings.DynamoDB.streamChanges]]:
    *
    *   - [[StreamChangesSetting.DynamoDBStreams]] — consume the source table's DynamoDB Stream via
    *     our custom `KinesisDynamoDBInputDStream` + `RecordAdapter`.
    *   - [[StreamChangesSetting.KinesisDataStreams]] — consume a pre-existing Kinesis Data Stream
    *     via Spark's built-in `KinesisInputDStream` with a JSON `messageHandler`. (This is Option A
    *     of the design plan: lean on Spark's production-grade receiver rather than forking it.)
    *   - [[StreamChangesSetting.Disabled]] — unreachable in practice (`AlternatorMigrator` guards);
    *     logged and skipped.
    *
    * Both branches emit `RDD[Option[StreamChange]]` tagged with a typed [[OpType]], so the shared
    * [[run]] method applies them identically without ever inspecting user-controlled attribute
    * names.
    *
    * @param snapshotStartTime
    *   Recorded by `AlternatorMigrator` immediately before the snapshot scan. Used as the Kinesis
    *   `AT_TIMESTAMP` default so writes that arrive during a multi-hour snapshot are still replayed
    *   from the start of the snapshot window — the single most important reason users pick Kinesis
    *   over DDB Streams.
    */
  def createDStream(
    spark: SparkSession,
    streamingContext: StreamingContext,
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    snapshotStartTime: Instant
  ): Unit = {
    // Deliberately untyped: `SparkAWSCredentials` (the trait) is `private[kinesis]` in
    // Spark 4.x, so user code can't name it. The companion object is still reachable,
    // though, so we let type inference carry the value through.
    val kinesisCreds =
      src.credentials
        .map { case AWSCredentials(accessKey, secretKey, maybeAssumeRole) =>
          val builder =
            SparkAWSCredentials.builder
              .basicCredentials(accessKey, secretKey)
          for (assumeRole <- maybeAssumeRole)
            builder.stsCredentials(assumeRole.arn, assumeRole.getSessionName)
          builder.build()
        }
        .getOrElse(SparkAWSCredentials.builder.build())

    target.streamChanges match {
      case StreamChangesSetting.Disabled =>
        // Unreachable in the normal flow: AlternatorMigrator only calls createDStream when
        // streamChanges is enabled. If it ever does happen, logging is safer than throwing.
        log.warn("createDStream invoked with streamChanges=Disabled; skipping DStream wiring")

      case StreamChangesSetting.DynamoDBStreams =>
        val ddbAppName = defaultCheckpointAppName(src, StreamChangesSetting.DynamoDBStreams)
        log.warn(
          s"Using deterministic KCL application (checkpoint) name '$ddbAppName'. Two operator " +
            "consequences: (1) on restart the migrator resumes from prior KCL leases instead of " +
            "replaying from TrimHorizon; (2) if two migrators consume the same source table " +
            s"'${src.table}' concurrently (e.g. fan-out to two targets), they share this lease " +
            "table and KCL splits shard ownership — each target sees only half the changes. The " +
            "DDB-streams path does not currently expose an appName override; use Kinesis Data " +
            "Streams (which does) for concurrent-migrator fan-out."
        )
        val metrics = new Metrics(spark)
        new KinesisDynamoDBInputDStream(
          streamingContext,
          streamName        = src.table,
          regionName        = src.region.orNull,
          initialPosition   = new KinesisInitialPositions.TrimHorizon,
          checkpointAppName = ddbAppName,
          messageHandler = {
            case recAdapter: RecordAdapter =>
              val rec = recAdapter.getInternalObject
              val newMap: DynamoItem = new util.HashMap[String, AttributeValueV1]()

              if (rec.getDynamodb.getNewImage ne null) {
                newMap.putAll(rec.getDynamodb.getNewImage)
              }

              newMap.putAll(rec.getDynamodb.getKeys)

              val op =
                rec.getEventName match {
                  case "INSERT" | "MODIFY" => OpType.Put
                  case "REMOVE"            => OpType.Delete
                }
              Some(StreamChange(newMap, op))

            case _ => None
          },
          kinesisCreds = kinesisCreds
        ).foreachRDD { msgs =>
          run(msgs, target, renamesMap, targetTableDesc, metrics)(spark)
        }

      case kinesis: StreamChangesSetting.KinesisDataStreams =>
        val initialPosition = kinesis.initialTimestamp match {
          case Some(ts) =>
            log.info(s"Kinesis initial position: AT_TIMESTAMP=$ts (from config)")
            new KinesisInitialPositions.AtTimestamp(Date.from(ts))
          case None =>
            log.info(
              s"Kinesis initial position: AT_TIMESTAMP=$snapshotStartTime " +
                "(defaulted to snapshot start time)"
            )
            new KinesisInitialPositions.AtTimestamp(Date.from(snapshotStartTime))
        }

        val appName = kinesis.appName.getOrElse(defaultCheckpointAppName(src, kinesis))
        log.info(s"Kinesis KCL application (checkpoint) name: $appName")

        val metrics = new Metrics(spark)

        // Option A's defining choice: use Spark's built-in KinesisInputDStream rather than a
        // custom receiver. We buildWithMessageHandler so the DStream element type is already
        // the internal `Option[StreamChange]` shape `run` expects — no second map is needed.
        // `checkpointInterval` is intentionally omitted: the Spark builder defaults it to the
        // streaming context's batch duration, which is the correct and most common choice.
        // `StreamingContext.graph.batchDuration` is `private[streaming]`, so we can't read it
        // from user code to pass it back in explicitly anyway.
        // `streamName` must be the bare stream-name segment, not the full ARN: Spark forwards
        // this string verbatim to KCL 1.x `DescribeStream`, whose service-side `StreamName`
        // parameter is validated against `[a-zA-Z0-9_.-]{1,128}` and rejects anything containing
        // `:` or `/` (i.e. an ARN). The full ARN is still used for
        // `EnableKinesisStreamingDestination` in `DynamoUtils`, which requires an ARN.
        KinesisInputDStream.builder
          .streamingContext(streamingContext)
          .streamName(kinesis.arnName)
          .regionName(src.region.orNull)
          .initialPosition(initialPosition)
          .checkpointAppName(appName)
          .kinesisCredentials(kinesisCreds)
          .buildWithMessageHandler { rec =>
            // Count silent drops so a stream full of malformed KDS payloads surfaces in the
            // per-batch log line instead of vanishing into the void.
            val parsed = KinesisJsonDeserializer.parseRecord(rec)
            if (parsed.isEmpty) metrics.droppedRecordsCount.add(1)
            parsed
          }
          .foreachRDD { msgs =>
            run(msgs, target, renamesMap, targetTableDesc, metrics)(spark)
          }
    }
  }

  /** Deterministic KCL application / lease-table name shared by both streaming paths.
    *
    *   - DDB-streams path: `migrator_<src.table>`. No per-destination disambiguation is possible
    *     because a DDB table has a single stream and the migrator does not support fanning out to
    *     multiple targets via DDB-streams anyway (KCL would split shards across migrators).
    *   - Kinesis path: `migrator_<src.table>_<arn-hash-8>`. Two migrators fanning out the same
    *     source table to two different targets via two different Kinesis destinations each get
    *     their own lease table (DDB supports up to 2 Kinesis destinations per source table). The
    *     hash is a SHA-256 prefix of the stream ARN so it collides only at a 2^-32 rate — good
    *     enough for a handful of destinations and still readable in logs. When a deterministic app
    *     name is inconvenient the user can override via `streamChanges.appName`.
    *
    * Changed in the previous release from `s"migrator_${src.table}_${System.currentTimeMillis()}"`
    * to the deterministic forms above. Consequences:
    *
    *   - Restart resumes from prior KCL leases rather than replaying from the beginning —
    *     particularly important for Kinesis where the retention window can be a year.
    *   - Old `_<millis>` lease tables are orphaned after the upgrade — they continue to bill in
    *     DynamoDB and must be deleted manually. Documented in stream-changes.rst.
    */
  private[writers] def defaultCheckpointAppName(
    src: SourceSettings.DynamoDB,
    streaming: StreamChangesSetting
  ): String = streaming match {
    case k: StreamChangesSetting.KinesisDataStreams =>
      s"migrator_${src.table}_${shortHash(k.streamArn)}"
    case _ =>
      s"migrator_${src.table}"
  }

  /** First 8 hex chars of SHA-256(`input`). Deterministic across JVMs and platforms, so two
    * migrators that pick up the same config at different times compute the same lease-table name.
    */
  private def shortHash(input: String): String = {
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(input.getBytes("UTF-8"))
    digest.take(4).map("%02x".format(_)).mkString
  }
}
