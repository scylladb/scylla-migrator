package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ readers, writers, DynamoUtils }
import com.scylladb.migrator.config.{
  MigratorConfig,
  SourceSettings,
  StreamChangesSetting,
  TargetSettings
}
import com.scylladb.migrator.writers.DynamoStreamReplication
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import software.amazon.awssdk.services.dynamodb.model.TableDescription

import java.time.Instant
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._
import scala.util.Using

object AlternatorMigrator {
  private val log = LogManager.getLogger("com.scylladb.migrator.alternator")

  def migrateFromDynamoDB(
    source: SourceSettings.DynamoDBLike,
    target: TargetSettings.DynamoDBLike,
    migratorConfig: MigratorConfig
  )(implicit spark: SparkSession): Unit = {
    val (sourceRDD, sourceTableDesc) =
      readers.DynamoDB.readRDD(spark, source, migratorConfig.skipSegments)
    val maybeStreamedSource = source match {
      case d: SourceSettings.DynamoDB if target.streamChanges.isEnabled => Some(d)
      case _                                                            => None
    }
    if (target.streamChanges.isEnabled && maybeStreamedSource.isEmpty) {
      throw new IllegalArgumentException(
        "streamChanges streaming replication is enabled on the target, but the source does not expose a live AWS DynamoDB table for streaming. " +
          "This combination should have been rejected at config-parse time. " +
          "Stream replication cannot proceed."
      )
    }
    migrate(
      sourceRDD,
      sourceTableDesc,
      maybeStreamedSource,
      target,
      migratorConfig,
      hadoopPartitionedSource = true
    )
  }

  def migrateToS3Export(
    source: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDBS3Export,
    migratorConfig: MigratorConfig
  )(implicit spark: SparkSession): Unit = {
    val (sourceRDD, sourceTableDesc) =
      readers.DynamoDB.readRDD(spark, source, migratorConfig.skipSegments)
    Using.resource(DynamoDbSavepointsManager(migratorConfig, sourceRDD, spark.sparkContext)) {
      savepointsManager =>
        try
          writers.DynamoDBS3Export.writeRDD(target, sourceRDD, Some(sourceTableDesc))
        catch {
          case NonFatal(e) =>
            log.error(
              "Caught error while writing DynamoDB S3 Export. Will create a savepoint before exiting",
              e
            )
        } finally
          savepointsManager.dumpMigrationState("final")
    }
  }

  /** Migrate an S3 DynamoDB export into the target, optionally chaining into a live-table change
    * stream once the export has been applied.
    *
    * When `target.streamChanges.isEnabled` the caller MUST also provide `source.streamSource` — the
    * coordinates of the still-live DynamoDB table the export was taken from. The migrator enables
    * the configured streaming destination on that live table and uses the export's `exportTime` (or
    * `startTime` as a fallback) as the Kinesis `AT_TIMESTAMP` default, so changes that happened
    * after the export was produced are replayed into the target.
    *
    * This addresses GitHub issue #250 acceptance criterion #4 (S3-export + AT_TIMESTAMP default
    * from the export's own snapshot instant). If the user asks for streamChanges but does not
    * provide a `streamSource`, we fail fast at migration start rather than silently dropping the
    * streaming request (which was the pre-review behaviour and finding ARCH-1).
    */
  def migrateFromS3Export(
    source: SourceSettings.DynamoDBS3Export,
    target: TargetSettings.DynamoDBLike,
    migratorConfig: MigratorConfig
  )(implicit spark: SparkSession): Unit = {
    val (sourceRDD, sourceTableDesc, exportStartTime) =
      readers.DynamoDBS3Export.readRDD(source)(spark.sparkContext)

    // Adapt the decoded items to the format expected by the EMR Hadoop connector
    val normalizedRDD =
      sourceRDD.map { item =>
        (new Text(), new DynamoDBItemWritable(item.asJava))
      }

    if (target.streamChanges.isEnabled) {
      val streamedSource = source.streamSource.getOrElse(
        sys.error(
          "target.streamChanges is enabled with an S3-export source, but source.streamSource is " +
            "not set. Supply the coordinates of the still-live DynamoDB table whose export is in " +
            "S3 so the migrator can enable the streaming destination on it and consume change " +
            "events from there. See docs/source/stream-changes.rst (section 'Chaining S3-export " +
            "with streamChanges')."
        )
      )

      if (exportStartTime.isEmpty) {
        log.warn(
          "S3 export manifest does not contain 'exportTime' or 'startTime'; the Kinesis " +
            "AT_TIMESTAMP will fall back to 'Instant.now()' when the migrator starts (before " +
            "the snapshot write phase). This means any writes between when the export was " +
            "produced and that captured timestamp may be LOST. If you need to replay those " +
            "writes, set `streamChanges.initialTimestamp` explicitly to the export's creation time."
        )
      } else {
        log.info(
          s"S3 export start time = ${exportStartTime.get}; using it as the Kinesis " +
            "AT_TIMESTAMP default for any writes that happened after the export was produced."
        )
      }

      migrate(
        normalizedRDD,
        sourceTableDesc,
        Some(streamedSource),
        target,
        migratorConfig,
        hadoopPartitionedSource   = false,
        snapshotStartTimeOverride = exportStartTime
      )
    } else {
      migrate(
        normalizedRDD,
        sourceTableDesc,
        None,
        target,
        migratorConfig,
        hadoopPartitionedSource = false
      )
    }
  }

  /** @param sourceRDD
    *   Data to migrate
    * @param sourceTableDesc
    *   Description of the table to replicate on the target database
    * @param maybeStreamedSource
    *   Settings of the source table in case `streamChanges` was `true`
    * @param target
    *   Target table settings
    * @param migratorConfig
    *   The complete original configuration
    * @param hadoopPartitionedSource
    *   Whether the source RDD uses `HadoopPartition`s (from the DynamoDB Hadoop connector)
    *   containing `DynamoDBSplit`s. When `true`, scan segment progress is tracked via
    *   `DynamoDbSavepointsManager`. Must be `false` for source types whose RDDs use other partition
    *   types (e.g., `ParallelCollectionPartition` from S3 exports).
    * @param snapshotStartTimeOverride
    *   When set (e.g. by `migrateFromS3Export`), use this as the Kinesis `AT_TIMESTAMP` default
    *   instead of capturing `Instant.now()` at the start of the migration. Ignored unless the
    *   target streams via Kinesis.
    * @param spark
    *   Spark session
    */
  def migrate(
    sourceRDD: RDD[(Text, DynamoDBItemWritable)],
    sourceTableDesc: TableDescription,
    maybeStreamedSource: Option[SourceSettings.DynamoDB],
    target: TargetSettings.DynamoDBLike,
    migratorConfig: MigratorConfig,
    hadoopPartitionedSource: Boolean,
    snapshotStartTimeOverride: Option[Instant] = None
  )(implicit spark: SparkSession): Unit = {

    log.info("We need to transfer: " + sourceRDD.getNumPartitions + " partitions in total")

    try {
      val targetTableDesc = {
        for (streamedSource <- maybeStreamedSource)
          target.streamChanges match {
            case StreamChangesSetting.DynamoDBStreams =>
              log.info(
                "Source is a Dynamo table and DynamoDB Streams replication requested; enabling " +
                  "Dynamo Stream"
              )
              DynamoUtils.enableDynamoStream(streamedSource)
            case kinesis: StreamChangesSetting.KinesisDataStreams =>
              require(
                streamedSource.region.isDefined,
                "streamChanges.type=kinesis requires source.region to be explicitly set: the " +
                  "Kinesis Data Streams KCL path needs a concrete region for both the Kinesis " +
                  "data-plane client and the DynamoDB lease table. Add `region: <aws-region>` " +
                  "to the source block in your config and retry."
              )
              // LOGIC-8 cross-check: the ARN's region must agree with source.region. If they
              // disagree the KCL connects to the wrong region and either sees no shards (best
              // case — silent zero-event stream) or fails with a confusing 403 (worst case —
              // looks like a credentials problem). Catching it here turns either into an
              // actionable error at process start. The `arnRegion` field on
              // `StreamChangesSetting.KinesisDataStreams` is the parsed region from the already-
              // validated ARN; using it here avoids re-importing the `private[config]`
              // `KinesisArn` helper from outside the `config` package.
              require(
                streamedSource.region.contains(kinesis.arnRegion),
                s"streamChanges.streamArn is in region '${kinesis.arnRegion}' but source.region " +
                  s"is '${streamedSource.region.get}'. The KCL client authenticates to the ARN's " +
                  s"region; a mismatch will either surface as 'zero events streamed' or as an " +
                  s"authentication error. Fix the config so the two regions match."
              )
              log.info(
                "Source is a Dynamo table and Kinesis Data Streams replication requested " +
                  s"(stream=${kinesis.streamArn}); enabling Kinesis streaming destination"
              )
              DynamoUtils.enableKinesisStreamingDestination(streamedSource, kinesis.streamArn)
              DynamoUtils.waitForKinesisStreamingActive(streamedSource, kinesis.streamArn)
            case StreamChangesSetting.Disabled =>
            // Unreachable: maybeStreamedSource is only populated when streamChanges.isEnabled is
            // true, which excludes Disabled. Left as an exhaustiveness hint for the compiler.
          }

        DynamoUtils.replicateTableDefinition(
          sourceTableDesc,
          target
        )
      }

      // Capture T0 AFTER the streaming destination is ACTIVE (ARCH-2). For a Kinesis enable
      // that takes minutes to reach ACTIVE, capturing before the wait would lose every write
      // that happened during the enable window — `AT_TIMESTAMP=<pre-activation>` does nothing
      // because no records were streaming yet. Moving the capture here means the default T0 is
      // "the earliest instant at which a record could have reached the Kinesis stream".
      //
      // For the DDB-streams path this value is not used (the 24h-bounded TrimHorizon is used
      // instead), but we still capture it unconditionally so the branches stay symmetric.
      //
      // When the caller supplied `snapshotStartTimeOverride` (e.g. an S3 export's exportTime),
      // that wins over the locally-captured Instant.now().
      val snapshotStartTime: Instant =
        snapshotStartTimeOverride.getOrElse(Instant.now())

      if (target.streamChanges.isEnabled && target.skipInitialSnapshotTransfer.contains(true)) {
        log.info("Skip transferring table snapshot")
      } else {
        if (hadoopPartitionedSource) {
          Using.resource(
            DynamoDbSavepointsManager(migratorConfig, sourceRDD, spark.sparkContext)
          ) { _ =>
            log.info("Starting write...")
            writers.DynamoDB
              .writeRDD(target, migratorConfig.renamesMap, sourceRDD, targetTableDesc)
          }
        } else {
          log.warn(
            "Savepoints are not supported when the source is a DynamoDB S3 export. " +
              "If the migration is interrupted, it will reprocess all data on restart."
          )
          log.info("Starting write...")
          writers.DynamoDB.writeRDD(target, migratorConfig.renamesMap, sourceRDD, targetTableDesc)
        }
        log.info("Done transferring table snapshot")
      }

      for (streamedSource <- maybeStreamedSource) {
        log.info("Starting to transfer changes")
        val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        DynamoStreamReplication.createDStream(
          spark,
          streamingContext,
          streamedSource,
          target,
          targetTableDesc,
          migratorConfig.renamesMap,
          snapshotStartTime
        )

        streamingContext.start()
        streamingContext.awaitTermination()
      }
    } catch {
      case NonFatal(e) =>
        throw new Exception("Caught error while writing the RDD.", e)
    }

  }

}
