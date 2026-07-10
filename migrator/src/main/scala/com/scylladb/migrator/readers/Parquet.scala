package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{
  MigratorConfig,
  SourceSettings,
  SparkSecretRedaction,
  TargetSettings
}
import com.scylladb.migrator.scylla.{ ScyllaMigrator, ScyllaParquetMigrator, SourceDataFrame }
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import scala.util.Using
import scala.util.control.NonFatal

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  /** Redact a source path/URI for logging: drop any `user:secret@` authority, query string, and
    * fragment (e.g. `s3a://AKIA:secret@bucket/p?X-Amz-Signature=...`), keeping only
    * scheme/host/port/path. Falls back to the raw string for plain local paths or unparseable
    * inputs. Never use for the actual read — only for log output.
    */
  private[readers] def redactPathForLog(path: String): String =
    try {
      val uri = new java.net.URI(path)
      if (uri.getScheme == null) path
      else {
        val authority =
          if (uri.getHost != null)
            uri.getHost + (if (uri.getPort >= 0) ":" + uri.getPort else "")
          else if (uri.getAuthority != null) "<redacted-authority>"
          else ""
        s"${uri.getScheme}://$authority${Option(uri.getPath).getOrElse("")}"
      }
    } catch { case NonFatal(_) => path }

  def migrateToScylla(
    config: MigratorConfig,
    source: SourceSettings.Parquet,
    target: TargetSettings.Scylla
  )(implicit spark: SparkSession): Unit = {

    val useFileTracking = config.savepoints.enableParquetFileTracking

    if (useFileTracking) {
      log.info("Starting Parquet migration with file-level savepoint tracking")
      migrateWithSavepoints(config, source, target)
    } else {
      log.info("Starting Parquet migration without savepoint tracking")
      migrateWithoutSavepoints(config, source, target)
    }
  }

  /** Parquet migration with file-level savepoint tracking.
    *
    * This mode tracks completion of individual Parquet files, enabling resume capability if the
    * migration is interrupted. Uses SparkListener to detect when all partitions of a file have been
    * processed.
    */
  private def migrateWithSavepoints(
    config: MigratorConfig,
    source: SourceSettings.Parquet,
    target: TargetSettings.Scylla
  )(implicit spark: SparkSession): Unit = {
    configureHadoopCredentials(spark, source)

    val allFiles = listParquetFiles(spark, source.path)
    val skipFiles = config.getSkipParquetFilesOrEmptySet
    val filesToProcess = allFiles.filterNot(skipFiles.contains)

    if (filesToProcess.isEmpty) {
      log.info("No Parquet files to process. Migration is complete.")
      return
    }

    log.info(s"Processing ${filesToProcess.size} Parquet files")

    val df = if (skipFiles.isEmpty) {
      spark.read.parquet(source.path)
    } else {
      spark.read.parquet(filesToProcess: _*)
    }

    log.info("Reading partition metadata for file tracking...")
    val metadata = PartitionMetadataReader.readMetadataFromDataFrame(df)

    val partitionToFiles = PartitionMetadataReader.buildPartitionToFileMap(metadata)
    val fileToPartitions = PartitionMetadataReader.buildFileToPartitionsMap(metadata)

    log.info(
      s"Discovered ${fileToPartitions.size} files with ${metadata.size} total partitions to process"
    )

    Using.resource(
      ParquetSavepointsManager(
        config,
        spark.sparkContext,
        SparkSecretRedaction.redactionRegex(spark)
      )
    ) { savepointsManager =>
      val sourceDF = if (TimestampColumns.hasPerColumnMetaInParquet(df.schema)) {
        log.info(
          "Detected per-column CQL timestamp metadata in Parquet schema. " +
            "Performing row explosion for TTL/writetime preservation."
        )
        val renamed = TimestampColumns.renameFromParquet(df)
        val (explodedRdd, writeSchema, timestampColumns, collectionAppendWrites) =
          Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, renamed)
        // `savepointsSupported = false` is hardcoded here on purpose: although Parquet sources
        // *do* support savepoints (`SourceSettings.Parquet.supportsSavepoints == true`), the
        // resume mechanism is the external `ParquetSavepointsManager` injected via
        // `ScyllaParquetMigrator.externalSavepointsManager`. Marking the DataFrame as
        // unsupported tells `ScyllaMigratorBase.createSavepointsManager` not to spin up an
        // internal CQL manager that would race with the external one.
        SourceDataFrame(
          renamed,
          Some(timestampColumns),
          savepointsSupported    = false,
          cassandraExplodedWrite = Some((explodedRdd, writeSchema)),
          collectionAppendWrites = collectionAppendWrites
        )
      } else {
        SourceDataFrame(df, None, savepointsSupported = false)
      }

      // Incremental per-file savepoint marking (via `FileCompletionListener`) is only correct for
      // a SINGLE-pass write. With per-element collection-append passes, the base write is a
      // separate Spark job that reads every partition of every file and would make the listener
      // mark all files complete BEFORE the append passes run. A crash mid-append would then
      // persist those files as done and skip their un-appended collection elements on resume
      // (silent data loss). For the multi-pass case we skip incremental tracking entirely and mark
      // files only after ALL passes have succeeded (below). Re-running reprocesses the whole file
      // set, which is safe because base inserts and collection appends are idempotent under
      // `USING TIMESTAMP`. The common single-pass path keeps its fine-grained per-file resume.
      val listener =
        if (sourceDF.collectionAppendWrites.isEmpty) {
          val l = new FileCompletionListener(partitionToFiles, fileToPartitions, savepointsManager)
          spark.sparkContext.addSparkListener(l)
          Some(l)
        } else {
          log.info(
            "Per-element collection-append passes detected; disabling incremental Parquet file " +
              "savepoint tracking. Files are marked complete only after all write passes succeed; " +
              "an interrupted run reprocesses the whole file set on resume (idempotent)."
          )
          None
        }

      try {
        log.info("Created DataFrame from Parquet source")

        ScyllaParquetMigrator.migrate(config, target, sourceDF, savepointsManager)

        // Listener events can trail the completed Spark action; a successful write means every
        // selected file was consumed, so make the final savepoint deterministic. For the
        // multi-pass path this is the ONLY point at which files are marked (see above).
        filesToProcess.foreach(savepointsManager.markFileAsProcessed)
        savepointsManager.dumpMigrationState("completed")

        listener.foreach { l =>
          log.info(
            s"Parquet migration completed successfully: " +
              s"${l.getCompletedFilesCount}/${l.getTotalFilesCount} files processed"
          )
        }
      } finally
        listener.foreach { l =>
          spark.sparkContext.removeSparkListener(l)
          log.info(s"Final progress: ${l.getProgressReport}")
        }
    }
  }

  /** Parquet migration without savepoint tracking.
    *
    * This mode reads all Parquet files using Spark's native parallelism but does not track
    * individual file completion. If migration is interrupted, it will restart from the beginning.
    */
  private def migrateWithoutSavepoints(
    config: MigratorConfig,
    source: SourceSettings.Parquet,
    target: TargetSettings.Scylla
  )(implicit spark: SparkSession): Unit = {
    val sourceDF = ParquetWithoutSavepoints.readDataFrame(spark, source)
    ScyllaMigrator.migrate(config, target, sourceDF)
  }

  def listParquetFiles(spark: SparkSession, path: String): Seq[String] = {
    val safePath = redactPathForLog(path)
    log.info(s"Discovering Parquet files in $safePath")

    try {
      val dataFrame = spark.read
        .option("recursiveFileLookup", "true")
        .parquet(path)

      val files = dataFrame.inputFiles.toSeq.distinct.sorted

      if (files.isEmpty) {
        throw new IllegalArgumentException(s"No Parquet files found in $safePath")
      }

      log.info(s"Found ${files.size} Parquet file(s)")
      files
    } catch {
      case e: AnalysisException =>
        val message = s"Failed to list Parquet files from $safePath"
        log.error(message)
        throw new IllegalArgumentException(message, e)
    }
  }

  /** Configures Hadoop S3A credentials for reading from AWS S3.
    *
    * This method sets the necessary Hadoop configuration properties for AWS access key, secret key,
    * and optionally a session token. When a session token is present, it sets the credentials
    * provider to TemporaryAWSCredentialsProvider as required by Hadoop.
    *
    * If a region is specified in the source configuration, this method also sets the S3A endpoint
    * region via the `fs.s3a.endpoint.region` property.
    *
    * For more details, see the official Hadoop AWS documentation:
    * https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication
    */
  private[readers] def configureHadoopCredentials(
    spark: SparkSession,
    source: SourceSettings.Parquet
  ): Unit =
    source.finalCredentials.foreach { credentials =>
      val credentialOptions =
        Seq(
          "fs.s3a.access.key" -> credentials.accessKey,
          "fs.s3a.secret.key" -> credentials.secretKey
        ) ++ credentials.maybeSessionToken.toSeq.map { sessionToken =>
          "fs.s3a.session.token" -> sessionToken
        }

      SparkSecretRedaction.ensureKeysRedacted(
        spark,
        credentialOptions.map(_._1),
        "Parquet S3A Hadoop configuration"
      )
      log.info("Loaded AWS credentials from config file")
      source.region.foreach { region =>
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", region)
      }
      credentialOptions.foreach { case (key, value) =>
        spark.sparkContext.hadoopConfiguration.set(key, value)
      }
      credentials.maybeSessionToken.foreach { sessionToken =>
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.session.token",
          sessionToken
        )
      }
    }
}
