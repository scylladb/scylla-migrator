package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeClient, AerospikeException, Host, ResultCode, ScanCallback }
import com.aerospike.client.policy.{ ClientPolicy, ScanPolicy, TlsPolicy }
import com.aerospike.client.query.PartitionFilter
import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

object Aerospike {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Aerospike")

  /** Total number of data partitions in Aerospike */
  private[migrator] val TotalAerospikePartitions = 4096

  /** Default record-key column name, mirrored in SourceSettings.Aerospike. */
  private[migrator] val KeyColumnName = "aero_key"

  /** Default TTL metadata column name, mirrored in SourceSettings.Aerospike. */
  private[migrator] val TtlColumnName = "aero_ttl"

  /** Default generation metadata column name, mirrored in SourceSettings.Aerospike. */
  private[migrator] val GenerationColumnName = "aero_generation"

  private[migrator] case class Metrics(
    recordsRead: LongAccumulator,
    invalidRecords: LongAccumulator,
    skippedRecords: LongAccumulator
  ) {
    def withMigrationReport(migrate: => Unit): Unit = {
      try migrate
      catch {
        case failure: Throwable =>
          try
            log.warn(
              s"Aerospike source INCOMPLETE evaluated records after migration failure: " +
                s"read=${recordsRead.value}, " +
                s"invalid=${invalidRecords.value}, skipped=${skippedRecords.value}. " +
                "Best-effort partial counts can include replay and omit accumulator " +
                "updates from failed tasks."
            )
          catch {
            // Diagnostics must never replace the original migration failure, even if logging fails.
            case _: Throwable => ()
          }
          throw failure
      }
      report()
    }

    def report(): Unit = {
      val message = s"Aerospike source evaluated records: read=${recordsRead.value}, " +
        s"invalid=${invalidRecords.value}, skipped=${skippedRecords.value}. Counts can include replay."
      if (invalidRecords.value > 0 || skippedRecords.value > 0) log.warn(message)
      else log.info(message)
    }
  }

  private[migrator] case class ReadResult(source: SourceDataFrame, metrics: Metrics)

  def readDataframe(spark: SparkSession, source: SourceSettings.Aerospike): SourceDataFrame =
    readWithMetrics(spark, source).source

  private[migrator] def readWithMetrics(
    spark: SparkSession,
    source: SourceSettings.Aerospike
  ): ReadResult = {
    AerospikeTypes.reset() // Driver-side best-effort only; executor warn-counters are per-JVM
    // Empty namespace/set strings pass decoding; validate them before constructing the reader.
    require(source.set.nonEmpty, "Aerospike 'set' must not be empty in the source configuration")
    require(
      source.namespace.nonEmpty,
      "Aerospike 'namespace' must not be empty in the source configuration"
    )
    AerospikeHosts.validateTls(
      AerospikeHosts.parse(source.hosts, source.port.getOrElse(DefaultPort)),
      source.tlsName
    )
    val schema = buildSchema(source)

    val connConfig = connectionConfig(source)
    val requireEnvironmentCredentials = envCredentialsAvailable

    log.info("Configured Aerospike schema:\n" + schema.treeString)

    // 4x parallelism balances per-partition overhead against scan throughput: enough splits
    // to keep all cores busy even with skewed partition sizes, capped at the Aerospike limit.
    val splitCount = source.splitCount.getOrElse(
      math.min(TotalAerospikePartitions, spark.sparkContext.defaultParallelism * 4)
    )
    validateSplitCount(splitCount)

    val binNames = schema.fields
      .map(_.name)
      .filterNot(generatedColumnNames(source).contains)
      .toIndexedSeq
    val broadcastSchema = spark.sparkContext.broadcast(schema)
    val broadcastBinNames = spark.sparkContext.broadcast(binNames)
    // Only broadcast credentials from config when executor env vars are not set on the driver.
    // When AEROSPIKE_USERNAME/AEROSPIKE_PASSWORD are available, executors will read them at
    // runtime — avoiding plaintext credentials in the Spark serialization stream.
    val broadcastCredentials = if (requireEnvironmentCredentials) {
      log.warn(
        "AEROSPIKE_USERNAME/AEROSPIKE_PASSWORD env vars detected on driver; " +
          "environment credentials take precedence over config credentials and will NOT be broadcast. " +
          "Executors must also have both env vars set."
      )
      spark.sparkContext.broadcast(Option.empty[(String, String)])
    } else {
      if (source.credentials.isDefined)
        log.warn(
          "Broadcasting Aerospike credentials from config. For sensitive environments, " +
            "set AEROSPIKE_USERNAME/AEROSPIKE_PASSWORD env vars on all nodes instead."
        )
      spark.sparkContext.broadcast(
        source.credentials.map(c => (c.username, c.password))
      )
    }
    val readConfig = buildReadConfig(source, splitCount)

    // Source transformation metrics count evaluated records; retries and repeated actions can replay them.
    val recordsRead = spark.sparkContext.longAccumulator("Aerospike Records Read")
    val invalidRecords = spark.sparkContext.longAccumulator("Aerospike Invalid Source Records")
    val skippedRecords = spark.sparkContext.longAccumulator("Aerospike Skipped Source Records")
    val rdd = new AerospikeRDD(
      spark.sparkContext,
      connConfig,
      readConfig,
      broadcastCredentials,
      broadcastBinNames,
      broadcastSchema,
      recordsRead,
      invalidRecords,
      skippedRecords,
      requireEnvironmentCredentials
    )

    val df = spark.createDataFrame(rdd, schema)
    // Note: broadcastSchema, broadcastBinNames, and broadcastCredentials are not destroyed here
    // because the DataFrame is lazily evaluated — executors need the broadcast data when the
    // RDD is computed. They will be cleaned up when the SparkContext is stopped.
    // Savepoints are not currently supported for Aerospike sources. A future enhancement
    // could leverage AerospikePartition boundaries as savepoint tokens to enable resumable
    // migrations for large datasets.
    ReadResult(
      SourceDataFrame(df, None, savepointsSupported = source.supportsSavepoints),
      Metrics(recordsRead, invalidRecords, skippedRecords)
    )
  }

  private[migrator] def buildReadConfig(
    source: SourceSettings.Aerospike,
    splitCount: Int
  ): AerospikeReadConfig = AerospikeReadConfig(
    source.keyMode,
    source.namespace,
    source.set,
    splitCount,
    source.queueSize.getOrElse(1024),
    source.pollTimeoutSeconds.getOrElse(120),
    source.maxScanRetries.getOrElse(3),
    source.maxPollRetries.getOrElse(5),
    source.keyColumnName,
    Option.when(source.ttlEnabled)(source.ttlColumnName),
    Option.when(source.generationEnabled)(source.generationColumnName),
    source.invalidRecordHandling
  )

  /** Validate that splitCount is within the allowed range */
  private[migrator] def validateSplitCount(splitCount: Int): Unit = {
    require(splitCount > 0, s"splitCount must be positive, got $splitCount")
    require(
      splitCount <= TotalAerospikePartitions,
      s"splitCount ($splitCount) must not exceed $TotalAerospikePartitions"
    )
  }

  /** Build the declared projection without connecting to Aerospike. */
  private[migrator] def buildSchema(source: SourceSettings.Aerospike): StructType = {
    validateGeneratedColumnNames(source)
    validateSchemaAgainstReservedNames(source.schema, source)
    val names = source.schema.keys.toSeq
    require(names.forall(_.trim.nonEmpty), "Aerospike schema bin names must not be empty")
    require(
      names.map(_.toLowerCase(java.util.Locale.ROOT)).distinct.size == names.size,
      "Aerospike schema bin names must be distinct, ignoring case"
    )
    val keyField = StructField(
      source.keyColumnName,
      AerospikeTypes.keyDataType(source.keyMode),
      nullable = false
    )
    val fields = source.schema.toSeq.map { case (name, typeName) =>
      StructField(name, AerospikeTypes.parseType(typeName), nullable = true)
    }
    StructType(keyField +: fields ++: metadataFields(source))
  }

  /** Build optional metadata columns based on source config */
  private def metadataFields(source: SourceSettings.Aerospike): Seq[StructField] = {
    val ttlField =
      if (source.ttlEnabled)
        Seq(StructField(source.ttlColumnName, IntegerType, nullable = true))
      else Seq.empty
    val genField =
      if (source.generationEnabled)
        Seq(StructField(source.generationColumnName, IntegerType, nullable = true))
      else Seq.empty
    ttlField ++ genField
  }

  /** Only enabled generated columns reserve a name. Optional metadata names can otherwise be
    * ordinary Aerospike bins.
    */
  private[migrator] def generatedColumnNames(source: SourceSettings.Aerospike): Set[String] =
    Set(source.keyColumnName) ++
      Option.when(source.ttlEnabled)(source.ttlColumnName) ++
      Option.when(source.generationEnabled)(source.generationColumnName)

  private[migrator] def validateGeneratedColumnNames(source: SourceSettings.Aerospike): Unit = {
    val configured = Seq(
      "keyColumn"        -> source.keyColumnName,
      "ttlColumn"        -> source.ttlColumnName,
      "generationColumn" -> source.generationColumnName
    )
    configured.foreach { case (setting, name) =>
      require(name.trim.nonEmpty, s"Aerospike '$setting' must not be empty")
    }
    val active = Seq(source.keyColumnName) ++
      Option.when(source.ttlEnabled)(source.ttlColumnName) ++
      Option.when(source.generationEnabled)(source.generationColumnName)
    require(
      active.map(_.toLowerCase(java.util.Locale.ROOT)).distinct.size == active.size,
      "Aerospike generated column names must be distinct, ignoring case. " +
        "Choose different keyColumn, ttlColumn and generationColumn names for enabled metadata."
    )
  }

  /** A collision must be fixed before reading: destination renames happen after source columns have
    * been constructed, so they cannot disambiguate metadata from a source bin.
    */
  private[migrator] def validateSchemaAgainstReservedNames(
    schema: scala.collection.immutable.ListMap[String, String],
    source: SourceSettings.Aerospike
  ): Unit = validateBinsAgainstReservedNames(schema.keys.toSeq, source)

  private[migrator] def validateBinsAgainstReservedNames(
    bins: Seq[String],
    source: SourceSettings.Aerospike
  ): Unit = {
    val reserved = generatedColumnNames(source).map(_.toLowerCase(java.util.Locale.ROOT))
    val declared =
      bins.filter(name => reserved.contains(name.toLowerCase(java.util.Locale.ROOT))).distinct
    if (declared.nonEmpty)
      throw new IllegalArgumentException(
        s"Aerospike bin name(s) collide with generated columns: ${declared.mkString(", ")}. " +
          "Configure different keyColumn, ttlColumn or generationColumn names, disable the " +
          "corresponding optional metadata, or explicitly exclude the bins. Target 'renames' " +
          "cannot resolve a source column collision."
      )
  }

  private[migrator] val DefaultPort = 3000
  private val DefaultSocketTimeoutMs = 10000
  private val DefaultTotalTimeoutMs = 30000
  private val DefaultReadMaxRetries = 3
  private val DefaultReadSleepBetweenRetriesMs = 500

  /** Resolve the executor connection settings without creating a client. */
  private[migrator] def connectionConfig(
    source: SourceSettings.Aerospike
  ): AerospikeConnectionConfig =
    AerospikeConnectionConfig(
      source.hosts.toList,
      source.port.getOrElse(DefaultPort),
      source.connectTimeoutMs,
      Some(resolvedSocketTimeout(source)),
      Some(resolvedTotalTimeout(source)),
      source.tlsName,
      source.maxConnsPerNode,
      source.connPoolsPerNode
    )

  /** Resolve socket timeout for executor data reading. */
  private def resolvedSocketTimeout(source: SourceSettings.Aerospike): Int =
    source.socketTimeoutMs.getOrElse(DefaultSocketTimeoutMs)

  /** Resolve total timeout for executor data reading. */
  private def resolvedTotalTimeout(source: SourceSettings.Aerospike): Int =
    source.totalTimeoutMs.getOrElse(
      source.socketTimeoutMs
        .map(s => math.min(s.toLong * 3, Int.MaxValue.toLong).toInt)
        .getOrElse(DefaultTotalTimeoutMs)
    )

  /** Environment credentials take precedence over config/broadcast credentials on every JVM. Both
    * variables must be present together. If the driver chose environment-only authentication,
    * executors must also supply the pair; never silently connect without authentication instead.
    */
  private[migrator] def resolveCredentialsFromEnvOr(
    fallback: Option[(String, String)],
    requireEnvironmentCredentials: Boolean = false,
    getenv: String => String = System.getenv
  ): Option[(String, String)] = {
    val user = Option(getenv("AEROSPIKE_USERNAME"))
    val pass = Option(getenv("AEROSPIKE_PASSWORD"))
    require(user.forall(_.nonEmpty), "AEROSPIKE_USERNAME must not be empty")
    require(pass.forall(_.nonEmpty), "AEROSPIKE_PASSWORD must not be empty")
    require(
      user.isDefined == pass.isDefined,
      "AEROSPIKE_USERNAME and AEROSPIKE_PASSWORD must both be set or both be absent"
    )
    val fromEnv = for {
      username <- user
      password <- pass
    } yield (username, password)
    require(
      !requireEnvironmentCredentials || fromEnv.isDefined,
      "Aerospike credentials were selected from the driver environment; " +
        "set both AEROSPIKE_USERNAME and AEROSPIKE_PASSWORD on every executor"
    )
    if (fromEnv.isDefined && fallback.isDefined)
      log.warn(
        "Aerospike environment credentials take precedence over config/broadcast credentials"
      )
    fromEnv.orElse(fallback)
  }

  /** Resolve Aerospike credentials from environment variables or source config. Env vars take
    * precedence to avoid broadcasting plaintext credentials.
    */
  private[migrator] def resolveCredentials(
    source: SourceSettings.Aerospike
  ): Option[(String, String)] =
    resolveCredentialsFromEnvOr(source.credentials.map(c => (c.username, c.password)))

  /** Check whether Aerospike credentials are available via environment variables on this JVM. */
  private def envCredentialsAvailable: Boolean =
    resolveCredentialsFromEnvOr(None).isDefined

  /** Build a ClientPolicy from connection config and credentials, without connecting. */
  private[migrator] def buildClientPolicy(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): ClientPolicy =
    buildClientPolicy(
      connConfig,
      credentials,
      AerospikeHosts.parse(connConfig.hosts, connConfig.port)
    )

  private def buildClientPolicy(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)],
    hosts: Array[Host]
  ): ClientPolicy = {
    AerospikeHosts.validateTls(hosts, connConfig.tlsName)
    val policy = new ClientPolicy()
    policy.readPolicyDefault.maxRetries          = DefaultReadMaxRetries
    policy.readPolicyDefault.sleepBetweenRetries = DefaultReadSleepBetweenRetriesMs
    connConfig.connectTimeoutMs.foreach(t => policy.timeout = t)
    connConfig.socketTimeoutMs.foreach { t =>
      policy.readPolicyDefault.socketTimeout = t
      policy.scanPolicyDefault.socketTimeout = t
    }
    connConfig.totalTimeoutMs.foreach { t =>
      policy.readPolicyDefault.totalTimeout = t
      policy.scanPolicyDefault.totalTimeout = t
    }
    if (connConfig.tlsName.isDefined)
      policy.tlsPolicy = new TlsPolicy()
    connConfig.maxConnsPerNode.foreach(n => policy.maxConnsPerNode = n)
    connConfig.connPoolsPerNode.foreach(n => policy.connPoolsPerNode = n)
    credentials.foreach { case (user, password) =>
      policy.user     = user
      policy.password = password
    }
    policy
  }

  private[migrator] def buildClient(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): AerospikeClient = {
    val hostList = AerospikeHosts.parse(connConfig.hosts, connConfig.port).map { parsed =>
      connConfig.tlsName.fold(parsed)(name => new Host(parsed.name, name, parsed.port))
    }
    val policy = buildClientPolicy(connConfig, credentials, hostList)
    new AerospikeClient(policy, hostList: _*)
  }

  /** Run scanPartitions with or without bin name filtering */
  private[migrator] def runScanPartitions(
    client: AerospikeClient,
    policy: ScanPolicy,
    filter: PartitionFilter,
    namespace: String,
    set: String,
    callback: ScanCallback,
    bins: Option[Seq[String]]
  ): Unit = bins match {
    case Some(b) if b.nonEmpty =>
      client.scanPartitions(policy, filter, namespace, set, callback, b: _*)
    case _ =>
      client.scanPartitions(policy, filter, namespace, set, callback)
  }

  /** Check whether an AerospikeException represents a retryable error */
  private[migrator] def isRetryable(e: AerospikeException): Boolean =
    e.getResultCode == ResultCode.TIMEOUT ||
      e.getResultCode == ResultCode.SERVER_NOT_AVAILABLE ||
      e.getResultCode == ResultCode.PARTITION_UNAVAILABLE ||
      e.getResultCode == ResultCode.DEVICE_OVERLOAD
}
