package com.scylladb.migrator.readers

import com.aerospike.client.{
  AerospikeClient,
  AerospikeException,
  Host,
  Info,
  Key,
  Record,
  ResultCode,
  ScanCallback
}
import com.aerospike.client.policy.{ ClientPolicy, InfoPolicy, ScanPolicy, TlsPolicy }
import com.aerospike.client.query.PartitionFilter
import com.scylladb.migrator.config.{ SchemaDiscoveryStrategy, SourceSettings }
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicReference }
import scala.jdk.CollectionConverters._

object Aerospike {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Aerospike")

  /** Total number of data partitions in Aerospike */
  private[migrator] val TotalAerospikePartitions = 4096

  /** Maximum recommended schemaSampleSize for schema discovery */
  private[migrator] val MaxSchemaSampleSize = 10000

  /** Maximum number of distinct bins tracked during schema discovery to prevent OOM */
  private val MaxDiscoveredBins = 10000

  /** Number of partitions for the initial schema discovery sample (progressive/single mode) */
  private val InitialSamplePartitions = 8

  /** Number of partitions for the extended schema discovery fallback (progressive mode) */
  private val ExtendedSamplePartitions = 64

  /** Column name used for the Aerospike record key in the resulting DataFrame */
  private[migrator] val KeyColumnName = "aero_key"

  /** Column name for the Aerospike record TTL (seconds remaining), when preserveTTL is enabled */
  private[migrator] val TtlColumnName = "aero_ttl"

  /** Column name for the Aerospike record generation count, when preserveGeneration is enabled */
  private[migrator] val GenerationColumnName = "aero_generation"

  def readDataframe(
    spark: SparkSession,
    source: SourceSettings.Aerospike
  ): SourceDataFrame = {
    AerospikeTypes.reset() // Clear stale warning state from prior jobs in the same JVM
    AerospikeTypes.requireJdk17()
    // Structural invariants not covered by the decoder (empty strings/seqs pass decoding)
    require(source.set.nonEmpty, "Aerospike 'set' must not be empty in the source configuration")
    require(
      source.namespace.nonEmpty,
      "Aerospike 'namespace' must not be empty in the source configuration"
    )
    require(source.hosts.nonEmpty, "Aerospike 'hosts' must not be empty")

    val connConfig = AerospikeConnectionConfig(
      source.hosts.toList,
      source.port.getOrElse(DefaultPort),
      source.connectTimeoutMs,
      Some(resolvedSocketTimeout(source)),
      Some(resolvedTotalTimeout(source)),
      source.tlsName,
      source.maxConnsPerNode,
      source.connPoolsPerNode
    )

    val schema = discoverSchema(source, connConfig)
    log.info("Discovered Aerospike schema:")
    schema.printTreeString()

    // 4x parallelism balances per-partition overhead against scan throughput: enough splits
    // to keep all cores busy even with skewed partition sizes, capped at the Aerospike limit.
    val splitCount = source.splitCount.getOrElse(
      math.min(TotalAerospikePartitions, spark.sparkContext.defaultParallelism * 4)
    )
    validateSplitCount(splitCount)

    val binNames = schema.fields
      .map(_.name)
      .filter { n =>
        n != KeyColumnName && n != TtlColumnName && n != GenerationColumnName
      }
      .toIndexedSeq
    val queueSize = source.queueSize.getOrElse(1024)
    val pollTimeoutSeconds = source.pollTimeoutSeconds.getOrElse(120)

    val broadcastSchema = spark.sparkContext.broadcast(schema)
    val broadcastBinNames = spark.sparkContext.broadcast(binNames)
    // Only broadcast credentials from config when executor env vars are not set on the driver.
    // When AEROSPIKE_USERNAME/AEROSPIKE_PASSWORD are available, executors will read them at
    // runtime — avoiding plaintext credentials in the Spark serialization stream.
    val broadcastCredentials = if (envCredentialsAvailable) {
      log.info(
        "AEROSPIKE_USERNAME/AEROSPIKE_PASSWORD env vars detected on driver; " +
          "credentials will NOT be broadcast. Executors must also have these env vars set."
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
    val readConfig = AerospikeReadConfig(
      source.namespace,
      source.set,
      splitCount,
      queueSize,
      pollTimeoutSeconds,
      source.maxScanRetries.getOrElse(3),
      source.maxPollRetries.getOrElse(5)
    )

    val recordsRead = spark.sparkContext.longAccumulator("Aerospike Records Read")

    val rdd = new AerospikeRDD(
      spark.sparkContext,
      connConfig,
      readConfig,
      broadcastCredentials,
      broadcastBinNames,
      broadcastSchema,
      recordsRead
    )

    val df = spark.createDataFrame(rdd, schema)
    // Note: broadcastSchema, broadcastBinNames, and broadcastCredentials are not destroyed here
    // because the DataFrame is lazily evaluated — executors need the broadcast data when the
    // RDD is computed. They will be cleaned up when the SparkContext is stopped.
    // Savepoints are not currently supported for Aerospike sources. A future enhancement
    // could leverage AerospikePartition boundaries as savepoint tokens to enable resumable
    // migrations for large datasets.
    SourceDataFrame(df, None, savepointsSupported = false)
  }

  /** Converge observed key types. Returns StringType when mixed key types are observed. */
  private def convergeKeyType(
    current: Option[DataType],
    observed: DataType
  ): Option[DataType] =
    current match {
      case None           => Some(observed)
      case Some(existing) => Some(if (existing != observed) StringType else existing)
    }

  /** Validate that splitCount is within the allowed range */
  private[migrator] def validateSplitCount(splitCount: Int): Unit = {
    require(splitCount > 0, s"splitCount must be positive, got $splitCount")
    require(
      splitCount <= TotalAerospikePartitions,
      s"splitCount ($splitCount) must not exceed $TotalAerospikePartitions"
    )
  }

  /** Compute a random starting partition offset for schema discovery sampling. Avoids always
    * scanning partitions 0..N-1, which may all reside on the same Aerospike node.
    */
  private def schemaSampleOffset(samplePartitionCount: Int): Int =
    scala.util.Random.nextInt(TotalAerospikePartitions - samplePartitionCount + 1)

  /** Build a ScanPolicy configured with the given sample size and timeouts */
  private def buildScanPolicy(source: SourceSettings.Aerospike, sampleSize: Int): ScanPolicy = {
    val policy = new ScanPolicy()
    policy.maxRecords    = sampleSize
    policy.socketTimeout = resolvedSocketTimeout(source)
    policy.totalTimeout  = resolvedTotalTimeout(source)
    policy
  }

  /** Discover the schema by scanning a sample of records, or use a user-provided schema override */
  private def discoverSchema(
    source: SourceSettings.Aerospike,
    connConfig: AerospikeConnectionConfig
  ): StructType = {
    val metaFields = metadataFields(source)

    // Warn when both schema override and discovery strategy are configured (TODO 18)
    if (source.schema.isDefined && source.schemaDiscoveryStrategy.isDefined)
      log.warn(
        "Both 'schema' and 'schemaDiscoveryStrategy' are configured. " +
          "The explicit 'schema' override takes precedence; 'schemaDiscoveryStrategy' will be ignored."
      )

    source.schema match {
      case Some(userSchema) =>
        source.bins.foreach(binFilter => validateSchemaAgainstBins(userSchema, binFilter))
        val fields = userSchema.map { case (name, typeName) =>
          StructField(name, AerospikeTypes.parseType(typeName), nullable = true)
        }.toSeq
        StructType(
          StructField(KeyColumnName, StringType, nullable = false) +: fields ++: metaFields
        )

      case None =>
        val credentials = resolveCredentials(source)
        // Client is intentionally cached in AerospikeClientHolder; reused by executors
        // and closed by JVM shutdown hook.
        val client =
          try AerospikeClientHolder.get(connConfig, credentials)
          catch {
            case e: AerospikeException =>
              throw new RuntimeException(
                s"Failed to connect to Aerospike at ${connConfig.hosts.mkString(",")}:${connConfig.port} for schema discovery",
                e
              )
          }

        try {
          // Verify the namespace exists before scanning to fail fast on misconfiguration
          try {
            val node = client.getNodes.headOption.getOrElse(
              throw new IllegalStateException("No Aerospike nodes available")
            )
            val namespaces =
              Info.request(new InfoPolicy(), node, "namespaces")
            if (!namespaces.split(";").contains(source.namespace))
              throw new IllegalArgumentException(
                s"Aerospike namespace '${source.namespace}' not found. " +
                  s"Available namespaces: $namespaces"
              )
          } catch {
            case e: IllegalArgumentException => throw e
            case e: Exception =>
              log.warn(
                s"Could not verify namespace '${source.namespace}' via info protocol: ${e.getMessage}. " +
                  "Proceeding with schema discovery anyway."
              )
          }

          val sampleSize = source.schemaSampleSize.getOrElse(1000)
          require(sampleSize > 0, "schemaSampleSize must be positive")
          if (sampleSize > MaxSchemaSampleSize)
            log.warn(
              s"schemaSampleSize=$sampleSize exceeds recommended maximum of $MaxSchemaSampleSize. " +
                "Very large values may increase schema discovery time without benefit."
            )

          log.info(
            s"Schema discovery sampling up to $sampleSize records. " +
              "Bins that only appear in later records will not be discovered."
          )

          val bins = new ConcurrentHashMap[String, DataType]()
          val binsLimitWarned = new AtomicBoolean(false)
          // Tracks the discovered key type across concurrent scan callback threads.
          // None = no key observed yet; Some(StringType) = null-user-key seen or mixed types
          // (forces hex digest fallback); Some(LongType/BinaryType) = uniform typed keys.
          // Once set to Some(StringType), it never reverts — StringType is the absorbing element.
          val keyTypeRef = new AtomicReference[Option[DataType]](None)
          val sampledCount = new AtomicInteger(0)
          // Collect type conflict messages during scanning; logged after scan completes
          // to avoid blocking scan callback threads on synchronous log appenders.
          val typeConflicts = new ConcurrentLinkedQueue[String]()

          val callback = new ScanCallback {
            override def scanCallback(key: Key, record: Record): Unit = {
              if (key.userKey != null) {
                val thisKeyType = key.userKey.getObject match {
                  case _: java.lang.Long => LongType
                  case _: Array[Byte]    => BinaryType
                  case _                 => StringType
                }
                keyTypeRef.getAndUpdate(convergeKeyType(_, thisKeyType))
              } else {
                // Null userKey forces StringType (hex digest fallback)
                keyTypeRef.getAndUpdate(convergeKeyType(_, StringType))
              }
              record.bins.asScala.foreach { case (name, value) =>
                if (value != null) {
                  if (bins.size() >= MaxDiscoveredBins && !bins.containsKey(name)) {
                    if (binsLimitWarned.compareAndSet(false, true))
                      log.warn(
                        s"Schema discovery: bin count reached $MaxDiscoveredBins, " +
                          "skipping new bins. Consider providing an explicit 'schema' override."
                      )
                  } else {
                    val newType = AerospikeTypes.inferSparkType(value)
                    // merge() is atomic — safe under concurrent scan callback threads
                    bins.merge(
                      name,
                      newType,
                      (existing, incoming) => {
                        val merged = AerospikeTypes.mergeTypes(existing, incoming)
                        if (merged != existing)
                          typeConflicts.add(
                            s"Schema discovery: type conflict for bin '$name' — " +
                              s"merging $existing and $incoming into $merged."
                          )
                        merged
                      }
                    )
                  }
                }
              }
              sampledCount.incrementAndGet()
            }
          }

          val strategy =
            source.schemaDiscoveryStrategy.getOrElse(SchemaDiscoveryStrategy.Progressive)
          strategy match {
            case SchemaDiscoveryStrategy.Single =>
              // Single scan with a small partition range — fastest startup.
              // Random offset avoids always sampling the same partitions (and the same node).
              val samplePartitionCount = math.min(InitialSamplePartitions, TotalAerospikePartitions)
              val offset = schemaSampleOffset(samplePartitionCount)
              tryScanPartitions(
                client,
                buildScanPolicy(source, sampleSize),
                PartitionFilter.range(offset, samplePartitionCount),
                source.namespace,
                source.set,
                callback,
                source.bins
              )

            case SchemaDiscoveryStrategy.Full =>
              // Full cluster scan — most thorough, may be slow on large clusters
              tryScanPartitions(
                client,
                buildScanPolicy(source, sampleSize),
                PartitionFilter.all(),
                source.namespace,
                source.set,
                callback,
                source.bins
              )

            case SchemaDiscoveryStrategy.Progressive =>
              // Scan a small partition range first to reduce cluster-wide fan-out.
              // Random offset avoids always sampling the same partitions.
              val samplePartitionCount = math.min(InitialSamplePartitions, TotalAerospikePartitions)
              val offset = schemaSampleOffset(samplePartitionCount)
              tryScanPartitions(
                client,
                buildScanPolicy(source, sampleSize),
                PartitionFilter.range(offset, samplePartitionCount),
                source.namespace,
                source.set,
                callback,
                source.bins
              )

              // Progressive fallback: 8 -> 64 -> all partitions.
              // A fresh ScanPolicy is used at each step because maxRecords is not reset between
              // scans. Each fallback step only executes if sampledCount == 0 (i.e., the previous
              // scan returned zero records), so the callback's state never accumulates across
              // overlapping scans.
              if (sampledCount.get() == 0) {
                log.warn(
                  s"No records found in initial $InitialSamplePartitions-partition sample, expanding to $ExtendedSamplePartitions partitions for schema discovery"
                )
                val extCount = math.min(ExtendedSamplePartitions, TotalAerospikePartitions)
                tryScanPartitions(
                  client,
                  buildScanPolicy(source, sampleSize),
                  PartitionFilter.range(schemaSampleOffset(extCount), extCount),
                  source.namespace,
                  source.set,
                  callback,
                  source.bins
                )
              }
              if (sampledCount.get() == 0) {
                log.warn(
                  s"No records found in $ExtendedSamplePartitions-partition sample, falling back to full cluster scan for schema discovery"
                )
                tryScanPartitions(
                  client,
                  buildScanPolicy(source, sampleSize),
                  PartitionFilter.all(),
                  source.namespace,
                  source.set,
                  callback,
                  source.bins
                )
              }
          }

          // Log type conflicts collected during the scan (outside the callback threads)
          var conflict = typeConflicts.poll()
          while (conflict != null) {
            log.warn(
              conflict + " Consider providing an explicit 'schema' override if this is unexpected."
            )
            conflict = typeConflicts.poll()
          }

          val finalSampledCount = sampledCount.get()
          log.info(
            s"Schema discovery sampled $finalSampledCount records, discovered ${bins.size()} bins"
          )
          if (finalSampledCount == 0) {
            log.warn(
              "Schema discovery found zero records. The resulting schema will contain only the 'aero_key' column. " +
                "Verify that the Aerospike namespace and set are correct and contain data."
            )
          }

          val fieldSeq = if (source.bins.isDefined) {
            source.bins.get.map { binName =>
              val binType = Option(bins.get(binName)).getOrElse(StringType)
              StructField(binName, binType, nullable = true)
            }
          } else {
            bins.asScala.toSeq.sortBy(_._1).map { case (name, dataType) =>
              StructField(name, dataType, nullable = true)
            }
          }

          val finalKeyType = keyTypeRef.get().getOrElse(StringType)
          StructType(
            StructField(KeyColumnName, finalKeyType, nullable = false) +: fieldSeq ++: metaFields
          )
        } finally
          // Release the driver-side client after schema discovery — it is only needed briefly
          // and executors will create their own connections via AerospikeClientHolder.
          AerospikeClientHolder.release(connConfig, credentials)
    }
  }

  /** Build optional metadata columns based on source config */
  private def metadataFields(source: SourceSettings.Aerospike): Seq[StructField] = {
    val ttlField =
      if (source.ttlEnabled)
        Seq(StructField(TtlColumnName, IntegerType, nullable = true))
      else Seq.empty
    val genField =
      if (source.generationEnabled)
        Seq(StructField(GenerationColumnName, IntegerType, nullable = true))
      else Seq.empty
    ttlField ++ genField
  }

  /** Validate that when both bins filter and schema override are provided, all schema keys are a
    * subset of the bins filter.
    */
  private[migrator] def validateSchemaAgainstBins(
    schema: scala.collection.immutable.ListMap[String, String],
    bins: Seq[String]
  ): Unit = {
    val schemaKeys = schema.keys.toSet
    val extraKeys = schemaKeys -- bins.toSet
    if (extraKeys.nonEmpty)
      throw new IllegalArgumentException(
        s"Schema override declares ${extraKeys.size} bin(s) not in the 'bins' filter: " +
          s"${extraKeys.mkString(", ")}. These bins will not be fetched from Aerospike " +
          "and will always be null. Remove them from the schema or add them to the bins filter."
      )
  }

  private[migrator] val DefaultPort = 3000
  private val DefaultSocketTimeoutMs = 10000
  private val DefaultTotalTimeoutMs = 30000

  /** Resolve socket timeout with a consistent default across schema discovery and data reading. */
  private def resolvedSocketTimeout(source: SourceSettings.Aerospike): Int =
    source.socketTimeoutMs.getOrElse(DefaultSocketTimeoutMs)

  /** Resolve total timeout with a consistent default across schema discovery and data reading. */
  private def resolvedTotalTimeout(source: SourceSettings.Aerospike): Int =
    source.totalTimeoutMs.getOrElse(
      source.socketTimeoutMs.map(_ * 3).getOrElse(DefaultTotalTimeoutMs)
    )

  /** Resolve credentials from environment variables, falling back to the given alternative. Used on
    * both the driver (with config credentials) and executors (with broadcast credentials).
    */
  private[migrator] def resolveCredentialsFromEnvOr(
    fallback: Option[(String, String)]
  ): Option[(String, String)] = {
    val fromEnv = for {
      user <- Option(System.getenv("AEROSPIKE_USERNAME"))
      pass <- Option(System.getenv("AEROSPIKE_PASSWORD"))
    } yield (user, pass)
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
  ): ClientPolicy = {
    val policy = new ClientPolicy()
    policy.readPolicyDefault.maxRetries          = 3
    policy.readPolicyDefault.sleepBetweenRetries = 500
    connConfig.connectTimeoutMs.foreach(t => policy.timeout = t)
    connConfig.socketTimeoutMs.foreach { t =>
      policy.readPolicyDefault.socketTimeout = t
      policy.scanPolicyDefault.socketTimeout = t
    }
    connConfig.totalTimeoutMs.foreach { t =>
      policy.readPolicyDefault.totalTimeout = t
      policy.scanPolicyDefault.totalTimeout = t
    }
    connConfig.tlsName.foreach { _ =>
      policy.tlsPolicy = new TlsPolicy()
    }
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
    val policy = buildClientPolicy(connConfig, credentials)
    val hostList = connConfig.hosts.flatMap { h =>
      Host.parseHosts(h, connConfig.port).map { parsed =>
        connConfig.tlsName.fold(parsed)(name => new Host(parsed.name, name, parsed.port))
      }
    }.toArray
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

  /** Run scanPartitions with a user-friendly error for unsupported Aerospike editions */
  private def tryScanPartitions(
    client: AerospikeClient,
    policy: ScanPolicy,
    filter: PartitionFilter,
    namespace: String,
    set: String,
    callback: ScanCallback,
    bins: Option[Seq[String]]
  ): Unit =
    try runScanPartitions(client, policy, filter, namespace, set, callback, bins)
    catch {
      case e: AerospikeException
          if e.getResultCode == ResultCode.PARAMETER_ERROR ||
            e.getResultCode == ResultCode.SERVER_NOT_AVAILABLE ||
            e.getResultCode == ResultCode.ROLE_VIOLATION =>
        throw new RuntimeException(
          "Partition scan failed. scanPartitions requires Aerospike Community Edition 6.0+ or Enterprise Edition. " +
            s"Check your Aerospike server version. Original error (code ${e.getResultCode}): ${e.getMessage}",
          e
        )
      case e: AerospikeException =>
        log.error(
          s"Partition scan failed with unexpected result code ${e.getResultCode}: ${e.getMessage}"
        )
        throw e
    }

  /** Check whether an AerospikeException represents a retryable error */
  private[migrator] def isRetryable(e: AerospikeException): Boolean =
    e.getResultCode == ResultCode.TIMEOUT ||
      e.getResultCode == ResultCode.SERVER_NOT_AVAILABLE ||
      e.getResultCode == ResultCode.DEVICE_OVERLOAD
}
