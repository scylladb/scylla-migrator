package com.scylladb.migrator

import com.datastax.spark.connector.cql.CassandraConnector
import com.scylladb.migrator.config.{
  MigratorConfig,
  SavepointTarget,
  SensitiveKeys,
  SparkSecretRedaction,
  TargetSettings
}
import io.circe.{ Json, JsonObject }
import io.circe.syntax._
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[migrator] case class SavepointCoordinates(epochMillis: Long, sequence: Long)

private[migrator] case class SavepointRecord(
  coordinates: SavepointCoordinates,
  reason: String,
  yaml: String
)

private[migrator] trait SavepointStore {
  def prepare(): Unit
  def seedState(): Option[SavepointCoordinates]
  def save(record: SavepointRecord): String
  def latestConfig(): MigratorConfig
}

private[migrator] object SavepointStore {
  private val RedactedValue = "<redacted>"

  def forConfig(
    config: MigratorConfig,
    sparkContext: Option[SparkContext],
    redactionRegex: Option[String] = None,
    hadoopConfiguration: Option[Configuration] = None
  ): SavepointStore =
    config.savepoints.resolvedTarget match {
      case target: SavepointTarget.StoragePathTarget =>
        file(
          target.storagePath,
          Some(configIdentitySha256(config)),
          Some(
            savepointHadoopConfiguration(
              config,
              sparkContext.map(_.hadoopConfiguration).orElse(hadoopConfiguration),
              redactionRegex.orElse(sparkContext.flatMap(SparkSecretRedaction.redactionRegex))
            )
          )
        )
      case _: SavepointTarget.TargetTable =>
        val scyllaTarget = config.target match {
          case target: TargetSettings.Scylla => target
          case other =>
            throw new IllegalArgumentException(
              s"savepoints.target.type: target-table requires target.type scylla, got ${other.getClass.getSimpleName}"
            )
        }
        val spark = sparkContext.getOrElse {
          throw new IllegalArgumentException(
            "savepoints.target.type: target-table requires a SparkContext to build the target connector"
          )
        }
        target(config, scyllaTarget, spark)
    }

  def file(config: MigratorConfig): SavepointStore =
    config.savepoints.resolvedTarget match {
      case target: SavepointTarget.StoragePathTarget =>
        file(
          target.storagePath,
          Some(configIdentitySha256(config)),
          Some(savepointHadoopConfiguration(config, None, None))
        )
      case _: SavepointTarget.TargetTable =>
        throw new IllegalArgumentException(
          "savepoints.target.type: target-table requires SavepointStore.forConfig with a " +
            "SparkContext, or an explicit SavepointStore."
        )
    }

  def file(path: String): SavepointStore =
    new FileSavepointStore(path)

  private def file(
    path: String,
    expectedConfigSha256: Option[String],
    hadoopConfiguration: Option[Configuration]
  ): SavepointStore =
    new FileSavepointStore(path, expectedConfigSha256, hadoopConfiguration)

  def target(
    config: MigratorConfig,
    target: TargetSettings.Scylla,
    sparkContext: SparkContext
  ): ScyllaTargetSavepointStore = {
    val savepointTarget = config.savepoints.resolvedTarget match {
      case targetTable: SavepointTarget.TargetTable => targetTable
      case other =>
        throw new IllegalArgumentException(
          s"savepoints.target.type must be target-table to build a target-table store, got ${other}"
        )
    }
    val keyspace = savepointTarget.keyspace.getOrElse(target.keyspace)
    val table = savepointTarget.table
    new ScyllaTargetSavepointStore(
      connector     = Connectors.targetConnector(sparkContext.getConf, target),
      keyspace      = keyspace,
      table         = table,
      jobId         = targetJobId(config),
      configSha256  = configIdentitySha256(config),
      schemaVersion = ScyllaTargetSavepointStore.SchemaVersion
    )
  }

  def targetJobId(config: MigratorConfig): String =
    config.savepoints.resolvedTarget match {
      case targetTable: SavepointTarget.TargetTable =>
        targetTable.jobId.getOrElse(s"auto-${configIdentitySha256(config).take(32)}")
      case _: SavepointTarget.StoragePathTarget =>
        s"auto-${configIdentitySha256(config).take(32)}"
    }

  def configIdentitySha256(config: MigratorConfig): String =
    sha256Hex(canonicalRedactedIdentity(config).noSpaces)

  private[migrator] def canonicalRedactedIdentity(config: MigratorConfig): Json = {
    val withoutVolatileFields =
      config.asJson.mapObject(
        _.remove("savepoints")
          .remove("validation")
          .remove("skipTokenRanges")
          .remove("skipSegments")
          .remove("skipParquetFiles")
      )
    canonicalJson(redactIdentity(withoutVolatileFields))
  }

  private def canonicalJson(json: Json): Json =
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(canonicalJson)),
      obj =>
        Json.fromJsonObject(
          JsonObject.fromIterable(
            obj.toIterable.toList
              .sortBy(_._1)
              .map { case (key, value) => key -> canonicalJson(value) }
          )
        )
    )

  private def redactIdentity(json: Json): Json =
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(redactIdentity)),
      obj =>
        Json.fromJsonObject(
          obj.toIterable.foldLeft(JsonObject.empty) { case (acc, (key, value)) =>
            val updatedValue =
              if (value.isString && SensitiveKeys.isSensitiveKey(key))
                Json.fromString(RedactedValue)
              else if (key == "where")
                // WHERE clauses affect the migrated dataset; digest them so the identity changes
                // without exposing the raw filter in the canonical identity JSON.
                value.asString
                  .map(where => Json.obj("sha256" -> sha256Hex(where).asJson))
                  .getOrElse(redactIdentity(value))
              else
                redactIdentity(value)
            acc.add(key, updatedValue)
          }
        )
    )

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(b => f"${b & 0xff}%02x").mkString
  }

  private[migrator] def savepointSortKey(name: String): Option[(Long, Long)] = {
    def reasonableSortKey(millis: Long, sequence: Long): Option[(Long, Long)] =
      if (SavepointsManager.isResumeSafeSortKey(millis, sequence))
        Some((millis, sequence))
      else None

    name match {
      case SavepointsManager.SavepointName(head, tailOrNull) =>
        try
          if (tailOrNull == null) {
            val millis = java.lang.Math.multiplyExact(java.lang.Long.parseLong(head), 1000L)
            reasonableSortKey(millis, -1L)
          } else {
            val millis = java.lang.Long.parseLong(head)
            val seq = java.lang.Long.parseLong(tailOrNull)
            reasonableSortKey(millis, seq)
          }
        catch {
          case _: NumberFormatException | _: ArithmeticException =>
            None
        }
      case _ =>
        None
    }
  }

  private[migrator] def savepointSortKey(path: java.nio.file.Path): Option[(Long, Long)] =
    savepointSortKey(path.getFileName.toString)

  private def savepointHadoopConfiguration(
    config: MigratorConfig,
    baseConfiguration: Option[Configuration],
    redactionRegex: Option[String]
  ): Configuration = {
    val conf = baseConfiguration.map(new Configuration(_)).getOrElse(new Configuration())

    config.savepoints.resolvedTarget match {
      case s3: SavepointTarget.S3 =>
        s3.region.foreach(conf.set("fs.s3a.endpoint.region", _))
        s3.credentials.foreach { configuredCredentials =>
          AwsUtils.computeFinalCredentials(Some(configuredCredentials), None, s3.region).foreach {
            credentials =>
              val credentialOptions =
                Seq(
                  "fs.s3a.access.key" -> credentials.accessKey,
                  "fs.s3a.secret.key" -> credentials.secretKey
                ) ++ credentials.maybeSessionToken.toSeq.map { sessionToken =>
                  "fs.s3a.session.token" -> sessionToken
                }

              ensureHadoopKeysRedacted(redactionRegex, credentialOptions.map(_._1))
              credentialOptions.foreach { case (key, value) => conf.set(key, value) }
              credentials.maybeSessionToken match {
                case Some(_) =>
                  conf.set("fs.s3a.aws.credentials.provider", S3ATemporaryCredentialsProvider)
                case None =>
                  conf.set("fs.s3a.aws.credentials.provider", S3ASimpleCredentialsProvider)
                  conf.unset("fs.s3a.session.token")
              }
          }
        }
      case gcs: SavepointTarget.GCS =>
        gcs.projectId.foreach(conf.set("fs.gs.project.id", _))
        gcs.credentials.foreach { credentials =>
          ensureHadoopKeysRedacted(
            redactionRegex,
            Seq("fs.gs.auth.service.account.json.keyfile")
          )
          conf.set("fs.gs.auth.type", GcsServiceAccountJsonKeyfileAuthType)
          conf.set(
            "fs.gs.auth.service.account.json.keyfile",
            credentials.serviceAccountJsonKeyfile
          )
        }
      case _ => ()
    }

    conf
  }

  private def ensureHadoopKeysRedacted(
    redactionRegex: Option[String],
    keys: Iterable[String]
  ): Unit =
    SparkSecretRedaction.ensureKeysRedacted(
      redactionRegex,
      keys,
      "savepoint Hadoop configuration"
    )

  private val S3ASimpleCredentialsProvider =
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  private val S3ATemporaryCredentialsProvider =
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"

  private val GcsServiceAccountJsonKeyfileAuthType = "SERVICE_ACCOUNT_JSON_KEYFILE"
}

private[migrator] class FileSavepointStore(
  path: String,
  expectedConfigSha256: Option[String] = None,
  hadoopConfiguration: Option[Configuration] = None
) extends SavepointStore {
  private val log = LogManager.getLogger(classOf[FileSavepointStore])
  private val pathIO = PathIO.forPath(path, hadoopConfiguration)

  override def prepare(): Unit =
    if (!pathIO.exists(path)) {
      log.debug(
        s"Directory ${pathIO.normalize(path)} does not exist. Creating it..."
      )
      pathIO.createDirectories(path)
    }

  override def seedState(): Option[SavepointCoordinates] = {
    if (!pathIO.exists(path)) return None

    var maxMillis = 0L
    var maxSeq = 0L
    try
      pathIO.listFileNames(path).foreach { name =>
        name match {
          case SavepointsManager.SavepointName(head, tailOrNull) =>
            val parsedCoordinates =
              try
                Some {
                  val millis =
                    if (tailOrNull == null)
                      java.lang.Math.multiplyExact(java.lang.Long.parseLong(head), 1000L)
                    else java.lang.Long.parseLong(head)
                  val seq =
                    if (tailOrNull == null) 0L
                    else java.lang.Long.parseLong(tailOrNull)
                  (millis, seq)
                }
              catch {
                case _: NumberFormatException | _: ArithmeticException => None
              }

            parsedCoordinates match {
              case None =>
                log.warn(
                  s"Ignoring hostile/corrupted savepoint filename ${name} during seed: " +
                    s"coordinates are not valid signed 64-bit integers."
                )
              case Some((millis, seq)) =>
                if (!SavepointsManager.isResumeSafeCoordinate(millis, seq)) {
                  log.warn(
                    s"Ignoring hostile/corrupted savepoint filename ${name} during seed " +
                      s"(millis=${millis}, seq=${seq} outside accepted bounds: " +
                      s"millis < ${SavepointsManager.MaxReasonableSeedValue}, " +
                      s"seq <= ${SavepointsManager.MaxSavepointSequenceValue}, and " +
                      s"max-sequence seeds must leave room for millisecond rollover)."
                  )
                } else if (millis > maxMillis || (millis == maxMillis && seq > maxSeq)) {
                  maxMillis = millis
                  maxSeq    = seq
                }
            }
          case _ => ()
        }
      }
    catch {
      case NonFatal(e) =>
        log.warn(
          s"Could not scan ${pathIO.normalize(path)} to seed savepoint state; " +
            s"falling back to clock-only ordering: ${e.getMessage}"
        )
        return None
    }

    if (maxMillis > 0L) Some(SavepointCoordinates(maxMillis, maxSeq))
    else None
  }

  override def save(record: SavepointRecord): String = {
    val finalPath =
      childPath(
        String.format(
          Locale.ROOT,
          "savepoint_%013d_%010d.yaml",
          java.lang.Long.valueOf(record.coordinates.epochMillis),
          java.lang.Long.valueOf(record.coordinates.sequence)
        )
      )

    pathIO.writeUtf8Atomically(finalPath, record.yaml.getBytes(StandardCharsets.UTF_8))
    pathIO.normalize(finalPath)
  }

  override def latestConfig(): MigratorConfig = {
    val candidates =
      if (!pathIO.exists(path)) Seq.empty
      else
        pathIO
          .listFileNames(path)
          .filter(name => name.startsWith("savepoint_") && name.endsWith(".yaml"))

    candidates
      .flatMap { name =>
        SavepointStore.savepointSortKey(name) match {
          case Some(sortKey) => Some(sortKey -> name)
          case None =>
            log.warn(
              s"Skipping savepoint file ${childPath(name)}: filename coordinates are invalid or unreasonable."
            )
            None
        }
      }
      .sortBy(_._1)
      .reverseIterator
      .map { case (_, savepointName) =>
        val savepoint = childPath(savepointName)
        try {
          val config = MigratorConfig.loadFromString(pathIO.readUtf8(savepoint))
          expectedConfigSha256 match {
            case None => Some(config)
            case Some(expected) =>
              val actual = SavepointStore.configIdentitySha256(config)
              if (actual == expected) Some(config)
              else {
                log.warn(
                  s"Skipping savepoint file ${savepoint}: config identity mismatch: " +
                    s"expected ${expected}, got ${actual}."
                )
                None
              }
          }
        } catch {
          case NonFatal(e) =>
            log.warn(
              s"Skipping invalid savepoint file ${savepoint}: ${Option(e.getMessage).getOrElse(e.toString)}"
            )
            None
        }
      }
      .collectFirst { case Some(config) => config }
      .getOrElse(
        throw new IllegalStateException(
          s"No valid file-backed savepoint found in ${pathIO.normalize(path)}"
        )
      )
  }

  private def childPath(name: String): String =
    if (path.endsWith("/")) s"${path}${name}"
    else s"${path}/${name}"
}

private[migrator] class ScyllaTargetSavepointStore(
  connector: CassandraConnector,
  keyspace: String,
  table: String,
  jobId: String,
  configSha256: String,
  schemaVersion: Int
) extends SavepointStore {
  import ScyllaTargetSavepointStore._

  private val log = LogManager.getLogger(classOf[ScyllaTargetSavepointStore])
  private val tableName = s"${cqlIdentifier(keyspace)}.${cqlIdentifier(table)}"

  override def prepare(): Unit =
    connector.withSessionDo { session =>
      session.execute(
        s"""CREATE TABLE IF NOT EXISTS ${tableName} (
           |  job_id text,
           |  epoch_millis bigint,
           |  sequence bigint,
           |  schema_version int,
           |  reason text,
           |  migrator_version text,
           |  config_sha256 text,
           |  config_yaml text,
           |  PRIMARY KEY ((job_id), epoch_millis, sequence)
           |) WITH CLUSTERING ORDER BY (epoch_millis DESC, sequence DESC)""".stripMargin
      )
    }

  override def seedState(): Option[SavepointCoordinates] =
    connector.withSessionDo { session =>
      val statement =
        session.prepare(
          s"SELECT epoch_millis, sequence, config_sha256 FROM ${tableName} WHERE job_id = ?"
        )
      val rows = session
        .execute(statement.bind(jobId))
        .iterator()
        .asScala
        .map(row =>
          StoredSavepointCoordinates(
            epochMillis  = row.getLong("epoch_millis"),
            sequence     = row.getLong("sequence"),
            configSha256 = Option(row.getString("config_sha256"))
          )
        )
        .toSeq

      newestCoordinates(rows, Some(configSha256), warnInvalidSeed)
    }

  override def save(record: SavepointRecord): String =
    connector.withSessionDo { session =>
      val statement =
        session.prepare(
          s"""INSERT INTO ${tableName} (
             |  job_id,
             |  epoch_millis,
             |  sequence,
             |  schema_version,
             |  reason,
             |  migrator_version,
             |  config_sha256,
             |  config_yaml
             |) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
        )
      session.execute(
        statement.bind(
          jobId,
          java.lang.Long.valueOf(record.coordinates.epochMillis),
          java.lang.Long.valueOf(record.coordinates.sequence),
          Integer.valueOf(schemaVersion),
          record.reason,
          BuildInfo.version,
          configSha256,
          record.yaml
        )
      )
      s"${tableName} job_id=${jobId} epoch_millis=${record.coordinates.epochMillis} sequence=${record.coordinates.sequence}"
    }

  override def latestConfig(): MigratorConfig =
    connector.withSessionDo { session =>
      val statement =
        session.prepare(
          s"SELECT epoch_millis, sequence, config_sha256, config_yaml FROM ${tableName} WHERE job_id = ?"
        )
      val rows = session
        .execute(statement.bind(jobId))
        .iterator()
        .asScala
        .map(row =>
          StoredSavepoint(
            epochMillis  = row.getLong("epoch_millis"),
            sequence     = row.getLong("sequence"),
            configYaml   = row.getString("config_yaml"),
            configSha256 = Option(row.getString("config_sha256"))
          )
        )
        .toSeq

      newestValidConfig(rows, configSha256, warnInvalidRow)
        .getOrElse(
          throw new IllegalStateException(
            s"No valid target-database savepoint found in ${tableName} for job_id=${jobId}"
          )
        )
    }

  private def warnInvalidRow(row: StoredSavepoint, error: Throwable): Unit =
    log.warn(
      s"Skipping invalid target-database savepoint in ${tableName} for job_id=${jobId} " +
        s"epoch_millis=${row.epochMillis} sequence=${row.sequence}: " +
        s"${Option(error.getMessage).getOrElse(error.toString)}"
    )

  private def warnInvalidSeed(row: StoredSavepointCoordinates, error: Throwable): Unit =
    log.warn(
      s"Skipping invalid target-database savepoint seed in ${tableName} for job_id=${jobId} " +
        s"epoch_millis=${row.epochMillis} sequence=${row.sequence}: " +
        s"${Option(error.getMessage).getOrElse(error.toString)}"
    )
}

private[migrator] object ScyllaTargetSavepointStore {
  val SchemaVersion: Int = 1

  private val UnquotedIdentifier = "^[A-Za-z][A-Za-z0-9_]*$".r
  private val QuotedIdentifier = "^\"((?:[^\"]|\"\")*)\"$".r

  private[migrator] case class StoredSavepoint(
    epochMillis: Long,
    sequence: Long,
    configYaml: String,
    configSha256: Option[String] = None
  ) {
    def coordinates: StoredSavepointCoordinates =
      StoredSavepointCoordinates(epochMillis, sequence, configSha256)
  }

  private[migrator] case class StoredSavepointCoordinates(
    epochMillis: Long,
    sequence: Long,
    configSha256: Option[String]
  )

  private[migrator] def newestValidConfig(
    rows: Iterable[StoredSavepoint],
    warnInvalidRow: (StoredSavepoint, Throwable) => Unit
  ): Option[MigratorConfig] =
    newestValidConfig(rows, None, warnInvalidRow)

  private[migrator] def newestValidConfig(
    rows: Iterable[StoredSavepoint],
    expectedConfigSha256: String,
    warnInvalidRow: (StoredSavepoint, Throwable) => Unit
  ): Option[MigratorConfig] =
    newestValidConfig(rows, Some(expectedConfigSha256), warnInvalidRow)

  private def newestValidConfig(
    rows: Iterable[StoredSavepoint],
    expectedConfigSha256: Option[String],
    warnInvalidRow: (StoredSavepoint, Throwable) => Unit
  ): Option[MigratorConfig] =
    rows.toSeq
      .filter(row => reasonableCoordinates(row.coordinates, warnInvalidRowFor(row, warnInvalidRow)))
      .filter(row =>
        matchesExpectedIdentity(
          row.coordinates,
          expectedConfigSha256,
          warnInvalidRowFor(row, warnInvalidRow)
        )
      )
      .sortBy(row => (row.epochMillis, row.sequence))
      .reverseIterator
      .map { row =>
        try {
          val config = MigratorConfig.loadFromString(row.configYaml)
          expectedConfigSha256 match {
            case None => Some(config)
            case Some(expected) =>
              val actual = SavepointStore.configIdentitySha256(config)
              if (actual == expected) Some(config)
              else {
                warnInvalidRow(
                  row,
                  new IllegalArgumentException(
                    s"config_yaml identity mismatch: expected ${expected}, got ${actual}"
                  )
                )
                None
              }
          }
        } catch {
          case NonFatal(e) =>
            warnInvalidRow(row, e)
            None
        }
      }
      .collectFirst { case Some(config) => config }

  private[migrator] def newestCoordinates(
    rows: Iterable[StoredSavepointCoordinates],
    expectedConfigSha256: Option[String],
    warnInvalidRow: (StoredSavepointCoordinates, Throwable) => Unit
  ): Option[SavepointCoordinates] =
    rows.toSeq
      .filter(row => reasonableCoordinates(row, warnInvalidRow))
      .filter(row => matchesExpectedIdentity(row, expectedConfigSha256, warnInvalidRow))
      .sortBy(row => (row.epochMillis, row.sequence))
      .lastOption
      .map(row => SavepointCoordinates(row.epochMillis, row.sequence))

  private def warnInvalidRowFor(
    row: StoredSavepoint,
    warnInvalidRow: (StoredSavepoint, Throwable) => Unit
  ): (StoredSavepointCoordinates, Throwable) => Unit =
    (_, error) => warnInvalidRow(row, error)

  private def reasonableCoordinates(
    row: StoredSavepointCoordinates,
    warnInvalidRow: (StoredSavepointCoordinates, Throwable) => Unit
  ): Boolean = {
    val reasonable =
      SavepointsManager.isResumeSafeCoordinate(row.epochMillis, row.sequence)
    if (!reasonable)
      warnInvalidRow(
        row,
        new IllegalArgumentException(
          s"unreasonable savepoint coordinates: epoch_millis=${row.epochMillis}, " +
            s"sequence=${row.sequence}; coordinates must be within bounds and max-sequence " +
            s"values must leave room for millisecond rollover"
        )
      )
    reasonable
  }

  private def matchesExpectedIdentity(
    row: StoredSavepointCoordinates,
    expectedConfigSha256: Option[String],
    warnInvalidRow: (StoredSavepointCoordinates, Throwable) => Unit
  ): Boolean =
    expectedConfigSha256 match {
      case None => true
      case Some(expected) =>
        row.configSha256 match {
          case Some(actual) if actual == expected => true
          case Some(actual) =>
            warnInvalidRow(
              row,
              new IllegalArgumentException(
                s"config_sha256 mismatch: expected ${expected}, got ${actual}"
              )
            )
            false
          case None =>
            warnInvalidRow(
              row,
              new IllegalArgumentException(s"missing config_sha256; expected ${expected}")
            )
            false
        }
    }

  private def cqlIdentifier(raw: String): String =
    raw match {
      case QuotedIdentifier(_) => raw
      case UnquotedIdentifier() =>
        raw.toLowerCase(Locale.ROOT)
      case other =>
        "\"" + other.replace("\"", "\"\"") + "\""
    }
}
