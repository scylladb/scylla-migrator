package com.scylladb.migrator

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel.LOCAL_QUORUM
import com.scylladb.migrator.config.{
  MigratorConfig,
  SavepointTarget,
  SparkSecretRedaction,
  TargetSettings
}
import io.circe.{ Json, JsonObject }
import io.circe.syntax._
import io.circe.yaml.syntax._
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
}

private[migrator] object SavepointStore {
  import MigratorConfig.RedactedValue

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
          Some(
            SavepointStorage.hadoopConfiguration(
              config,
              sparkContext.map(_.hadoopConfiguration).orElse(hadoopConfiguration),
              redactionRegex.orElse(sparkContext.flatMap(SparkSecretRedaction.redactionRegex))
            )
          )
        )
      case _: SavepointTarget.TargetTable => target(config, sparkContext)
    }

  def file(config: MigratorConfig): SavepointStore =
    config.savepoints.resolvedTarget match {
      case target: SavepointTarget.StoragePathTarget =>
        file(
          target.storagePath,
          Some(SavepointStorage.hadoopConfiguration(config, None, None))
        )
      case _: SavepointTarget.TargetTable =>
        throw new IllegalArgumentException(
          "savepoints.target.type: target-table requires SavepointStore.forConfig with a " +
            "SparkContext, or an explicit SavepointStore."
        )
    }

  private def file(
    path: String,
    hadoopConfiguration: Option[Configuration]
  ): SavepointStore = new FileSavepointStore(path, hadoopConfiguration)

  def target(
    config: MigratorConfig,
    sparkContext: Option[SparkContext]
  ): ScyllaTargetSavepointStore = {
    val target = config.target match {
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
    val savepointTarget = config.savepoints.resolvedTarget match {
      case targetTable: SavepointTarget.TargetTable => targetTable
      case other =>
        throw new IllegalArgumentException(
          s"savepoints.target.type must be target-table to build a target-table store, got ${other}"
        )
    }
    // The connector treats target.keyspace as an exact internal name, not unquoted CQL.
    val keyspace = savepointTarget.keyspace.getOrElse(
      com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(target.keyspace).asCql(true)
    )
    val table = savepointTarget.table
    new ScyllaTargetSavepointStore(
      connector     = Connectors.targetConnector(spark.getConf, target),
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
    val withWhereIdentity = sourceWhere(withoutVolatileFields).fold(withoutVolatileFields) { value =>
      withWhereDigest(withoutVolatileFields, sha256Hex(value))
    }
    canonicalJson(MigratorConfig.redactSecrets(withWhereIdentity))
  }

  // WHERE affects dataset identity but is redacted in persisted configurations. Keep only its
  // digest alongside table payloads so validation still checks the payload, not just row metadata.
  private val WhereDigestField = "_savepointWhereSha256"

  def renderSavepoint(config: MigratorConfig): String =
    config.savepoints.resolvedTarget match {
      case _: SavepointTarget.TargetTable =>
        val where = sourceWhere(config.asJson)
        val redacted = MigratorConfig.redactSecrets(config.asJson)
        where
          .fold(redacted)(value =>
            redacted.mapObject(_.add(WhereDigestField, sha256Hex(value).asJson))
          )
          .asYaml
          .spaces2
      case _ => config.renderRedacted
    }

  private[migrator] def payloadIdentitySha256(yaml: String, config: MigratorConfig): String = {
    val json = io.circe.yaml.parser.parse(yaml).fold(throw _, identity)
    val digest = json.hcursor.get[Option[String]](WhereDigestField).fold(throw _, identity)
    val canonical = canonicalRedactedIdentity(config)
    val withWhereIdentity = digest match {
      case None => canonical // Older payloads with an unredacted WHERE remain verifiable.
      case Some(value) =>
        require(value.matches("[0-9a-f]{64}"), "Invalid savepoint WHERE digest")
        require(
          sourceWhere(json).contains(RedactedValue),
          "Savepoint WHERE digest requires a redacted WHERE"
        )
        withWhereDigest(canonical, value)
    }
    sha256Hex(canonicalJson(withWhereIdentity).noSpaces)
  }

  private def sourceWhere(json: Json): Option[String] =
    json.hcursor.downField("source").get[Option[String]]("where").toOption.flatten

  private def withWhereDigest(json: Json, digest: String): Json =
    json.mapObject { obj =>
      obj.add(
        "source",
        obj("source").get.mapObject(
          _.add("where", Json.obj("sha256" -> digest.asJson))
        )
      )
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

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(b => f"${b & 0xff}%02x").mkString
  }

}

private[migrator] class FileSavepointStore(
  path: String,
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
        SavepointsManager.savepointSortKey(name) match {
          case Some((millis, seq)) =>
            if (millis > maxMillis || (millis == maxMillis && seq > maxSeq)) {
              maxMillis = millis
              maxSeq    = seq
            }
          case None if name.startsWith("savepoint_") && name.endsWith(".yaml") =>
            log.warn(s"Ignoring invalid or unsafe savepoint filename ${name} during seed.")
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
          s"SELECT epoch_millis, sequence, config_sha256 FROM ${tableName} WHERE job_id = ? ORDER BY epoch_millis DESC, sequence DESC"
        )
      val rows = session
        .execute(statement.bind(jobId).setConsistencyLevel(LOCAL_QUORUM))
        .iterator()
        .asScala
        .map(row =>
          StoredSavepointCoordinates(
            epochMillis  = row.getLong("epoch_millis"),
            sequence     = row.getLong("sequence"),
            configSha256 = Option(row.getString("config_sha256"))
          )
        )

      newestCoordinates(rows, configSha256, warnInvalidSeed)
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
        statement
          .bind(
            jobId,
            java.lang.Long.valueOf(record.coordinates.epochMillis),
            java.lang.Long.valueOf(record.coordinates.sequence),
            Integer.valueOf(schemaVersion),
            record.reason,
            BuildInfo.version,
            configSha256,
            record.yaml
          )
          .setConsistencyLevel(LOCAL_QUORUM)
      )
      s"${tableName} job_id=${jobId} epoch_millis=${record.coordinates.epochMillis} sequence=${record.coordinates.sequence}"
    }

  def latestConfig(): MigratorConfig =
    connector.withSessionDo { session =>
      val statement =
        session.prepare(
          s"SELECT epoch_millis, sequence, config_sha256, config_yaml FROM ${tableName} WHERE job_id = ? ORDER BY epoch_millis DESC, sequence DESC"
        )
      val rows = session
        .execute(statement.bind(jobId).setConsistencyLevel(LOCAL_QUORUM))
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

      newestValidConfig(rows, configSha256, warnInvalidRow)
        .getOrElse(
          throw new IllegalStateException(
            s"No valid target-table savepoint found in ${tableName} for job_id=${jobId}"
          )
        )
    }

  private def warnInvalidRow(row: StoredSavepoint, error: Throwable): Unit =
    log.warn(
      s"Skipping invalid target-table savepoint in ${tableName} for job_id=${jobId} " +
        s"epoch_millis=${row.epochMillis} sequence=${row.sequence}: " +
        s"${Option(error.getMessage).getOrElse(error.toString)}"
    )

  private def warnInvalidSeed(row: StoredSavepointCoordinates, error: Throwable): Unit =
    log.warn(
      s"Skipping invalid target-table savepoint seed in ${tableName} for job_id=${jobId} " +
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

  // Input is in descending clustering order, as returned by the target-table query.
  private[migrator] def newestValidConfig(
    rows: IterableOnce[StoredSavepoint],
    expectedConfigSha256: String,
    warnInvalidRow: (StoredSavepoint, Throwable) => Unit
  ): Option[MigratorConfig] =
    rows.iterator
      .filter(row => reasonableCoordinates(row.coordinates, warnInvalidRowFor(row, warnInvalidRow)))
      .filter(row =>
        matchesExpectedIdentity(
          row.coordinates,
          expectedConfigSha256,
          warnInvalidRowFor(row, warnInvalidRow)
        )
      )
      .map { row =>
        try {
          val config = MigratorConfig.loadFromString(row.configYaml)
          val actual = SavepointStore.payloadIdentitySha256(row.configYaml, config)
          if (actual == expectedConfigSha256) Some(config)
          else {
            warnInvalidRow(
              row,
              new IllegalArgumentException(
                s"config_yaml identity mismatch: expected ${expectedConfigSha256}, got ${actual}."
              )
            )
            None
          }
        } catch {
          case NonFatal(e) =>
            warnInvalidRow(row, e)
            None
        }
      }
      .collectFirst { case Some(config) => config }

  private[migrator] def newestCoordinates(
    rows: IterableOnce[StoredSavepointCoordinates],
    expectedConfigSha256: String,
    warnInvalidRow: (StoredSavepointCoordinates, Throwable) => Unit
  ): Option[SavepointCoordinates] =
    rows.iterator
      .filter(row => reasonableCoordinates(row, warnInvalidRow))
      .filter(row => matchesExpectedIdentity(row, expectedConfigSha256, warnInvalidRow))
      .nextOption()
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
    expectedConfigSha256: String,
    warnInvalidRow: (StoredSavepointCoordinates, Throwable) => Unit
  ): Boolean =
    row.configSha256 match {
      case Some(actual) if actual == expectedConfigSha256 => true
      case actual =>
        warnInvalidRow(
          row,
          new IllegalArgumentException(
            s"config_sha256 mismatch: expected ${expectedConfigSha256}, got ${actual.getOrElse("missing")}."
          )
        )
        false
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
