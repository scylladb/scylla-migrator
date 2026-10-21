package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, Json, JsonObject }
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._

import java.util.Locale

/** Configuration for periodic savepoints written during a migration run.
  *
  * @param intervalSeconds
  *   How often, in seconds, a savepoint should be written.
  * @param path
  *   Filesystem path or Hadoop filesystem URI where file-backed savepoint data will be stored.
  * @param enableParquetFileTracking
  *   When `true` (the default), enables tracking of already-processed Parquet files as part of the
  *   savepoint state. This prevents the same Parquet file from being migrated more than once if the
  *   job is restarted or savepoints are resumed. Set this to `false` to keep the legacy behavior
  *   where Parquet files are not tracked in savepoints. Disabling tracking may be useful for
  *   backwards compatibility with older savepoints or when file tracking is handled by an external
  *   mechanism, but it means repeated runs may reprocess the same Parquet files.
  * @param target
  *   Savepoint storage target. Missing target defaults to `filesystem` using `path`.
  * @param autoResume
  *   When `true` (the default), merge saved progress into the launch configuration. Lookup failures
  *   warn and leave the launch configuration unchanged.
  */
case class Savepoints(
  intervalSeconds: Int = Savepoints.DefaultIntervalSeconds,
  path: String = Savepoints.DefaultPath,
  enableParquetFileTracking: Boolean = true,
  target: Option[SavepointTarget] = None,
  autoResume: Boolean = true
) {
  def resolvedTarget: SavepointTarget =
    target.getOrElse(SavepointTarget.Filesystem(path))

  def effectiveTarget: SavepointTarget =
    resolvedTarget

  def storagePath: String =
    resolvedTarget match {
      case target: SavepointTarget.StoragePathTarget => target.storagePath
      case _: SavepointTarget.TargetTable            => path
    }

  def filesystemPath: String = storagePath
}

sealed trait SavepointTarget {
  def targetType: String
}

object SavepointTarget {
  private[config] def isTableType(value: String): Boolean =
    value == "target-table" || value == "target-database"

  private val UriScheme = """^([A-Za-z][A-Za-z0-9+.-]*):/{2}.*""".r

  sealed trait StoragePathTarget extends SavepointTarget {
    def storagePath: String
  }

  case class Filesystem(path: String = Savepoints.DefaultPath) extends StoragePathTarget {
    override val targetType: String = "filesystem"
    override def storagePath: String = path
  }

  case class S3(
    bucket: String,
    prefix: Option[String] = None,
    region: Option[String] = None,
    credentials: Option[AWSCredentials] = None
  ) extends StoragePathTarget {
    override val targetType: String = "s3"

    override def storagePath: String = {
      val normalizedPrefix = prefix
        .map(_.stripPrefix("/").stripSuffix("/"))
        .filter(_.nonEmpty)

      normalizedPrefix match {
        case Some(p) => s"s3a://${bucket}/${p}"
        case None    => s"s3a://${bucket}"
      }
    }
  }

  case class GCSCredentials(serviceAccountJsonKeyfile: String)

  object GCSCredentials {
    implicit val decoder: Decoder[GCSCredentials] =
      Decoder.instance { cursor =>
        val keyfile =
          cursor
            .get[Option[String]]("serviceAccountJsonKeyfile")
            .flatMap {
              case Some(value) => Right(value)
              case None        => cursor.get[String]("keyfile")
            }

        keyfile.flatMap { value =>
          nonEmpty(value, "serviceAccountJsonKeyfile", cursor).map(_ => GCSCredentials(value.trim))
        }
      }

    implicit val encoder: Encoder[GCSCredentials] = deriveEncoder
  }

  case class GCS(
    bucket: String,
    prefix: Option[String] = None,
    projectId: Option[String] = None,
    credentials: Option[GCSCredentials] = None
  ) extends StoragePathTarget {
    override val targetType: String = "gcs"

    override def storagePath: String = {
      val normalizedPrefix = prefix
        .map(_.stripPrefix("/").stripSuffix("/"))
        .filter(_.nonEmpty)

      normalizedPrefix match {
        case Some(p) => s"gs://${bucket}/${p}"
        case None    => s"gs://${bucket}"
      }
    }
  }

  case class TargetTable(
    keyspace: Option[String] = None,
    table: String = Savepoints.DefaultTargetTable,
    jobId: Option[String] = None
  ) extends SavepointTarget {
    override val targetType: String = "target-table"
  }

  private def nonEmpty(value: String, field: String, cursor: io.circe.HCursor) =
    Either.cond(
      value.trim.nonEmpty,
      (),
      DecodingFailure(s"Savepoints target '$field' must not be empty", cursor.history)
    )

  private def scheme(path: String): Option[String] =
    path match {
      case UriScheme(rawScheme) => Some(rawScheme.toLowerCase(Locale.ROOT))
      case _ if path.regionMatches(true, 0, "file:", 0, "file:".length) => Some("file")
      case _                                                            => None
    }

  private def localFilesystemPath(path: String, cursor: io.circe.HCursor) =
    scheme(path) match {
      case Some(pathScheme) if pathScheme != "file" =>
        Left(
          DecodingFailure(
            s"Savepoints target type 'filesystem' accepts only local filesystem paths, got '${pathScheme}://'. " +
              "Use savepoints.path for raw Hadoop URI paths, or use target type 's3' or 'gcs'.",
            cursor.history
          )
        )
      case _ => Right(())
    }

  implicit val decoder: Decoder[SavepointTarget] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "filesystem" | "path" =>
          for {
            path <- cursor.get[String]("path")
            _    <- nonEmpty(path, "path", cursor)
            _    <- localFilesystemPath(path, cursor)
          } yield Filesystem(path)
        case "s3" | "aws-s3" | "aws-bucket" =>
          for {
            bucket      <- cursor.get[String]("bucket")
            prefix      <- cursor.get[Option[String]]("prefix")
            region      <- cursor.get[Option[String]]("region")
            credentials <- cursor.get[Option[AWSCredentials]]("credentials")
            _           <- nonEmpty(bucket, "bucket", cursor)
          } yield S3(bucket.trim, prefix, region, credentials)
        case "gcs" | "gcp" | "google-cloud-storage" | "google-gcs" =>
          for {
            bucket      <- cursor.get[String]("bucket")
            prefix      <- cursor.get[Option[String]]("prefix")
            projectId   <- cursor.get[Option[String]]("projectId")
            credentials <- cursor.get[Option[GCSCredentials]]("credentials")
            _           <- nonEmpty(bucket, "bucket", cursor)
          } yield GCS(bucket.trim, prefix, projectId, credentials)
        case tableType if isTableType(tableType) =>
          for {
            keyspace <- cursor.get[Option[String]]("keyspace")
            table    <- cursor.getOrElse[String]("table")(Savepoints.DefaultTargetTable)
            jobId    <- cursor.get[Option[String]]("jobId")
          } yield TargetTable(keyspace, table, jobId)
        case "hadoop" =>
          Left(
            DecodingFailure(
              "Savepoints target type 'hadoop' is not supported. Use savepoints.path for raw " +
                "Hadoop URI paths, or use target type 's3' or 'gcs'.",
              cursor.history
            )
          )
        case otherwise =>
          Left(
            DecodingFailure(
              s"Invalid savepoints.target.type '${otherwise}'. Canonical target types: filesystem, s3, gcs, target-table",
              cursor.history
            )
          )
      }
    }

  implicit val encoder: Encoder[SavepointTarget] =
    Encoder.instance {
      case t: Filesystem =>
        JsonObject(
          "type" -> t.targetType.asJson,
          "path" -> t.path.asJson
        ).asJson
      case t: S3 =>
        deriveEncoder[S3]
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString(t.targetType))
          .asJson
      case t: GCS =>
        deriveEncoder[GCS]
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString(t.targetType))
          .asJson
      case target @ TargetTable(keyspace, table, jobId) =>
        JsonObject(
          "type"     -> target.targetType.asJson,
          "keyspace" -> keyspace.asJson,
          "table"    -> table.asJson,
          "jobId"    -> jobId.asJson
        ).filter { case (_, value) => !value.isNull }.asJson
    }
}

object SavepointsTarget {
  type StoragePathTarget = SavepointTarget.StoragePathTarget
  type Filesystem = SavepointTarget.Filesystem
  type S3 = SavepointTarget.S3
  type GCSCredentials = SavepointTarget.GCSCredentials
  type GCS = SavepointTarget.GCS
  type TargetTable = SavepointTarget.TargetTable

  val Filesystem = SavepointTarget.Filesystem
  val S3 = SavepointTarget.S3
  val GCSCredentials = SavepointTarget.GCSCredentials
  val GCS = SavepointTarget.GCS
  val TargetTable = SavepointTarget.TargetTable
}

object Savepoints {
  val DefaultIntervalSeconds: Int = 300
  val DefaultPath: String = "/app/savepoints"
  val DefaultTargetTable: String = "scylla_migrator_savepoints"
  val Default: Savepoints = Savepoints()

  implicit val decoder: Decoder[Savepoints] =
    Decoder.instance { cursor =>
      val targetCursor = cursor
        .downField("target")
        .success
        .filter(_.focus.exists(!_.isNull))

      val tableTarget = targetCursor.exists(
        _.get[String]("type").toOption.exists(SavepointTarget.isTableType)
      )

      for {
        intervalSeconds <- if (tableTarget)
                             cursor.getOrElse[Int]("intervalSeconds")(DefaultIntervalSeconds)
                           else cursor.get[Int]("intervalSeconds")
        maybePath <- cursor.get[Option[String]]("path")
        _ <- Either.cond(
               maybePath.isEmpty || targetCursor.isEmpty,
               (),
               DecodingFailure(
                 "Specify only one of savepoints.path or savepoints.target.",
                 cursor.history
               )
             )
        _ <- Either.cond(
               maybePath.forall(_.trim.nonEmpty) &&
                 (maybePath.isDefined || targetCursor.isDefined),
               (),
               DecodingFailure(
                 "Savepoints configuration requires an explicit nonempty path or target destination.",
                 cursor.history
               )
             )
        path = maybePath.getOrElse(DefaultPath)
        enableParquetFileTracking <-
          cursor.getOrElse[Boolean]("enableParquetFileTracking")(true)
        autoResume <- cursor.getOrElse[Boolean]("autoResume")(true)
        target <- targetCursor match {
                    case Some(cursor) =>
                      SavepointTarget.decoder
                        .apply(cursor)
                        .map(Some(_))
                    case _ => Right(None)
                  }
        resolvedPath = target match {
                         case Some(target: SavepointTarget.StoragePathTarget) =>
                           target.storagePath
                         case _ => path
                       }
      } yield Savepoints(
        intervalSeconds,
        resolvedPath,
        enableParquetFileTracking,
        target,
        autoResume
      )
    }

  implicit val encoder: Encoder[Savepoints] =
    Encoder.instance { savepoints =>
      val targetField =
        savepoints.target match {
          case Some(target) => List("target" -> target.asJson)
          case None         => List("path" -> savepoints.path.asJson)
        }

      // Only emit `autoResume` when it diverges from the default so that configurations that do
      // not set it (the vast majority) round-trip byte-for-byte as before.
      val autoResumeField =
        if (savepoints.autoResume) Nil
        else List("autoResume" -> savepoints.autoResume.asJson)

      Json.obj(
        (
          List("intervalSeconds" -> savepoints.intervalSeconds.asJson) ++ targetField ++ List(
            "enableParquetFileTracking" -> savepoints.enableParquetFileTracking.asJson
          ) ++ autoResumeField
        ): _*
      )
    }

  // Keyspace/table restrictions are narrower than generic quoted CQL identifiers.
  private def validMetadataName(name: String): Boolean =
    name.matches("[A-Za-z0-9_]+|\"[A-Za-z0-9_]+\"")

  private[config] def validate(savepoints: Savepoints): List[String] = {
    val errors = List.newBuilder[String]
    if (savepoints.intervalSeconds <= 0)
      errors += s"'intervalSeconds' must be positive, got ${savepoints.intervalSeconds}."
    savepoints.resolvedTarget match {
      case SavepointTarget.TargetTable(keyspace, table, jobId) =>
        if (keyspace.exists(_.trim.isEmpty))
          errors += "'target.keyspace' must not be empty when set."
        if (table.trim.isEmpty)
          errors += "'target.table' must not be empty."
        keyspace.filter(_.trim.nonEmpty).foreach { name =>
          if (!validMetadataName(name))
            errors += "'target.keyspace' must contain only ASCII letters, digits, or underscores, optionally enclosed in CQL double quotes."
        }
        if (table.trim.nonEmpty && !validMetadataName(table))
          errors += "'target.table' must contain only ASCII letters, digits, or underscores, optionally enclosed in CQL double quotes."
        if (jobId.exists(_.trim.isEmpty))
          errors += "'target.jobId' must not be empty when set."
      case _: SavepointTarget.StoragePathTarget => ()
    }
    errors.result()
  }
}
