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
  *   job is restarted or savepoints are resumed.
  * @param resumeFromLatest
  *   When `true`, load the newest valid savepoint from the configured backend before migration.
  * @param target
  *   Savepoint storage target. Missing target defaults to `filesystem` using `path`.
  *
  * Set this to `false` to keep the legacy behavior where Parquet files are not tracked in
  * savepoints. Disabling tracking may be useful for backwards compatibility with older savepoints
  * or when file tracking is handled by an external mechanism, but it means repeated runs may
  * reprocess the same Parquet files.
  */
case class Savepoints(
  intervalSeconds: Int = 300,
  path: String = Savepoints.DefaultPath,
  enableParquetFileTracking: Boolean = true,
  resumeFromLatest: Boolean = false,
  target: Option[SavepointTarget] = None
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
          nonEmpty(value, "serviceAccountJsonKeyfile", cursor).map(_ =>
            GCSCredentials(value.trim)
          )
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

  private[config] def decoderWithFilesystemDefault(
    filesystemDefaultPath: String
  ): Decoder[SavepointTarget] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "filesystem" | "path" =>
          for {
            path <- cursor.getOrElse[String]("path")(filesystemDefaultPath)
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
        case "target-table" | "target-database" =>
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
              s"Invalid savepoints.target.type '${otherwise}'. Valid values: filesystem, s3, gcs, target-table",
              cursor.history
            )
          )
      }
    }

  implicit val decoder: Decoder[SavepointTarget] =
    decoderWithFilesystemDefault(Savepoints.DefaultPath)

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
      case TargetTable(keyspace, table, jobId) =>
        JsonObject(
          "type"     -> "target-table".asJson,
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
  val DefaultPath: String = "/app/savepoints"
  val DefaultTargetTable: String = "scylla_migrator_savepoints"
  val Default: Savepoints = Savepoints(intervalSeconds = 300, path = DefaultPath)

  implicit val decoder: Decoder[Savepoints] =
    Decoder.instance { cursor =>
      val targetCursor = cursor
        .downField("target")
        .success
        .filter(_.focus.exists(!_.isNull))

      val filesystemTargetWithoutOwnPath =
        targetCursor.exists { target =>
          val targetType = target.get[String]("type").toOption
          val isFilesystem = targetType.exists(t => t == "filesystem" || t == "path")
          val hasOwnPath = target.downField("path").success.exists(_.focus.exists(!_.isNull))
          isFilesystem && !hasOwnPath
        }

      for {
        intervalSeconds <- cursor.getOrElse[Int]("intervalSeconds")(300)
        maybePath       <- cursor.get[Option[String]]("path")
        _ <- Either.cond(
               maybePath.isEmpty || targetCursor.isEmpty || filesystemTargetWithoutOwnPath,
               (),
               DecodingFailure(
                 "Specify only one of savepoints.path or savepoints.target.",
                 cursor.history
               )
             )
        path =
          maybePath
            .filter(_.trim.nonEmpty)
            .getOrElse(DefaultPath)
        enableParquetFileTracking <-
          cursor.getOrElse[Boolean]("enableParquetFileTracking")(true)
        resumeFromLatest <- cursor.getOrElse[Boolean]("resumeFromLatest")(false)
        target <- targetCursor match {
                    case Some(cursor) =>
                      SavepointTarget
                        .decoderWithFilesystemDefault(path)
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
        resumeFromLatest,
        target
      )
    }

  implicit val encoder: Encoder[Savepoints] =
    Encoder.instance { savepoints =>
      JsonObject(
        "intervalSeconds"           -> savepoints.intervalSeconds.asJson,
        "path"                      -> savepoints.path.asJson,
        "enableParquetFileTracking" -> savepoints.enableParquetFileTracking.asJson,
        "resumeFromLatest"          -> savepoints.resumeFromLatest.asJson,
        "target"                    -> savepoints.target.asJson
      ).filter { case (_, value) => !value.isNull }.asJson
    }

  private[config] def validate(savepoints: Savepoints): List[String] = {
    val errors = List.newBuilder[String]
    if (savepoints.intervalSeconds <= 0)
      errors += s"'intervalSeconds' must be positive, got ${savepoints.intervalSeconds}."
    savepoints.resolvedTarget match {
      case SavepointTarget.Filesystem(path) =>
        if (path.trim.isEmpty) errors += "'target.path' must not be empty."
      case s3: SavepointTarget.S3 =>
        if (s3.bucket.trim.isEmpty) errors += "'target.bucket' must not be empty."
      case gcs: SavepointTarget.GCS =>
        if (gcs.bucket.trim.isEmpty) errors += "'target.bucket' must not be empty."
      case SavepointTarget.TargetTable(keyspace, table, jobId) =>
        if (keyspace.exists(_.trim.isEmpty))
          errors += "'target.keyspace' must not be empty when set."
        if (table.trim.isEmpty)
          errors += "'target.table' must not be empty."
        if (jobId.exists(_.trim.isEmpty))
          errors += "'target.jobId' must not be empty when set."
    }
    errors.result()
  }
}
