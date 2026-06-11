package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._

import java.util.Locale

sealed trait SavepointsTarget {
  def targetType: String
}

object SavepointsTarget {
  private val UriScheme = """^([A-Za-z][A-Za-z0-9+.-]*):/{2}.*""".r

  sealed trait StoragePathTarget extends SavepointsTarget {
    def storagePath: String
  }

  case class Filesystem(path: String) extends StoragePathTarget {
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

  implicit val decoder: Decoder[SavepointsTarget] =
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
        case "hadoop" =>
          Left(
            DecodingFailure(
              "Savepoints target type 'hadoop' is not supported. Use savepoints.path for raw " +
                "Hadoop URI paths, or use target type 's3' or 'gcs'.",
              cursor.history
            )
          )
        case "target-table" | "target-database" =>
          Left(
            DecodingFailure(
              "Savepoints target type 'target-table' is not supported by this runtime. " +
                "Use a local filesystem, savepoints.path Hadoop URI, S3, or GCS savepoint target.",
              cursor.history
            )
          )
        case otherwise =>
          Left(
            DecodingFailure(
              s"Invalid savepoints target type: ${otherwise}",
              cursor.history
            )
          )
      }
    }

  implicit val encoder: Encoder[SavepointsTarget] =
    Encoder.instance {
      case t: Filesystem =>
        deriveEncoder[Filesystem]
          .encodeObject(t)
          .add("type", Json.fromString(t.targetType))
          .asJson
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
    }
}

/** Configuration for periodic savepoints written during a migration run.
  *
  * @param intervalSeconds
  *   How often, in seconds, a savepoint should be written.
  * @param path
  *   Local filesystem path or Hadoop filesystem URI (directory or prefix) where savepoint data will
  *   be stored.
  * @param enableParquetFileTracking
  *   When `true` (the default), enables tracking of already-processed Parquet files as part of the
  *   savepoint state. This prevents the same Parquet file from being migrated more than once if the
  *   job is restarted or savepoints are resumed.
  *
  * Set this to `false` to keep the legacy behavior where Parquet files are not tracked in
  * savepoints. Disabling tracking may be useful for backwards compatibility with older savepoints
  * or when file tracking is handled by an external mechanism, but it means repeated runs may
  * reprocess the same Parquet files.
  * @param target
  *   Optional typed savepoint destination. When omitted, the legacy `path` field is interpreted as
  *   a filesystem/Hadoop URI destination.
  * @param autoResume
  *   When `true` (the default), the migrator scans this savepoints location on startup and merges
  *   the skip-state (`skipTokenRanges`/`skipSegments`/`skipParquetFiles`) of the most recent
  *   savepoint into the running configuration, so re-running the same config resumes where the
  *   previous run left off. Set to `false` to always start from the configuration as written (for
  *   example, to force a full re-migration while reusing the same savepoints directory).
  */
case class Savepoints(
  intervalSeconds: Int,
  path: String,
  enableParquetFileTracking: Boolean = true,
  target: Option[SavepointsTarget] = None,
  autoResume: Boolean = true
) {
  def effectiveTarget: SavepointsTarget.StoragePathTarget =
    target match {
      case Some(target: SavepointsTarget.StoragePathTarget) => target
      case None                                             => SavepointsTarget.Filesystem(path)
    }

  def storagePath: String = effectiveTarget.storagePath
}

object Savepoints {
  val Default: Savepoints = Savepoints(intervalSeconds = 300, path = "/app/savepoints")

  implicit val decoder: Decoder[Savepoints] =
    Decoder.instance { cursor =>
      for {
        intervalSeconds <- cursor.get[Int]("intervalSeconds")
        enableParquetFileTracking <-
          cursor.getOrElse[Boolean]("enableParquetFileTracking")(true)
        autoResume  <- cursor.getOrElse[Boolean]("autoResume")(true)
        maybePath   <- cursor.get[Option[String]]("path")
        maybeTarget <- cursor.get[Option[SavepointsTarget]]("target")
        _ <- Either.cond(
               maybePath.isEmpty || maybeTarget.isEmpty,
               (),
               DecodingFailure(
                 "Specify only one of savepoints.path or savepoints.target.",
                 cursor.history
               )
             )
        path <- maybeTarget match {
                  case Some(target: SavepointsTarget.StoragePathTarget) =>
                    Right(target.storagePath)
                  case Some(other) =>
                    Left(
                      DecodingFailure(
                        s"Savepoints target type '${other.targetType}' does not resolve to a storage path.",
                        cursor.history
                      )
                    )
                  case None =>
                    maybePath
                      .filter(_.trim.nonEmpty)
                      .toRight(
                        DecodingFailure(
                          "Savepoints configuration requires either 'path' or 'target'.",
                          cursor.history
                        )
                      )
                }
      } yield Savepoints(intervalSeconds, path, enableParquetFileTracking, maybeTarget, autoResume)
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
}
