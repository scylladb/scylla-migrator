package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import com.scylladb.migrator.config.BillingModeCodec._

sealed trait TargetSettings
object TargetSettings {
  case class Scylla(
    host: String,
    port: Int,
    localDC: Option[String],
    credentials: Option[Credentials],
    sslOptions: Option[SSLOptions],
    keyspace: String,
    table: String,
    connections: Option[Int],
    stripTrailingZerosForDecimals: Boolean,
    writeTTLInS: Option[Int],
    writeWritetimestampInuS: Option[Long],
    consistencyLevel: String,
    dropNullPrimaryKeys: Option[Boolean] = None,
    cloud: Option[CloudConfig] = None
  ) extends TargetSettings

  /** Mirror of `SourceSettings.Cassandra` cloud handling. See the doc-comment on [[CloudConfig]]
    * for why `host`/`port`/`localDC`/`sslOptions` are rejected when `cloud` is set.
    */
  object Scylla {
    private val SentinelHost: String = ""
    private val SentinelPort: Int = 0

    implicit val decoder: Decoder[Scylla] = Decoder.instance { c =>
      val hasHost = c.get[Option[String]]("host").exists(_.isDefined)
      val hasPort = c.get[Option[Int]]("port").exists(_.isDefined)
      for {
        cloud <- c.get[Option[CloudConfig]]("cloud")
        _ <- (cloud.isDefined, hasHost, hasPort) match {
               case (true, true, _) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (true, _, true) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (false, true, true) => Right(())
               case (false, _, _) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'host' and 'port' are required unless 'cloud' is set.",
                     c.history
                   )
                 )
               case (true, false, false) => Right(())
             }
        host                          <- c.getOrElse[String]("host")(SentinelHost)
        port                          <- c.getOrElse[Int]("port")(SentinelPort)
        localDC                       <- c.get[Option[String]]("localDC")
        credentials                   <- c.get[Option[Credentials]]("credentials")
        sslOptions                    <- c.get[Option[SSLOptions]]("sslOptions")
        keyspace                      <- c.get[String]("keyspace")
        table                         <- c.get[String]("table")
        connections                   <- c.get[Option[Int]]("connections")
        stripTrailingZerosForDecimals <- c.get[Boolean]("stripTrailingZerosForDecimals")
        writeTTLInS                   <- c.get[Option[Int]]("writeTTLInS")
        writeWritetimestampInuS       <- c.get[Option[Long]]("writeWritetimestampInuS")
        consistencyLevel              <- c.get[String]("consistencyLevel")
        dropNullPrimaryKeys           <- c.get[Option[Boolean]]("dropNullPrimaryKeys")
        _ <- if (cloud.isDefined && localDC.isDefined)
               Left(
                 DecodingFailure(
                   "Scylla target: 'localDC' must not be set when using 'cloud' (the bundle " +
                     "carries the local DC).",
                   c.history
                 )
               )
             else Right(())
        _ <- if (cloud.isDefined && sslOptions.isDefined)
               Left(
                 DecodingFailure(
                   "Scylla target: 'sslOptions' must not be set when using 'cloud' (the bundle " +
                     "carries the TLS material).",
                   c.history
                 )
               )
             else Right(())
      } yield Scylla(
        host,
        port,
        localDC,
        credentials,
        sslOptions,
        keyspace,
        table,
        connections,
        stripTrailingZerosForDecimals,
        writeTTLInS,
        writeWritetimestampInuS,
        consistencyLevel,
        dropNullPrimaryKeys,
        cloud
      )
    }

    implicit val encoder: Encoder.AsObject[Scylla] = Encoder.AsObject.instance { s =>
      val common = io.circe.JsonObject(
        "localDC"                       -> s.localDC.asJson,
        "credentials"                   -> s.credentials.asJson,
        "sslOptions"                    -> s.sslOptions.asJson,
        "keyspace"                      -> s.keyspace.asJson,
        "table"                         -> s.table.asJson,
        "connections"                   -> s.connections.asJson,
        "stripTrailingZerosForDecimals" -> s.stripTrailingZerosForDecimals.asJson,
        "writeTTLInS"                   -> s.writeTTLInS.asJson,
        "writeWritetimestampInuS"       -> s.writeWritetimestampInuS.asJson,
        "consistencyLevel"              -> s.consistencyLevel.asJson,
        "dropNullPrimaryKeys"           -> s.dropNullPrimaryKeys.asJson
      )
      s.cloud match {
        case Some(cloud) => common.add("cloud", cloud.asJson)
        case None =>
          common.add("host", s.host.asJson).add("port", s.port.asJson)
      }
    }
  }

  case class DynamoDB(
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    writeThroughput: Option[Int],
    throughputWritePercent: Option[Float],
    streamChanges: Boolean,
    skipInitialSnapshotTransfer: Option[Boolean],
    removeConsumedCapacity: Option[Boolean] = Some(true),
    billingMode: Option[BillingMode] = None,
    alternator: Option[AlternatorSettings] = None
  ) extends TargetSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  case class Parquet(
    path: String,
    compression: String = "snappy",
    mode: String = "error"
  ) extends TargetSettings
  object Parquet {
    val validCompressionCodecs: Set[String] =
      Set("none", "uncompressed", "snappy", "gzip", "lzo", "brotli", "lz4", "zstd")
    val validModes: Set[String] =
      Set("error", "overwrite", "append", "ignore")
  }

  case class DynamoDBS3Export(
    path: String
  ) extends TargetSettings

  implicit val decoder: Decoder[TargetSettings] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "scylla" | "cassandra" =>
          Scylla.decoder.apply(cursor)
        case "dynamodb" | "dynamo" =>
          deriveDecoder[DynamoDB].apply(cursor)
        case "parquet" =>
          for {
            path        <- cursor.get[String]("path")
            compression <- cursor.getOrElse[String]("compression")("snappy")
            mode        <- cursor.getOrElse[String]("mode")("error")
            _ <- Either.cond(
                   path.trim.nonEmpty,
                   (),
                   DecodingFailure("Parquet target 'path' must not be empty", cursor.history)
                 )
            _ <- Either.cond(
                   Parquet.validCompressionCodecs.contains(compression.toLowerCase),
                   (),
                   DecodingFailure(
                     s"Invalid Parquet compression codec '$compression'. " +
                       s"Valid values: ${Parquet.validCompressionCodecs.toSeq.sorted.mkString(", ")}",
                     cursor.history
                   )
                 )
            _ <- Either.cond(
                   Parquet.validModes.contains(mode.toLowerCase),
                   (),
                   DecodingFailure(
                     s"Invalid Parquet write mode '$mode'. " +
                       s"Valid values: ${Parquet.validModes.toSeq.sorted.mkString(", ")}",
                     cursor.history
                   )
                 )
          } yield Parquet(path.trim, compression.toLowerCase, mode.toLowerCase)
        case "dynamodb-s3-export" =>
          for {
            path <- cursor.get[String]("path")
            _ <- Either.cond(
                   path.trim.nonEmpty,
                   (),
                   DecodingFailure(
                     "DynamoDB S3 Export target 'path' must not be empty",
                     cursor.history
                   )
                 )
          } yield DynamoDBS3Export(path.trim)
        case otherwise =>
          Left(DecodingFailure(s"Invalid target type: ${otherwise}", cursor.history))
      }
    }

  implicit val encoder: Encoder[TargetSettings] =
    Encoder.instance {
      case t: Scylla =>
        Scylla.encoder.encodeObject(t).add("type", Json.fromString("scylla")).asJson

      case t: DynamoDB =>
        deriveEncoder[DynamoDB].encodeObject(t).add("type", Json.fromString("dynamodb")).asJson

      case t: Parquet =>
        deriveEncoder[Parquet].encodeObject(t).add("type", Json.fromString("parquet")).asJson

      case t: DynamoDBS3Export =>
        deriveEncoder[DynamoDBS3Export]
          .encodeObject(t)
          .add("type", Json.fromString("dynamodb-s3-export"))
          .asJson
    }
}
