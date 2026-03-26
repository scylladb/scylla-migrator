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
    dropNullPrimaryKeys: Option[Boolean] = None
  ) extends TargetSettings

  /** Common trait for DynamoDB-protocol targets (both AWS DynamoDB and Scylla Alternator). */
  sealed trait DynamoDBLike extends TargetSettings {
    def endpoint: Option[DynamoDBEndpoint]
    def region: Option[String]
    def credentials: Option[AWSCredentials]
    def table: String
    def writeThroughput: Option[Int]
    def throughputWritePercent: Option[Float]
    def streamChanges: Boolean
    def skipInitialSnapshotTransfer: Option[Boolean]
    def removeConsumedCapacity: Option[Boolean]
    def billingMode: Option[BillingMode]
    def alternatorSettings: Option[AlternatorSettings]
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
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
    billingMode: Option[BillingMode] = None
  ) extends DynamoDBLike {
    val alternatorSettings: Option[AlternatorSettings] = None
  }

  case class Alternator(
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
    datacenter: Option[String] = None,
    rack: Option[String] = None,
    activeRefreshIntervalMs: Option[Long] = None,
    idleRefreshIntervalMs: Option[Long] = None,
    compression: Option[Boolean] = None,
    optimizeHeaders: Option[Boolean] = None,
    maxConnections: Option[Int] = None,
    connectionMaxIdleTimeMs: Option[Long] = None,
    connectionTimeToLiveMs: Option[Long] = None,
    connectionAcquisitionTimeoutMs: Option[Long] = None,
    connectionTimeoutMs: Option[Long] = None
  ) extends DynamoDBLike {
    val alternatorSettings: Option[AlternatorSettings] = Some(
      AlternatorSettings(
        datacenter                     = datacenter,
        rack                           = rack,
        activeRefreshIntervalMs        = activeRefreshIntervalMs,
        idleRefreshIntervalMs          = idleRefreshIntervalMs,
        compression                    = compression,
        optimizeHeaders                = optimizeHeaders,
        maxConnections                 = maxConnections,
        connectionMaxIdleTimeMs        = connectionMaxIdleTimeMs,
        connectionTimeToLiveMs         = connectionTimeToLiveMs,
        connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs,
        connectionTimeoutMs            = connectionTimeoutMs
      )
    )
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
          deriveDecoder[Scylla].apply(cursor)
        case "dynamodb" | "dynamo" =>
          deriveDecoder[DynamoDB].apply(cursor)
        case "alternator" =>
          deriveDecoder[Alternator].apply(cursor)
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
        deriveEncoder[Scylla].encodeObject(t).add("type", Json.fromString("scylla")).asJson

      case t: DynamoDB =>
        deriveEncoder[DynamoDB].encodeObject(t).add("type", Json.fromString("dynamodb")).asJson

      case t: Alternator =>
        deriveEncoder[Alternator].encodeObject(t).add("type", Json.fromString("alternator")).asJson

      case t: Parquet =>
        deriveEncoder[Parquet].encodeObject(t).add("type", Json.fromString("parquet")).asJson

      case t: DynamoDBS3Export =>
        deriveEncoder[DynamoDBS3Export]
          .encodeObject(t)
          .add("type", Json.fromString("dynamodb-s3-export"))
          .asJson
    }
}
