package com.scylladb.migrator.config

import cats.implicits._
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json, ObjectEncoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class DynamoDBEndpoint(host: String, port: Int) {
  def renderEndpoint = s"${host}:${port}"
}

object DynamoDBEndpoint {
  implicit val encoder = deriveEncoder[DynamoDBEndpoint]
  implicit val decoder = deriveDecoder[DynamoDBEndpoint]
}

sealed trait SourceSettings
object SourceSettings {
  case class Cassandra(host: String,
                       port: Int,
                       localDC: Option[String],
                       credentials: Option[Credentials],
                       sslOptions: Option[SSLOptions],
                       keyspace: String,
                       table: String,
                       splitCount: Option[Int],
                       connections: Option[Int],
                       fetchSize: Int,
                       preserveTimestamps: Boolean,
                       defaultTTL: Option[Long],
                       where: Option[String])
      extends SourceSettings
  case class DynamoDB(endpoint: Option[DynamoDBEndpoint],
                      region: Option[String],
                      credentials: Option[AWSCredentials],
                      table: String,
                      scanSegments: Option[Int],
                      readThroughput: Option[Int],
                      throughputReadPercent: Option[Float],
                      maxMapTasks: Option[Int])
      extends SourceSettings
  case class Parquet(path: String, credentials: Option[AWSCredentials]) extends SourceSettings

  implicit val decoder: Decoder[SourceSettings] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "cassandra" | "scylla" =>
        deriveDecoder[Cassandra].apply(cursor)
      case "parquet" =>
        deriveDecoder[Parquet].apply(cursor)
      case "dynamo" | "dynamodb" =>
        deriveDecoder[DynamoDB].apply(cursor)
      case otherwise =>
        Left(DecodingFailure(s"Unknown source type: ${otherwise}", cursor.history))
    }
  }

  implicit val encoder: Encoder[SourceSettings] = Encoder.instance {
    case s: Cassandra =>
      deriveEncoder[Cassandra]
        .encodeObject(s)
        .add("type", Json.fromString("cassandra"))
        .asJson
    case s: DynamoDB =>
      deriveEncoder[DynamoDB]
        .encodeObject(s)
        .add("type", Json.fromString("dynamodb"))
        .asJson
    case s: Parquet =>
      deriveEncoder[Parquet]
        .encodeObject(s)
        .add("type", Json.fromString("parquet"))
        .asJson
  }
}
