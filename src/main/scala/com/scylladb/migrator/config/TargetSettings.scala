package com.scylladb.migrator.config

import cats.implicits._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._

sealed trait TargetSettings
object TargetSettings {
  case class Scylla(host: String,
                    port: Int,
                    credentials: Option[Credentials],
                    keyspace: String,
                    table: String,
                    connections: Option[Int],
                    stripTrailingZerosForDecimals: Boolean)
      extends TargetSettings

  implicit val decoder: Decoder[TargetSettings] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "scylla" | "cassandra" =>
          deriveDecoder[Scylla].apply(cursor)
        case otherwise =>
          Left(DecodingFailure(s"Invalid target type: ${otherwise}", cursor.history))
      }
    }

  implicit val encoder: Encoder[TargetSettings] =
    Encoder.instance {
      case t: Scylla =>
        deriveEncoder[Scylla].encodeObject(t).add("type", Json.fromString("scylla")).asJson
    }
}
