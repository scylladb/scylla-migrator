package com.scylladb.migrator.config

import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }

sealed trait SourceSettings

object SourceSettings {
  case class Cassandra(host: String,
                       port: Int,
                       credentials: Option[Credentials],
                       keyspace: String,
                       table: String,
                       splitCount: Option[Int],
                       connections: Option[Int],
                       fetchSize: Int,
                       preserveTimestamps: Boolean,
                       where: Option[String])
      extends SourceSettings

  implicit val decoder: Decoder[SourceSettings] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "cassandra" | "scylla" =>
        Decoder
          .forProduct10(
            "host",
            "port",
            "credentials",
            "keyspace",
            "table",
            "splitCount",
            "connections",
            "fetchSize",
            "preserveTimestamps",
            "where")(Cassandra.apply)
          .apply(cursor)

      case otherwise =>
        Left(DecodingFailure(s"Unknown source type: ${otherwise}", cursor.history))
    }
  }

  implicit val encoder: Encoder[SourceSettings] = Encoder.instance {
    case s: Cassandra =>
      Encoder
        .forProduct10(
          "host",
          "port",
          "credentials",
          "keyspace",
          "table",
          "splitCount",
          "connections",
          "fetchSize",
          "preserveTimestamps",
          "where")(Cassandra.unapply(_: Cassandra).get)
        .encodeObject(s)
        .add("type", Json.fromString("cassandra"))
        .asJson
  }
}
