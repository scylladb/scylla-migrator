package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class TargetSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          connections: Option[Int])
object TargetSettings {
  implicit val encoder: Encoder[TargetSettings] = deriveEncoder[TargetSettings]
  implicit val decoder: Decoder[TargetSettings] = deriveDecoder[TargetSettings]
}
