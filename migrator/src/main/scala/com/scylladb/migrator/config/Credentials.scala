package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Credentials(username: String, password: String)
object Credentials {
  implicit val encoder: Encoder[Credentials] = deriveEncoder[Credentials]
  implicit val decoder: Decoder[Credentials] = deriveDecoder[Credentials]
}
