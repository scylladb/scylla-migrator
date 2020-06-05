package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class AWSCredentials(accessKey: String, secretKey: String)
object AWSCredentials {
  implicit val decoder: Decoder[AWSCredentials] = deriveDecoder
  implicit val encoder: Encoder[AWSCredentials] = deriveEncoder
}
