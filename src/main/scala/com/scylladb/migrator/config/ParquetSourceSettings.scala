package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class ParquetSourceSettings(path: String, credentials: Option[AWSCredentials])
object ParquetSourceSettings {
  implicit val decoder: Decoder[ParquetSourceSettings] = deriveDecoder
  implicit val encoder: Encoder[ParquetSourceSettings] = deriveEncoder
}
