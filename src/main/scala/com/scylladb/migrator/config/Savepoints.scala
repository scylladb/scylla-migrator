package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Savepoints(intervalSeconds: Int, path: String)
object Savepoints {
  implicit val encoder: Encoder[Savepoints] = deriveEncoder[Savepoints]
  implicit val decoder: Decoder[Savepoints] = deriveDecoder[Savepoints]
}
