package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Validation(compareTimestamps: Boolean,
                      ttlToleranceMillis: Long,
                      writetimeToleranceMillis: Long,
                      failuresToFetch: Int,
                      floatingPointTolerance: Double)
object Validation {
  implicit val encoder: Encoder[Validation] = deriveEncoder[Validation]
  implicit val decoder: Decoder[Validation] = deriveDecoder[Validation]
}
