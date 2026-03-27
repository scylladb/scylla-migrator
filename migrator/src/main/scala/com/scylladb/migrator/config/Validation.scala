package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.deriveEncoder

case class Validation(
  compareTimestamps: Boolean,
  ttlToleranceMillis: Long,
  writetimeToleranceMillis: Long,
  failuresToFetch: Int,
  floatingPointTolerance: Double,
  timestampMsTolerance: Long,
  copyMissingRows: Boolean = false
)
object Validation {
  implicit val encoder: Encoder[Validation] = deriveEncoder[Validation]
  implicit val decoder: Decoder[Validation] = Decoder.instance { c =>
    for {
      compareTimestamps        <- c.get[Boolean]("compareTimestamps")
      ttlToleranceMillis       <- c.get[Long]("ttlToleranceMillis")
      writetimeToleranceMillis <- c.get[Long]("writetimeToleranceMillis")
      failuresToFetch          <- c.get[Int]("failuresToFetch")
      floatingPointTolerance   <- c.get[Double]("floatingPointTolerance")
      timestampMsTolerance     <- c.get[Long]("timestampMsTolerance")
      copyMissingRows          <- c.getOrElse[Boolean]("copyMissingRows")(false)
    } yield Validation(
      compareTimestamps,
      ttlToleranceMillis,
      writetimeToleranceMillis,
      failuresToFetch,
      floatingPointTolerance,
      timestampMsTolerance,
      copyMissingRows
    )
  }
}
