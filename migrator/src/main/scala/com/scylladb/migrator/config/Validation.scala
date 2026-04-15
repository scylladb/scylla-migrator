package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder }
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

/** @param compareTimestamps
  *   Whether to compare TTL and WRITETIME metadata (Cassandra only).
  * @param ttlToleranceMillis
  *   Tolerance for TTL comparisons.
  * @param writetimeToleranceMillis
  *   Tolerance for WRITETIME comparisons.
  * @param failuresToFetch
  *   Maximum number of row failures to collect before stopping.
  * @param floatingPointTolerance
  *   Tolerance for floating-point value comparisons.
  * @param timestampMsTolerance
  *   Tolerance in milliseconds for timestamp comparisons.
  * @param hashColumns
  *   When set, validation compares these columns via a single content-hash column instead of
  *   joining their raw values. This reduces Spark-side join/shuffle volume, but the current
  *   validator still reads the original payload columns from MySQL and ScyllaDB before hashing.
  *   Only applies to MySQL-to-ScyllaDB validation. Use source-side column names here; renames are
  *   applied automatically.
  */
case class Validation(
  compareTimestamps: Boolean,
  ttlToleranceMillis: Long,
  writetimeToleranceMillis: Long,
  failuresToFetch: Int,
  floatingPointTolerance: Double,
  timestampMsTolerance: Long,
  hashColumns: Option[List[String]] = None
)
object Validation {
  private implicit val circeConfig: Configuration = Configuration.default.withDefaults
  implicit val encoder: Encoder[Validation] = deriveConfiguredEncoder[Validation]
  implicit val decoder: Decoder[Validation] = Decoder.instance { cursor =>
    deriveConfiguredDecoder[Validation].apply(cursor).flatMap { v =>
      if (v.failuresToFetch > 0) Right(v)
      else
        Left(
          DecodingFailure(
            s"failuresToFetch must be > 0, got: ${v.failuresToFetch}",
            cursor.history
          )
        )
    }
  }
}
