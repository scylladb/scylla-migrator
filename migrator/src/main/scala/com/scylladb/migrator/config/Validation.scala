package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
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
  *   When set, these columns are replaced by a single MD5 hash on the MySQL side and computed via
  *   Spark on the ScyllaDB side. This dramatically reduces data transfer for large text/blob
  *   columns. Only applies to MySQL-to-ScyllaDB validation.
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
  implicit val config: Configuration = Configuration.default.withDefaults
  implicit val encoder: Encoder[Validation] = deriveConfiguredEncoder[Validation]
  implicit val decoder: Decoder[Validation] = deriveConfiguredDecoder[Validation]
}
