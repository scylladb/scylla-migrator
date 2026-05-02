package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder }
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

import java.util.Locale

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
  * @param copyMissingRows
  *   If true, rows that exist in the source but are missing in the target are copied to the target
  *   during validation. Only missing rows are copied — rows with value differences are still only
  *   reported, never overwritten. Defaults to false.
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
  copyMissingRows: Boolean = false,
  hashColumns: Option[List[String]] = None
)
object Validation {
  private implicit val circeConfig: Configuration = Configuration.default.withDefaults
  implicit val encoder: Encoder[Validation] = deriveConfiguredEncoder[Validation]
  implicit val decoder: Decoder[Validation] = Decoder.instance { cursor =>
    deriveConfiguredDecoder[Validation].apply(cursor).flatMap { v =>
      val hashColumns = v.hashColumns.getOrElse(Nil)
      val duplicateHashColumns = hashColumns
        .groupBy(_.toLowerCase(Locale.ROOT))
        .values
        .filter(_.size > 1)
        .map(_.mkString("/"))
        .toList
      val validationErrors = List(
        Option.when(v.failuresToFetch <= 0)(
          s"failuresToFetch must be > 0, got: ${v.failuresToFetch}"
        ),
        Option.when(v.ttlToleranceMillis < 0)(
          s"ttlToleranceMillis must be >= 0, got: ${v.ttlToleranceMillis}"
        ),
        Option.when(v.writetimeToleranceMillis < 0)(
          s"writetimeToleranceMillis must be >= 0, got: ${v.writetimeToleranceMillis}"
        ),
        Option.when(v.timestampMsTolerance < 0)(
          s"timestampMsTolerance must be >= 0, got: ${v.timestampMsTolerance}"
        ),
        Option.when(
          v.floatingPointTolerance < 0 ||
            v.floatingPointTolerance.isNaN ||
            v.floatingPointTolerance.isInfinity
        )(
          s"floatingPointTolerance must be a finite value >= 0, got: ${v.floatingPointTolerance}"
        ),
        Option.when(hashColumns.exists(_.trim.isEmpty))(
          "hashColumns must not contain empty or blank column names"
        ),
        Option.when(duplicateHashColumns.nonEmpty)(
          s"hashColumns contains duplicate column names: ${duplicateHashColumns.mkString(", ")}"
        )
      ).flatten

      if (validationErrors.isEmpty) Right(v)
      else
        Left(
          DecodingFailure(
            validationErrors.mkString("; "),
            cursor.history
          )
        )
    }
  }
}
