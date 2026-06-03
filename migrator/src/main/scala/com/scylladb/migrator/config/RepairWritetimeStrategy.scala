package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }

/** Controls the CQL TIMESTAMP used when `copyMissingRows` writes back missing rows during
  * Cassandra/Scylla validation with `preserveTimestamps: true`.
  *
  *   - '''Source''': use the original source writetime. The write may be shadowed by a newer delete
  *     tombstone on the target (known limitation).
  *   - '''Coordinator''': use current coordinator wall-clock time (µs). Beats most tombstones but
  *     may resurrect rows that were intentionally deleted on the target only.
  *   - '''Config''': use the fixed value from `target.writeWritetimestampInuS`. Requires that
  *     setting to be present in the target configuration.
  */
sealed trait RepairWritetimeStrategy
object RepairWritetimeStrategy {
  case object Source extends RepairWritetimeStrategy
  case object Coordinator extends RepairWritetimeStrategy
  case object Config extends RepairWritetimeStrategy

  implicit val encoder: Encoder[RepairWritetimeStrategy] =
    Encoder.encodeString.contramap {
      case Source      => "source"
      case Coordinator => "coordinator"
      case Config      => "config"
    }

  implicit val decoder: Decoder[RepairWritetimeStrategy] =
    Decoder.decodeString.emap {
      case s if s.equalsIgnoreCase("source")      => Right(Source)
      case s if s.equalsIgnoreCase("coordinator") => Right(Coordinator)
      case s if s.equalsIgnoreCase("config")      => Right(Config)
      case other =>
        Left(
          s"Invalid repairWritetimeStrategy '$other'. Allowed: source, coordinator, config"
        )
    }
}
