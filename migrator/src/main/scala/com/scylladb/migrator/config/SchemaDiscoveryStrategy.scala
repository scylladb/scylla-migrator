package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }

/** Controls how many Aerospike partitions are scanned during schema discovery.
  *
  * "progressive" (default): scans 8 -> 64 -> all partitions, stopping after finding records.
  * "single": scans only 8 partitions (fastest startup, may miss bins in sparse data). "full": scans
  * all partitions (most thorough, may be slow on large clusters).
  */
sealed trait SchemaDiscoveryStrategy

object SchemaDiscoveryStrategy {
  case object Progressive extends SchemaDiscoveryStrategy
  case object Single extends SchemaDiscoveryStrategy
  case object Full extends SchemaDiscoveryStrategy

  implicit val decoder: Decoder[SchemaDiscoveryStrategy] = Decoder.decodeString.emap { s =>
    s.toLowerCase match {
      case "progressive" => Right(Progressive)
      case "single"      => Right(Single)
      case "full"        => Right(Full)
      case _ =>
        Left(
          s"Unknown schemaDiscoveryStrategy: '$s'. Valid values: progressive, single, full"
        )
    }
  }

  implicit val encoder: Encoder[SchemaDiscoveryStrategy] = Encoder.encodeString.contramap {
    case Progressive => "progressive"
    case Single      => "single"
    case Full        => "full"
  }
}
