package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }

/** Response to the shared non-frozen list write budget being exceeded by a record. */
sealed trait AerospikeListOverflowHandling extends Serializable

object AerospikeListOverflowHandling {
  case object Skip extends AerospikeListOverflowHandling
  case object Nullify extends AerospikeListOverflowHandling
  case object Empty extends AerospikeListOverflowHandling

  implicit val decoder: Decoder[AerospikeListOverflowHandling] = Decoder.decodeString.emap {
    case "skip"    => Right(Skip)
    case "nullify" => Right(Nullify)
    case "empty"   => Right(Empty)
    case value =>
      Left(s"Unknown Aerospike listOverflowHandling: '$value'. Valid values: skip, nullify, empty")
  }

  implicit val encoder: Encoder[AerospikeListOverflowHandling] = Encoder.encodeString.contramap {
    case Skip    => "skip"
    case Nullify => "nullify"
    case Empty   => "empty"
  }
}
