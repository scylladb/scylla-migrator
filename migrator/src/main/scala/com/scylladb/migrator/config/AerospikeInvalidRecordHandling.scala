package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }

/** Response to invalid record values; invalid keys always skip the record. */
sealed trait AerospikeInvalidRecordHandling extends Serializable

object AerospikeInvalidRecordHandling {
  case object Skip extends AerospikeInvalidRecordHandling
  case object Null extends AerospikeInvalidRecordHandling

  implicit val decoder: Decoder[AerospikeInvalidRecordHandling] = Decoder.decodeString.emap {
    case "skip" => Right(Skip)
    case "null" => Right(Null)
    case value =>
      Left(s"Unknown Aerospike invalidRecordHandling: '$value'. Valid values: skip, null")
  }

  implicit val encoder: Encoder[AerospikeInvalidRecordHandling] = Encoder.encodeString.contramap {
    case Skip => "skip"
    case Null => "null"
  }
}
