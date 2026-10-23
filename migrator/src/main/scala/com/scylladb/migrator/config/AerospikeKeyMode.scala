package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }

/** Required record identity representation, independent of the declared bin schema. */
sealed trait AerospikeKeyMode extends Serializable

object AerospikeKeyMode {
  case object StringKey extends AerospikeKeyMode
  case object IntegerKey extends AerospikeKeyMode
  case object BinaryKey extends AerospikeKeyMode
  case object Digest extends AerospikeKeyMode

  implicit val decoder: Decoder[AerospikeKeyMode] = Decoder.decodeString.emap {
    case "string"  => Right(StringKey)
    case "integer" => Right(IntegerKey)
    case "binary"  => Right(BinaryKey)
    case "digest"  => Right(Digest)
    case value =>
      Left(s"Unknown Aerospike keyMode: '$value'. Valid values: string, integer, binary, digest")
  }

  implicit val encoder: Encoder[AerospikeKeyMode] = Encoder.encodeString.contramap {
    case StringKey  => "string"
    case IntegerKey => "integer"
    case BinaryKey  => "binary"
    case Digest     => "digest"
  }
}
