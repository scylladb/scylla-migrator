package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, Json }

/** TTL configuration for the target Scylla table.
  *
  * Parsed from a single YAML field that accepts either a plain number or a `"value:policy"` string:
  *   - `7776000` — equivalent to `"7776000:set-if-missing"`
  *   - `"7776000:set-if-missing"` — apply TTL only to cells that have no TTL on the source
  *   - `"7776000:always"` — apply TTL to all cells, overriding any existing source TTL
  *   - `"7776000:update-if-present"` — apply TTL only to cells that already have a TTL on the
  *     source
  */
case class TTLConfig(value: Long, policy: TTLPolicy)

sealed trait TTLPolicy
object TTLPolicy {
  case object SetIfMissing extends TTLPolicy
  case object Always extends TTLPolicy
  case object UpdateIfPresent extends TTLPolicy

  def fromString(s: String): Either[String, TTLPolicy] =
    s match {
      case "set-if-missing"    => Right(SetIfMissing)
      case "always"            => Right(Always)
      case "update-if-present" => Right(UpdateIfPresent)
      case other =>
        Left(
          s"Unknown TTL policy: '$other'. Valid values: set-if-missing, always, update-if-present"
        )
    }
}

object TTLConfig {

  private def validateValue(
    value: Long
  ): Either[String, Long] =
    if (value <= 0)
      Left(s"ttl value must be positive, got: $value")
    else if (value > Int.MaxValue)
      Left(
        s"ttl value must not exceed ${Int.MaxValue} (Cassandra TTL is a 32-bit integer), got: $value"
      )
    else
      Right(value)

  implicit val decoder: Decoder[TTLConfig] = Decoder.instance { cursor =>
    // Try as a number first (plain value, defaults to set-if-missing)
    cursor.as[Long] match {
      case Right(value) =>
        validateValue(value).left
          .map(msg => DecodingFailure(msg, cursor.history))
          .map(v => TTLConfig(v, TTLPolicy.SetIfMissing))
      case Left(_) =>
        // Try as a string "value:policy"
        cursor.as[String].flatMap { str =>
          str.split(":", 2) match {
            case Array(valueStr, policyStr) =>
              for {
                value <- valueStr.trim.toLongOption
                           .toRight(
                             DecodingFailure(
                               s"Invalid ttl value: '$valueStr'. Expected a number",
                               cursor.history
                             )
                           )
                _ <- validateValue(value).left
                       .map(msg => DecodingFailure(msg, cursor.history))
                policy <- TTLPolicy
                            .fromString(policyStr.trim)
                            .left
                            .map(msg => DecodingFailure(msg, cursor.history))
              } yield TTLConfig(value, policy)
            case Array(valueStr) =>
              // Plain number as a string, defaults to set-if-missing
              for {
                value <- valueStr.trim.toLongOption
                           .toRight(
                             DecodingFailure(
                               s"Invalid ttl format: '$str'. Expected a number or 'value:policy'",
                               cursor.history
                             )
                           )
                _ <- validateValue(value).left
                       .map(msg => DecodingFailure(msg, cursor.history))
              } yield TTLConfig(value, TTLPolicy.SetIfMissing)
            case _ =>
              Left(
                DecodingFailure(
                  s"Invalid ttl format: '$str'. Expected a number or 'value:policy'",
                  cursor.history
                )
              )
          }
        }
    }
  }

  implicit val encoder: Encoder[TTLConfig] = Encoder.instance { config =>
    config.policy match {
      case TTLPolicy.SetIfMissing => Json.fromLong(config.value)
      case _ => Json.fromString(s"${config.value}:${policyToString(config.policy)}")
    }
  }

  private def policyToString(policy: TTLPolicy): String =
    policy match {
      case TTLPolicy.SetIfMissing    => "set-if-missing"
      case TTLPolicy.Always          => "always"
      case TTLPolicy.UpdateIfPresent => "update-if-present"
    }
}
