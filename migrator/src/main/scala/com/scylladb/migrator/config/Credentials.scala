package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Credentials(username: String, password: String) {
  override def toString: String = s"Credentials($username, ****)"
}
object Credentials {

  /** Default encoder masks passwords for safety. Use `roundTripEncoder` for serialization fidelity.
    */
  implicit val encoder: Encoder[Credentials] = Encoder.instance { c =>
    io.circe.Json.obj(
      "username" -> io.circe.Json.fromString(c.username),
      "password" -> io.circe.Json.fromString("****")
    )
  }
  implicit val decoder: Decoder[Credentials] = deriveDecoder[Credentials]

  /** Encoder that preserves the actual password, for use in savepoints or round-trip serialization.
    */
  val roundTripEncoder: Encoder[Credentials] = deriveEncoder[Credentials]
}
