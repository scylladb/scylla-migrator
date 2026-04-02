package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder, Json }
import io.circe.syntax._
import io.circe.generic.semiauto.deriveDecoder

case class Credentials(username: String, password: String)
object Credentials {

  /** Encode credentials with the password redacted. This prevents secrets from appearing in
    * savepoint YAML files written by [[com.scylladb.migrator.SavepointsManager]]. Credentials are
    * always read from the original configuration file, never from savepoints.
    */
  implicit val encoder: Encoder[Credentials] = Encoder.instance { c =>
    Json.obj(
      "username" -> c.username.asJson,
      "password" -> Json.fromString("<redacted>")
    )
  }
  implicit val decoder: Decoder[Credentials] = deriveDecoder[Credentials]
}
