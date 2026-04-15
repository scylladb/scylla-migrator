package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Credentials(username: String, password: String)
object Credentials {

  /** Credentials must serialize losslessly because resumable savepoints persist executable config.
    * Use [[com.scylladb.migrator.config.MigratorConfig.renderRedacted]] for log/display output.
    */
  implicit val encoder: Encoder[Credentials] = deriveEncoder[Credentials]
  implicit val decoder: Decoder[Credentials] = deriveDecoder[Credentials]
}
