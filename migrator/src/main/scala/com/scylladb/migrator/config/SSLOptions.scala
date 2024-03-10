package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class SSLOptions(clientAuthEnabled: Boolean,
                      enabled: Boolean,
                      enabledAlgorithms: Option[Set[String]],
                      keyStorePassword: Option[String],
                      keyStorePath: Option[String],
                      keyStoreType: Option[String],
                      protocol: Option[String],
                      trustStorePassword: Option[String],
                      trustStorePath: Option[String],
                      trustStoreType: Option[String])

object SSLOptions {
  implicit val encoder: Encoder[SSLOptions] = deriveEncoder[SSLOptions]
  implicit val decoder: Decoder[SSLOptions] = deriveDecoder[SSLOptions]
}
