package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class SSLOptions(
  clientAuthEnabled: Boolean,
  enabled: Boolean,
  enabledAlgorithms: Option[Set[String]],
  keyStorePassword: Option[String],
  keyStorePath: Option[String],
  keyStoreType: Option[String],
  protocol: Option[String],
  trustStorePassword: Option[String],
  trustStorePath: Option[String],
  trustStoreType: Option[String]
)

object SSLOptions {
  implicit val encoder: Encoder[SSLOptions] = deriveEncoder[SSLOptions]
  implicit val decoder: Decoder[SSLOptions] = deriveDecoder[SSLOptions]

  /** Shared default values used by [[com.scylladb.migrator.Connectors]] and
    * [[com.scylladb.migrator.scylla.ScyllaSparkConnectionOptions]].
    */
  val DefaultTrustStoreType: String = "JKS"
  val DefaultKeyStoreType: String = "JKS"
  val DefaultProtocol: String = "TLS"
  // These two CBC cipher suites are intentionally narrower than the Spark Cassandra Connector
  // defaults (which also include TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 and
  // TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384). The narrower set maximises compatibility with older
  // ScyllaDB/Cassandra deployments. Users whose clusters require GCM suites can override via the
  // `enabledAlgorithms` config property.
  val DefaultEnabledAlgorithms: Set[String] =
    Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
}
