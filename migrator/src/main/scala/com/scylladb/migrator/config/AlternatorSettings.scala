package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class AlternatorSettings(
  datacenter: Option[String] = None,
  rack: Option[String] = None,
  activeRefreshIntervalMs: Option[Long] = None,
  idleRefreshIntervalMs: Option[Long] = None,
  compression: Option[Boolean] = None,
  optimizeHeaders: Option[Boolean] = None,
  maxConnections: Option[Int] = None,
  connectionMaxIdleTimeMs: Option[Long] = None,
  connectionTimeToLiveMs: Option[Long] = None,
  connectionAcquisitionTimeoutMs: Option[Long] = None,
  connectionTimeoutMs: Option[Long] = None
)

object AlternatorSettings {
  implicit val decoder: Decoder[AlternatorSettings] = deriveDecoder
  implicit val encoder: Encoder[AlternatorSettings] = deriveEncoder
}
