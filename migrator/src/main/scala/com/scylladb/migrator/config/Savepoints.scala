package com.scylladb.migrator.config

import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

case class Savepoints(intervalSeconds: Int, path: String, enableParquetFileTracking: Boolean = true)

object Savepoints {
  implicit val config: Configuration = Configuration.default.withDefaults
  implicit val codec: Codec[Savepoints] = deriveConfiguredCodec[Savepoints]
}
