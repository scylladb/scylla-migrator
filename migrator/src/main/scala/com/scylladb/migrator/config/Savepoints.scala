package com.scylladb.migrator.config

import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

/**
  * Configuration for periodic savepoints written during a migration run.
  *
  * @param intervalSeconds
  *   How often, in seconds, a savepoint should be written.
  * @param path
  *   Filesystem path (directory or prefix) where savepoint data will be stored.
  * @param enableParquetFileTracking
  *   When `true` (the default), enables tracking of already-processed Parquet files
  *   as part of the savepoint state. This prevents the same Parquet file from
  *   being migrated more than once if the job is restarted or savepoints are
  *   resumed.
  *
  *   Set this to `false` to keep the legacy behavior where Parquet files are not
  *   tracked in savepoints. Disabling tracking may be useful for backwards
  *   compatibility with older savepoints or when file tracking is handled by an
  *   external mechanism, but it means repeated runs may reprocess the same
  *   Parquet files.
  */
case class Savepoints(intervalSeconds: Int, path: String, enableParquetFileTracking: Boolean = true)

object Savepoints {
  implicit val config: Configuration = Configuration.default.withDefaults
  implicit val codec: Codec[Savepoints] = deriveConfiguredCodec[Savepoints]
}
