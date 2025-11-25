package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import org.apache.spark.sql.SparkSession

trait ParquetProcessingStrategy {
  def migrate(config: MigratorConfig,
              source: SourceSettings.Parquet,
              target: TargetSettings.Scylla)(implicit spark: SparkSession): Unit
}
