package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Parquet reader implementation without savepoint tracking.
  *
  * This implementation provides simple Parquet file reading without file-level savepoint tracking.
  * Enable via configuration: `savepoints.enableParquetFileTracking = false`
  */
object ParquetWithoutSavepoints {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.ParquetWithoutSavepoints")

  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame = {
    log.info(s"Reading Parquet files from ${source.path} (without savepoint tracking)")

    Parquet.configureHadoopCredentials(spark, source)

    val df = spark.read.parquet(source.path)
    log.info(s"Loaded Parquet DataFrame with ${df.rdd.getNumPartitions} partitions")

    SourceDataFrame(df, None, savepointsSupported = false)
  }
}
