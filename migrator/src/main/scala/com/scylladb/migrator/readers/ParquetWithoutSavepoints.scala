package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

object ParquetWithoutSavepoints {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.ParquetWithoutSavepoints")

  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame = {
    log.info(s"Reading Parquet files from ${source.path} (without savepoint tracking)")

    Parquet.configureHadoopCredentials(spark, source)

    val df = spark.read.parquet(source.path)
    log.info(s"Loaded Parquet DataFrame with ${df.rdd.getNumPartitions} partitions")

    if (TimestampColumns.hasPerColumnMetaInParquet(df.schema)) {
      log.info(
        "Detected per-column CQL timestamp metadata in Parquet schema. " +
          "Performing row explosion for TTL/writetime preservation."
      )
      val renamed = TimestampColumns.renameFromParquet(df)
      val (explodedDf, timestampColumns) =
        Cassandra.explodeDataframeFromPerColumnMeta(spark, renamed)
      SourceDataFrame(explodedDf, Some(timestampColumns), savepointsSupported = false)
    } else {
      SourceDataFrame(df, None, savepointsSupported = false)
    }
  }
}
