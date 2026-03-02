package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.Parquet")

  def writeDataframe(target: TargetSettings.Parquet, df: DataFrame): Unit = {
    val codec = target.compression.toLowerCase
    val partitions = df.rdd.getNumPartitions
    log.info(
      s"Writing DataFrame to Parquet at ${target.path} " +
        s"(${partitions} partitions, compression: ${target.compression}, mode: ${target.mode})"
    )
    try
      df.write
        .mode(target.mode)
        .option("compression", codec)
        .parquet(target.path)
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to write Parquet to ${target.path}: ${e.getMessage}", e)
    }
    log.info(s"Successfully wrote Parquet files to ${target.path}")
  }
}
