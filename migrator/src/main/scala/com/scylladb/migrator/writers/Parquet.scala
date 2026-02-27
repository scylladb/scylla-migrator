package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.Parquet")

  def writeDataframe(target: TargetSettings.Parquet, df: DataFrame): Unit = {
    log.info(
      s"Writing DataFrame to Parquet at ${target.path} " +
        s"(${df.rdd.getNumPartitions} partitions, compression: ${target.compression})"
    )
    df.write
      .option("compression", target.compression)
      .parquet(target.path)
    log.info(s"Successfully wrote Parquet files to ${target.path}")
  }
}
