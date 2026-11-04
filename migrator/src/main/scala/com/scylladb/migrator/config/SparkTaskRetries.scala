package com.scylladb.migrator.config

import org.apache.spark.SparkConf

/** Keep the application default while honoring a retry budget supplied to spark-submit. */
private[migrator] object SparkTaskRetries {
  def applyDefault(conf: SparkConf): Unit =
    conf.setIfMissing("spark.task.maxFailures", "1024")
}
