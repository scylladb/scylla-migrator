package com.scylladb.migrator.config

import org.apache.spark.SparkConf

class SparkTaskRetriesTest extends munit.FunSuite {
  test("an absent task retry budget receives the existing application default") {
    val conf = new SparkConf(false)
    SparkTaskRetries.applyDefault(conf)
    assertEquals(conf.getInt("spark.task.maxFailures", 0), 1024)
  }

  test("an explicit task retry budget survives repeated application of defaults") {
    val conf = new SparkConf(false).set("spark.task.maxFailures", "2")
    SparkTaskRetries.applyDefault(conf)
    SparkTaskRetries.applyDefault(conf)
    assertEquals(conf.getInt("spark.task.maxFailures", 0), 2)
  }
}
