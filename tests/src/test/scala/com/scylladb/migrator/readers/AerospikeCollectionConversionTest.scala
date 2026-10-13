package com.scylladb.migrator.readers

import org.apache.spark.sql.types._
import scala.jdk.CollectionConverters._

class AerospikeCollectionConversionTest extends munit.FunSuite {
  private def list(values: Any*): java.util.List[Any] = values.toList.asJava
  private def map(values: (Any, Any)*): java.util.Map[Any, Any] = values.toMap.asJava
  test("explicit nested string types preserve large integers and mixed numbers") {
    val integer = 9007199254740993L
    val integers = list(map(integer -> list(integer)))
    val doubles = list(map(1.5 -> list(2.5)))
    val expected = ArrayType(MapType(StringType, ArrayType(StringType, true), true), true)
    assertEquals(
      AerospikeTypes.convertValue(integers, expected),
      Seq(Map("9007199254740993" -> Seq("9007199254740993")))
    )
    assertEquals(AerospikeTypes.convertValue(doubles, expected), Seq(Map("1.5" -> Seq("2.5"))))
    val mixed = list(map(integer -> list(integer), 1.5 -> list(2.5)))
    assertEquals(
      AerospikeTypes.convertValue(mixed, expected),
      Seq(Map("9007199254740993" -> Seq("9007199254740993"), "1.5" -> Seq("2.5")))
    )
  }

  test("explicit nested types preserve empty collections") {
    val schema = ArrayType(MapType(LongType, ArrayType(BinaryType, true), true), true)
    assertEquals(
      AerospikeTypes.convertValue(list(map(), map(42L -> list())), schema),
      Seq(Map.empty, Map(42L -> Seq.empty))
    )
  }

  test("explicit nested double schemas reject precision loss in list elements and map entries") {
    val integer = 9007199254740993L
    val cases = Seq(
      list(integer)               -> ArrayType(DoubleType, true),
      map(integer -> "value")     -> MapType(DoubleType, StringType, true),
      map("key" -> list(integer)) -> MapType(StringType, ArrayType(DoubleType, true), true)
    )
    for ((value, schema) <- cases) {
      val error = intercept[IllegalStateException] {
        AerospikeTypes.convertValue(value, schema)
      }
      assert(error.getMessage.contains("cannot be represented exactly as DoubleType"))
    }
  }

  test("GeoJSON client values preserve JSON without unexpected-type warnings") {
    val logger = org.apache.logging.log4j.LogManager
      .getLogger(
        "com.scylladb.migrator.readers.AerospikeTypes"
      )
      .asInstanceOf[org.apache.logging.log4j.core.Logger]
    val messages = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
      "geojson-test",
      null,
      null,
      false,
      Array.empty[org.apache.logging.log4j.core.config.Property]
    ) {
      override def append(event: org.apache.logging.log4j.core.LogEvent): Unit = {
        messages.add(event.getMessage.getFormattedMessage)
        ()
      }
    }
    val oldLevel = logger.getLevel
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(org.apache.logging.log4j.Level.WARN)
    AerospikeTypes.reset()
    try {
      val json = """{"type":"Point","coordinates":[1.25,2.5]}"""
      val value = new com.aerospike.client.Value.GeoJSONValue(json)
      assertEquals(AerospikeTypes.convertValue(value, StringType), json)
      assertEquals(AerospikeTypes.convertValue(list(value), ArrayType(StringType, true)), Seq(json))
      assert(messages.isEmpty)
    } finally {
      logger.removeAppender(appender)
      logger.setLevel(oldLevel)
      appender.stop()
    }
  }
}
