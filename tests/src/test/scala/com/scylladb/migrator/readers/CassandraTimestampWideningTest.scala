package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SparkSecretRedaction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

import scala.collection.immutable.ArraySeq

class CassandraTimestampWideningTest extends munit.FunSuite {

  private lazy val spark: SparkSession = {
    val sparkConf = new SparkConf(false)
    SparkSecretRedaction.ensureMigratorRedactionRegex(sparkConf)
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[1]")
      .appName("CassandraTimestampWideningTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("widenCqlTimestamps replaces TimestampType with LongType at top level") {
    val input = StructType(
      Seq(
        StructField("id", LongType, nullable            = false),
        StructField("validity", TimestampType, nullable = true),
        StructField("name", StringType, nullable        = true)
      )
    )

    val widened = Cassandra.widenCqlTimestamps(input).asInstanceOf[StructType]

    assertEquals(widened.fields(0), StructField("id", LongType, nullable = false))
    assertEquals(widened.fields(1), StructField("validity", LongType, nullable = true))
    assertEquals(widened.fields(2), StructField("name", StringType, nullable = true))
  }

  test("widenCqlTimestamps recurses into ArrayType, MapType, and nested StructType") {
    val nestedUdt = StructType(Seq(StructField("ts", TimestampType, nullable = true)))
    val input = StructType(
      Seq(
        StructField("times", ArrayType(TimestampType, containsNull = true)),
        StructField("by_ts", MapType(TimestampType, StringType)),
        StructField("by_str", MapType(StringType, TimestampType)),
        StructField("udt", nestedUdt)
      )
    )

    val widened = Cassandra.widenCqlTimestamps(input).asInstanceOf[StructType]

    assertEquals(
      widened.fields(0).dataType,
      ArrayType(LongType, containsNull = true): org.apache.spark.sql.types.DataType
    )
    assertEquals(
      widened.fields(1).dataType,
      MapType(LongType, StringType): org.apache.spark.sql.types.DataType
    )
    assertEquals(
      widened.fields(2).dataType,
      MapType(StringType, LongType): org.apache.spark.sql.types.DataType
    )
    assertEquals(
      widened.fields(3).dataType,
      StructType(
        Seq(StructField("ts", LongType, nullable = true))
      ): org.apache.spark.sql.types.DataType
    )
  }

  test("widenTimestampValue converts java.sql.Timestamp to epoch millis Long") {
    val ts = new java.sql.Timestamp(1700000000123L)
    assertEquals(Cassandra.widenTimestampValue(ts), 1700000000123L)
  }

  test("widenTimestampValue preserves CQL timestamp millis that overflow Spark TimestampType") {
    // 9999999999999999 ms ≈ year 318683 AD; multiplying by 1000 overflows Long, so Spark's
    // TimestampType encoder rejects it. As a bare millis Long it round-trips losslessly.
    val outOfRangeMillis = 9999999999999999L
    val ts = new java.sql.Timestamp(outOfRangeMillis)
    assertEquals(Cassandra.widenTimestampValue(ts), outOfRangeMillis)
  }

  test("widenTimestampValue handles java.util.Date and java.time.Instant") {
    val d = new java.util.Date(42L)
    val i = java.time.Instant.ofEpochMilli(43L)
    assertEquals(Cassandra.widenTimestampValue(d), 42L)
    assertEquals(Cassandra.widenTimestampValue(i), 43L)
  }

  test("widenTimestampValue recurses into Row, Set, List, Map") {
    val ts1 = new java.sql.Timestamp(10L)
    val ts2 = new java.sql.Timestamp(20L)

    assertEquals(Cassandra.widenTimestampValue(Row(ts1, "x")), Row(10L, "x"))
    assertEquals(Cassandra.widenTimestampValue(List(ts1, ts2)), List(10L, 20L))
    assertEquals(Cassandra.widenTimestampValue(Set(ts1, ts2)), Set(10L, 20L))
    assertEquals(
      Cassandra.widenTimestampValue(Map("a" -> ts1, "b" -> ts2)),
      Map("a" -> 10L, "b" -> 20L)
    )
  }

  test("widenTimestampValue passes through values it does not need to convert") {
    assertEquals(Cassandra.widenTimestampValue(42L), 42L)
    assertEquals(Cassandra.widenTimestampValue("hello"), "hello")
    assertEquals(Cassandra.widenTimestampValue(null), null)
  }

  test("createDataFrame succeeds with widened schema for an out-of-range CQL timestamp") {
    val outOfRangeMillis = 9999999999999999L
    val widenedSchema = Cassandra
      .widenCqlTimestamps(
        StructType(
          Seq(
            StructField("id", LongType, nullable            = false),
            StructField("validity", TimestampType, nullable = true)
          )
        )
      )
      .asInstanceOf[StructType]

    val raw = new java.sql.Timestamp(outOfRangeMillis)
    val row = Row.fromSeq(ArraySeq[Any](1L, raw).map(Cassandra.widenTimestampValue))

    val rdd = spark.sparkContext.parallelize(Seq(row))
    val df = spark.createDataFrame(rdd, widenedSchema)

    val collected = df.collect().toList
    assertEquals(collected.size, 1)
    assertEquals(collected.head.getAs[Long]("id"), 1L)
    assertEquals(collected.head.getAs[Long]("validity"), outOfRangeMillis)
  }
}
