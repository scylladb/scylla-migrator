package com.scylladb.migrator.readers

import com.aerospike.client.{ Key, Record }
import com.scylladb.migrator.config.{ AerospikeInvalidRecordHandling, AerospikeKeyMode }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.jdk.CollectionConverters._

class AerospikeRecordPolicyTest extends munit.FunSuite {
  private def config(mode: AerospikeKeyMode, policy: AerospikeInvalidRecordHandling) =
    AerospikeReadConfig(mode, "ns", "set", 1, 1, 1, 0, 1, "id", None, None, policy)

  private def convert(
    key: Key,
    mode: AerospikeKeyMode,
    policy: AerospikeInvalidRecordHandling,
    bad: Any,
    declared: DataType
  ): Option[Row] = AerospikeRDD.recordToRow(
    key,
    new Record(
      Map[String, Object]("good" -> "preserved", "bad" -> bad.asInstanceOf[Object]).asJava,
      0,
      0
    ),
    StructType(
      Seq(
        StructField("id", AerospikeTypes.keyDataType(mode), nullable = false),
        StructField("good", StringType),
        StructField("bad", declared)
      )
    ),
    config(mode, policy),
    0L
  )

  private val policies =
    Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)

  test("invalid scalar or collection replaces the whole declared bin only in null mode") {
    val collision = new java.util.LinkedHashMap[Any, Any]()
    collision.put(1L, "number")
    collision.put("1", "string")
    val badValues = Seq[(Any, DataType)](
      "private-source-content"                         -> LongType,
      9007199254740993L                                -> DoubleType,
      1L                                               -> BooleanType,
      java.util.Arrays.asList[Any](1L, "bad")          -> ArrayType(LongType, true),
      java.util.Arrays.asList[Any](1L, null)           -> ArrayType(LongType, true),
      java.util.Collections.singletonMap("key", "bad") -> MapType(StringType, LongType, true),
      collision                                        -> MapType(StringType, StringType, true)
    )
    val key = new Key("ns", "set", "id")
    for ((bad, declared) <- badValues) {
      assertEquals(convert(key, AerospikeKeyMode.StringKey, policies.head, bad, declared), None)
      assertEquals(
        convert(key, AerospikeKeyMode.StringKey, policies.last, bad, declared),
        Some(Row("id", "preserved", null))
      )
    }
    assertEquals(
      convert(key, AerospikeKeyMode.StringKey, policies.head, true, BooleanType),
      Some(Row("id", "preserved", true))
    )
  }

  test("missing and mismatched stored identities skip records in both modes") {
    val keys = Seq(
      AerospikeKeyMode.StringKey  -> new Key("ns", "set", "string"),
      AerospikeKeyMode.IntegerKey -> new Key("ns", "set", 42L),
      AerospikeKeyMode.BinaryKey  -> new Key("ns", "set", Array[Byte](42))
    )
    for {
      policy       <- policies
      (mode, good) <- keys
      (other, key) <- keys if other != mode
    } {
      assertEquals(convert(key, mode, policy, 1L, LongType), None)
      val missing = new Key("ns", good.digest, "set", null)
      assertEquals(convert(missing, mode, policy, 1L, LongType), None)
      assert(convert(missing, AerospikeKeyMode.Digest, policy, 1L, LongType).isDefined)
    }
  }

  test("unexpected source conversion exceptions stay fatal in both modes") {
    val error = new IllegalStateException("unexpected formatting failure")
    val value = new Object { override def toString: String = throw error }
    for (policy <- policies)
      assertEquals(
        intercept[IllegalStateException] {
          convert(new Key("ns", "set", "id"), AerospikeKeyMode.StringKey, policy, value, StringType)
        },
        error
      )
  }

  test("read counts include records rejected for keys and bin shapes without duplicate lookahead") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("bad", LongType)))
    def record(value: Object) = new Record(Map("bad" -> value).asJava, 0, 0)
    val good = new Key("ns", "set", "good")
    val missing = new Key("ns", good.digest, "set", null)
    val input = Seq(
      missing                           -> record(Long.box(1)),
      new Key("ns", "set", 42L)         -> record(Long.box(1)),
      good                              -> record(Long.box(1)),
      new Key("ns", "set", "bad-shape") -> record("invalid")
    )
    for (policy <- policies) {
      val records = input.iterator
      val readCount = new org.apache.spark.util.LongAccumulator
      var completedReadCount = -1L
      val rows = AerospikeRDD.validRows[(Key, Record)](
        () => if (records.hasNext) Some(records.next()) else None,
        { case (key, value) =>
          AerospikeRDD
            .recordToRow(key, value, schema, config(AerospikeKeyMode.StringKey, policy), 0L)
        },
        () => completedReadCount = readCount.value,
        () => readCount.add(1L)
      )
      assertEquals(readCount.value.longValue(), 0L)
      assert(rows.hasNext)
      assertEquals(readCount.value.longValue(), 3L)
      assert(rows.hasNext)
      assertEquals(readCount.value.longValue(), 3L)
      assertEquals(rows.next(), Row("good", 1L))
      assertEquals(readCount.value.longValue(), 3L)
      val remaining = rows.toList
      assertEquals(
        remaining,
        if (policy == AerospikeInvalidRecordHandling.Null)
          List(Row("bad-shape", null))
        else Nil
      )
      assertEquals(readCount.value.longValue(), input.size.toLong)
      assertEquals(completedReadCount, input.size.toLong)
      assert(!rows.hasNext)
      intercept[NoSuchElementException](rows.next())
      assertEquals(readCount.value.longValue(), input.size.toLong)
    }
  }

}
