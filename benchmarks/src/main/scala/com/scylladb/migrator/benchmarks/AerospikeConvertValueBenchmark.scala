package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.readers.AerospikeTypes
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

/** Microbenchmarks for Aerospike value conversion and type inference. */
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class AerospikeConvertValueBenchmark {

  var longValue: java.lang.Long = _
  var intValue: java.lang.Integer = _
  var doubleValue: java.lang.Double = _
  var stringValue: String = _
  var boolValue: java.lang.Boolean = _
  var bytesValue: Array[Byte] = _
  var listValue: java.util.List[String] = _
  var mapValue: java.util.Map[String, String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    longValue   = java.lang.Long.valueOf(42L)
    intValue    = java.lang.Integer.valueOf(42)
    doubleValue = java.lang.Double.valueOf(3.14)
    stringValue = "hello world"
    boolValue   = java.lang.Boolean.TRUE
    bytesValue  = Array[Byte](1, 2, 3, 4, 5)
    listValue   = java.util.Arrays.asList("a", "b", "c")
    mapValue    = new java.util.HashMap[String, String]()
    mapValue.put("key1", "val1")
    mapValue.put("key2", "val2")
  }

  @Benchmark
  def convertLong(): Any = AerospikeTypes.convertValue(longValue, LongType)

  @Benchmark
  def convertInt(): Any = AerospikeTypes.convertValue(intValue, LongType)

  @Benchmark
  def convertDouble(): Any = AerospikeTypes.convertValue(doubleValue, DoubleType)

  @Benchmark
  def convertString(): Any = AerospikeTypes.convertValue(stringValue, StringType)

  @Benchmark
  def convertBooleanPassthrough(): Any = AerospikeTypes.convertValue(boolValue, BooleanType)

  @Benchmark
  def convertBooleanToString(): Any = AerospikeTypes.convertValue(boolValue, StringType)

  @Benchmark
  def convertBytes(): Any = AerospikeTypes.convertValue(bytesValue, BinaryType)

  @Benchmark
  def convertList(): Any =
    AerospikeTypes.convertValue(listValue, ArrayType(StringType, containsNull = true))

  @Benchmark
  def convertMap(): Any =
    AerospikeTypes.convertValue(
      mapValue,
      MapType(StringType, StringType, valueContainsNull = true)
    )

  @Benchmark
  def inferTypeLong(): Any = AerospikeTypes.inferSparkType(longValue)

  @Benchmark
  def inferTypeString(): Any = AerospikeTypes.inferSparkType(stringValue)

  @Benchmark
  def inferTypeList(): Any = AerospikeTypes.inferSparkType(listValue)

  @Benchmark
  def inferTypeMap(): Any = AerospikeTypes.inferSparkType(mapValue)
}
