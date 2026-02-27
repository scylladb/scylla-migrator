package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.readers.Cassandra
import org.apache.spark.unsafe.types.UTF8String
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class ConvertValueBenchmark {

  private var utf8Value: UTF8String = _
  private var plainString: String = _
  private var intValue: Integer = _
  private var mapValue: Map[UTF8String, UTF8String] = _
  private var listValue: List[UTF8String] = _
  private var setValue: Set[UTF8String] = _
  private var arrayBufValue: ArrayBuffer[UTF8String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    utf8Value   = UTF8String.fromString("hello-world")
    plainString = "hello-world"
    intValue    = 42
    mapValue = Map(
      UTF8String.fromString("k1") -> UTF8String.fromString("v1"),
      UTF8String.fromString("k2") -> UTF8String.fromString("v2"),
      UTF8String.fromString("k3") -> UTF8String.fromString("v3")
    )
    listValue = List(
      UTF8String.fromString("a"),
      UTF8String.fromString("b"),
      UTF8String.fromString("c")
    )
    setValue = Set(
      UTF8String.fromString("x"),
      UTF8String.fromString("y"),
      UTF8String.fromString("z")
    )
    arrayBufValue = ArrayBuffer(
      UTF8String.fromString("p"),
      UTF8String.fromString("q"),
      UTF8String.fromString("r")
    )
  }

  @Benchmark
  def convert_utf8String(): Any =
    Cassandra.convertValue(utf8Value)

  @Benchmark
  def convert_plainString(): Any =
    Cassandra.convertValue(plainString)

  @Benchmark
  def convert_int(): Any =
    Cassandra.convertValue(intValue)

  @Benchmark
  def convert_map(): Any =
    Cassandra.convertValue(mapValue)

  @Benchmark
  def convert_list(): Any =
    Cassandra.convertValue(listValue)

  @Benchmark
  def convert_set(): Any =
    Cassandra.convertValue(setValue)

  @Benchmark
  def convert_arrayBuffer(): Any =
    Cassandra.convertValue(arrayBufValue)
}
