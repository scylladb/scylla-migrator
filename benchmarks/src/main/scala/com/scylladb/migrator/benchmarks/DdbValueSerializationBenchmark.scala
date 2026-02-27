package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.alternator.DdbValue
import org.openjdk.jmh.annotations._

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class DdbValueSerializationBenchmark {

  private var flatItem: Map[String, DdbValue] = _
  private var nestedItem: Map[String, DdbValue] = _
  private var flatItemBytes: Array[Byte] = _
  private var nestedItemBytes: Array[Byte] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    flatItem = Map(
      "pk"     -> DdbValue.S("user-123"),
      "sk"     -> DdbValue.S("2024-01-15"),
      "name"   -> DdbValue.S("Alice"),
      "age"    -> DdbValue.N("30"),
      "active" -> DdbValue.Bool(true),
      "empty"  -> DdbValue.Null(true)
    )

    nestedItem = Map(
      "pk" -> DdbValue.S("order-456"),
      "address" -> DdbValue.M(
        Map(
          "street" -> DdbValue.S("123 Main St"),
          "city"   -> DdbValue.S("Springfield"),
          "zip"    -> DdbValue.N("62701")
        )
      ),
      "tags"   -> DdbValue.L(Seq(DdbValue.S("urgent"), DdbValue.S("reviewed"), DdbValue.N("42"))),
      "scores" -> DdbValue.Ns(Seq("100", "200", "300"))
    )

    flatItemBytes   = serialize(flatItem)
    nestedItemBytes = serialize(nestedItem)
  }

  private def serialize(obj: AnyRef): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }

  private def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject().asInstanceOf[T]
  }

  @Benchmark
  def serialize_flatItem(): Array[Byte] =
    serialize(flatItem)

  @Benchmark
  def serialize_nestedItem(): Array[Byte] =
    serialize(nestedItem)

  @Benchmark
  def deserialize_flatItem(): Map[String, DdbValue] =
    deserialize(flatItemBytes)

  @Benchmark
  def deserialize_nestedItem(): Map[String, DdbValue] =
    deserialize(nestedItemBytes)

  @Benchmark
  def roundtrip_flatItem(): Map[String, DdbValue] =
    deserialize(serialize(flatItem))

  @Benchmark
  def roundtrip_nestedItem(): Map[String, DdbValue] =
    deserialize(serialize(nestedItem))
}
