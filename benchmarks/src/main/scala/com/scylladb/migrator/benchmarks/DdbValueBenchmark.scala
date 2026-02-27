package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.alternator.DdbValue
import org.openjdk.jmh.annotations._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class DdbValueBenchmark {

  private var flatItem: AttributeValue = _
  private var nestedItem: AttributeValue = _
  private var setItem: AttributeValue = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    // Flat item: a Map with simple scalar attributes
    flatItem = AttributeValue
      .fromM(
        Map(
          "pk"     -> AttributeValue.fromS("user-123"),
          "sk"     -> AttributeValue.fromS("2024-01-15"),
          "name"   -> AttributeValue.fromS("Alice"),
          "age"    -> AttributeValue.fromN("30"),
          "active" -> AttributeValue.fromBool(true),
          "empty"  -> AttributeValue.fromNul(true)
        ).asJava
      )

    // Nested item: Map containing a nested Map and a List
    nestedItem = AttributeValue
      .fromM(
        Map(
          "pk" -> AttributeValue.fromS("order-456"),
          "address" -> AttributeValue.fromM(
            Map(
              "street" -> AttributeValue.fromS("123 Main St"),
              "city"   -> AttributeValue.fromS("Springfield"),
              "zip"    -> AttributeValue.fromN("62701")
            ).asJava
          ),
          "tags" -> AttributeValue.fromL(
            java.util.Arrays.asList(
              AttributeValue.fromS("urgent"),
              AttributeValue.fromS("reviewed"),
              AttributeValue.fromN("42")
            )
          )
        ).asJava
      )

    // Set item: contains SS, NS
    setItem = AttributeValue
      .fromM(
        Map(
          "pk"     -> AttributeValue.fromS("item-789"),
          "colors" -> AttributeValue.fromSs(java.util.Arrays.asList("red", "green", "blue")),
          "scores" -> AttributeValue.fromNs(java.util.Arrays.asList("100", "200", "300"))
        ).asJava
      )
  }

  @Benchmark
  def convertFlatItem(): Map[String, DdbValue] =
    flatItem.m().asScala.view.mapValues(DdbValue.from).toMap

  @Benchmark
  def convertNestedItem(): Map[String, DdbValue] =
    nestedItem.m().asScala.view.mapValues(DdbValue.from).toMap

  @Benchmark
  def convertSetItem(): Map[String, DdbValue] =
    setItem.m().asScala.view.mapValues(DdbValue.from).toMap
}
