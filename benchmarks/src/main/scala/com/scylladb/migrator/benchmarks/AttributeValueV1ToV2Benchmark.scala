package com.scylladb.migrator.benchmarks

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
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
class AttributeValueV1ToV2Benchmark {

  private var flatItems: java.util.Map[String, AttributeValueV1] = _
  private var nestedItems: java.util.Map[String, AttributeValueV1] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    flatItems = Map(
      "pk"     -> new AttributeValueV1().withS("user-123"),
      "sk"     -> new AttributeValueV1().withS("2024-01-15"),
      "name"   -> new AttributeValueV1().withS("Alice"),
      "age"    -> new AttributeValueV1().withN("30"),
      "active" -> new AttributeValueV1().withBOOL(true),
      "empty"  -> new AttributeValueV1().withNULL(true)
    ).asJava

    val innerMap = Map(
      "street" -> new AttributeValueV1().withS("123 Main St"),
      "city"   -> new AttributeValueV1().withS("Springfield"),
      "zip"    -> new AttributeValueV1().withN("62701")
    ).asJava

    val innerList = java.util.Arrays.asList(
      new AttributeValueV1().withS("urgent"),
      new AttributeValueV1().withS("reviewed"),
      new AttributeValueV1().withN("42")
    )

    nestedItems = Map(
      "pk"      -> new AttributeValueV1().withS("order-456"),
      "address" -> new AttributeValueV1().withM(innerMap),
      "tags"    -> new AttributeValueV1().withL(innerList)
    ).asJava
  }

  @Benchmark
  def convertFlatItems(): java.util.Map[String, AttributeValue] = {
    val result = new java.util.HashMap[String, AttributeValue](flatItems.size())
    flatItems.forEach((k, v) => result.put(k, AttributeValueUtils.fromV1(v)))
    result
  }

  @Benchmark
  def convertNestedItems(): java.util.Map[String, AttributeValue] = {
    val result = new java.util.HashMap[String, AttributeValue](nestedItems.size())
    nestedItems.forEach((k, v) => result.put(k, AttributeValueUtils.fromV1(v)))
    result
  }
}
