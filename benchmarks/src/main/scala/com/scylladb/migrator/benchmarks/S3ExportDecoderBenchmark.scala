package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.readers.DynamoDBS3Export
import io.circe.parser.decode
import org.openjdk.jmh.annotations._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class S3ExportDecoderBenchmark {

  private var simpleJson: String = _
  private var multiAttrJson: String = _
  private var nestedJson: String = _
  private var wideJson: String = _
  private var deeplyNestedJson: String = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    simpleJson = """{"Item":{"name":{"S":"hello"}}}"""

    multiAttrJson =
      """{"Item":{"pk":{"S":"user-123"},"age":{"N":"30"},"active":{"BOOL":true},"empty":{"NULL":true}}}"""

    nestedJson =
      """{"Item":{"pk":{"S":"order-456"},"address":{"M":{"street":{"S":"123 Main St"},"city":{"S":"Springfield"},"zip":{"N":"62701"}}},"tags":{"L":[{"S":"urgent"},{"S":"reviewed"},{"N":"42"}]}}}"""

    // Wide item: 50 attributes (typical production item)
    val wideAttrs = (1 to 50)
      .map(j => s""""col$j":{"S":"value-$j"}""")
      .mkString(",")
    wideJson = s"""{"Item":{"pk":{"S":"wide-1"},$wideAttrs}}"""

    // Deeply nested item: 3 levels of Map nesting
    deeplyNestedJson =
      """{"Item":{"pk":{"S":"deep-1"},"l1":{"M":{"a":{"M":{"b":{"M":{"c":{"S":"deep"},"d":{"N":"42"}}},"e":{"L":[{"S":"x"},{"N":"1"}]}}},"f":{"S":"shallow"}}}}}"""
  }

  @Benchmark
  def decodeSimpleItem(): Map[String, AttributeValue] =
    decode(simpleJson)(DynamoDBS3Export.itemDecoder).fold(throw _, identity)

  @Benchmark
  def decodeMultiAttrItem(): Map[String, AttributeValue] =
    decode(multiAttrJson)(DynamoDBS3Export.itemDecoder).fold(throw _, identity)

  @Benchmark
  def decodeNestedItem(): Map[String, AttributeValue] =
    decode(nestedJson)(DynamoDBS3Export.itemDecoder).fold(throw _, identity)

  @Benchmark
  def decodeWideItem(): Map[String, AttributeValue] =
    decode(wideJson)(DynamoDBS3Export.itemDecoder).fold(throw _, identity)

  @Benchmark
  def decodeDeeplyNestedItem(): Map[String, AttributeValue] =
    decode(deeplyNestedJson)(DynamoDBS3Export.itemDecoder).fold(throw _, identity)
}
