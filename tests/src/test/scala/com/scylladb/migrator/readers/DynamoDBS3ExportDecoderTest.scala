package com.scylladb.migrator.readers

import io.circe.parser.decode
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util.Base64
import scala.jdk.CollectionConverters._

class DynamoDBS3ExportDecoderTest extends munit.FunSuite {

  private def decodeItem(json: String): Map[String, AttributeValue] =
    decode(json)(DynamoDBS3Export.itemDecoder)
      .fold(error => fail(s"Decoding failed: $error"), identity)

  test("Decode String (S)") {
    val result = decodeItem("""{"Item":{"name":{"S":"hello"}}}""")
    assertEquals(result("name"), AttributeValue.fromS("hello"))
  }

  test("Decode Number (N)") {
    val result = decodeItem("""{"Item":{"age":{"N":"42"}}}""")
    assertEquals(result("age"), AttributeValue.fromN("42"))
  }

  test("Decode Boolean (BOOL)") {
    val result = decodeItem("""{"Item":{"active":{"BOOL":true}}}""")
    assertEquals(result("active"), AttributeValue.fromBool(true))
  }

  test("Decode Null (NULL)") {
    val result = decodeItem("""{"Item":{"nothing":{"NULL":true}}}""")
    assertEquals(result("nothing"), AttributeValue.fromNul(true))
  }

  test("Decode Binary (B) — Base64") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val b64 = Base64.getEncoder.encodeToString(bytes)
    val result = decodeItem(s"""{"Item":{"data":{"B":"$b64"}}}""")
    assertEquals(result("data").b().asByteArray().toList, bytes.toList)
  }

  test("Decode String Set (SS)") {
    val result = decodeItem("""{"Item":{"tags":{"SS":["a","b","c"]}}}""")
    assertEquals(result("tags").ss().asScala.toSet, Set("a", "b", "c"))
  }

  test("Decode Number Set (NS)") {
    val result = decodeItem("""{"Item":{"nums":{"NS":["1","2","3"]}}}""")
    assertEquals(result("nums").ns().asScala.toSet, Set("1", "2", "3"))
  }

  test("Decode Binary Set (BS)") {
    val b1 = Base64.getEncoder.encodeToString(Array[Byte](1, 2))
    val b2 = Base64.getEncoder.encodeToString(Array[Byte](3, 4))
    val result = decodeItem(s"""{"Item":{"bins":{"BS":["$b1","$b2"]}}}""")
    val decoded = result("bins").bs().asScala.map(_.asByteArray().toList).toSet
    assertEquals(decoded, Set(List[Byte](1, 2), List[Byte](3, 4)))
  }

  test("Decode List (L) with mixed types") {
    val result = decodeItem("""{"Item":{"list":{"L":[{"S":"a"},{"N":"1"},{"BOOL":false}]}}}""")
    val list = result("list").l().asScala.toList
    assertEquals(list(0), AttributeValue.fromS("a"))
    assertEquals(list(1), AttributeValue.fromN("1"))
    assertEquals(list(2), AttributeValue.fromBool(false))
  }

  test("Decode Map (M) with nested attributes") {
    val result = decodeItem("""{"Item":{"nested":{"M":{"k1":{"S":"v1"},"k2":{"N":"99"}}}}}""")
    val map = result("nested").m().asScala
    assertEquals(map("k1"), AttributeValue.fromS("v1"))
    assertEquals(map("k2"), AttributeValue.fromN("99"))
  }

  test("Unknown type produces error") {
    val json = """{"Item":{"bad":{"UNKNOWN":"value"}}}"""
    // The implementation calls sys.error for unknown types, throwing a RuntimeException
    intercept[RuntimeException] {
      decode(json)(DynamoDBS3Export.itemDecoder)
        .fold(throw _, identity)
    }
  }

  test("Multi-attribute item decoding") {
    val result = decodeItem(
      """{"Item":{"name":{"S":"Alice"},"age":{"N":"30"},"active":{"BOOL":true}}}"""
    )
    assertEquals(result.size, 3)
    assertEquals(result("name"), AttributeValue.fromS("Alice"))
    assertEquals(result("age"), AttributeValue.fromN("30"))
    assertEquals(result("active"), AttributeValue.fromBool(true))
  }
}
