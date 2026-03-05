package com.scylladb.migrator.writers

import com.scylladb.migrator.readers.DynamoDBS3Export.itemDecoder
import io.circe.parser.decode
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util.Base64
import scala.jdk.CollectionConverters._

class DynamoDBS3ExportEncoderTest extends munit.FunSuite {

  /** Encode an item to DynamoDB JSON, then decode it back and compare. */
  private def roundtrip(item: Map[String, AttributeValue]): Map[String, AttributeValue] = {
    val json = DynamoDBS3Export.encodeItem(item)
    decode(json)(itemDecoder).fold(error => fail(s"Roundtrip decode failed: $error"), identity)
  }

  test("Roundtrip String (S)") {
    val item = Map("name" -> AttributeValue.fromS("hello"))
    assertEquals(roundtrip(item), item)
  }

  test("Roundtrip Number (N)") {
    val item = Map("age" -> AttributeValue.fromN("42"))
    assertEquals(roundtrip(item), item)
  }

  test("Roundtrip Boolean (BOOL)") {
    val item = Map("active" -> AttributeValue.fromBool(true))
    assertEquals(roundtrip(item), item)
  }

  test("Roundtrip Null (NULL)") {
    val item = Map("nothing" -> AttributeValue.fromNul(true))
    assertEquals(roundtrip(item), item)
  }

  test("Roundtrip Binary (B)") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val item = Map("data" -> AttributeValue.fromB(SdkBytes.fromByteArray(bytes)))
    val result = roundtrip(item)
    assertEquals(result("data").b().asByteArray().toList, bytes.toList)
  }

  test("Roundtrip String Set (SS)") {
    val item = Map("tags" -> AttributeValue.fromSs(List("a", "b", "c").asJava))
    val result = roundtrip(item)
    assertEquals(result("tags").ss().asScala.toSet, Set("a", "b", "c"))
  }

  test("Roundtrip Number Set (NS)") {
    val item = Map("nums" -> AttributeValue.fromNs(List("1", "2", "3").asJava))
    val result = roundtrip(item)
    assertEquals(result("nums").ns().asScala.toSet, Set("1", "2", "3"))
  }

  test("Roundtrip Binary Set (BS)") {
    val b1 = SdkBytes.fromByteArray(Array[Byte](1, 2))
    val b2 = SdkBytes.fromByteArray(Array[Byte](3, 4))
    val item = Map("bins" -> AttributeValue.fromBs(List(b1, b2).asJava))
    val result = roundtrip(item)
    val decoded = result("bins").bs().asScala.map(_.asByteArray().toList).toSet
    assertEquals(decoded, Set(List[Byte](1, 2), List[Byte](3, 4)))
  }

  test("Roundtrip List (L) with mixed types") {
    val list = List(
      AttributeValue.fromS("a"),
      AttributeValue.fromN("1"),
      AttributeValue.fromBool(false)
    )
    val item = Map("list" -> AttributeValue.fromL(list.asJava))
    val result = roundtrip(item)
    val decoded = result("list").l().asScala.toList
    assertEquals(decoded(0), AttributeValue.fromS("a"))
    assertEquals(decoded(1), AttributeValue.fromN("1"))
    assertEquals(decoded(2), AttributeValue.fromBool(false))
  }

  test("Roundtrip Map (M) with nested attributes") {
    val nested = Map(
      "k1" -> AttributeValue.fromS("v1"),
      "k2" -> AttributeValue.fromN("99")
    )
    val item = Map("nested" -> AttributeValue.fromM(nested.asJava))
    val result = roundtrip(item)
    val decodedMap = result("nested").m().asScala
    assertEquals(decodedMap("k1"), AttributeValue.fromS("v1"))
    assertEquals(decodedMap("k2"), AttributeValue.fromN("99"))
  }

  test("Roundtrip multi-attribute item") {
    val item = Map(
      "id"     -> AttributeValue.fromS("pk-1"),
      "name"   -> AttributeValue.fromS("Alice"),
      "age"    -> AttributeValue.fromN("30"),
      "active" -> AttributeValue.fromBool(true)
    )
    val result = roundtrip(item)
    assertEquals(result.size, 4)
    assertEquals(result("id"), AttributeValue.fromS("pk-1"))
    assertEquals(result("name"), AttributeValue.fromS("Alice"))
    assertEquals(result("age"), AttributeValue.fromN("30"))
    assertEquals(result("active"), AttributeValue.fromBool(true))
  }

  test("Roundtrip deeply nested structure") {
    val item = Map(
      "id" -> AttributeValue.fromS("nested-1"),
      "data" -> AttributeValue.fromM(
        Map(
          "list" -> AttributeValue.fromL(
            List(
              AttributeValue.fromM(
                Map("inner" -> AttributeValue.fromS("deep")).asJava
              )
            ).asJava
          )
        ).asJava
      )
    )
    val result = roundtrip(item)
    val inner = result("data").m().get("list").l().get(0).m().get("inner")
    assertEquals(inner, AttributeValue.fromS("deep"))
  }

  test("Strings with special characters are escaped properly") {
    val item = Map("msg" -> AttributeValue.fromS("hello \"world\"\nnewline\ttab\\backslash"))
    val result = roundtrip(item)
    assertEquals(result("msg").s(), "hello \"world\"\nnewline\ttab\\backslash")
  }

  test("Roundtrip empty List (L: [])") {
    val item = Map("emptyList" -> AttributeValue.fromL(List.empty[AttributeValue].asJava))
    val result = roundtrip(item)
    assertEquals(result("emptyList").l().asScala.toList, List.empty)
  }

  test("Roundtrip empty Map (M: {})") {
    val item = Map("emptyMap" -> AttributeValue.fromM(Map.empty[String, AttributeValue].asJava))
    val result = roundtrip(item)
    assertEquals(result("emptyMap").m().asScala.toMap, Map.empty[String, AttributeValue])
  }

  test("Roundtrip empty String Set (SS: [])") {
    val item = Map("emptySS" -> AttributeValue.fromSs(List.empty[String].asJava))
    val result = roundtrip(item)
    assertEquals(result("emptySS").ss().asScala.toList, List.empty)
  }

  test("Roundtrip empty Number Set (NS: [])") {
    val item = Map("emptyNS" -> AttributeValue.fromNs(List.empty[String].asJava))
    val result = roundtrip(item)
    assertEquals(result("emptyNS").ns().asScala.toList, List.empty)
  }

  test("Roundtrip empty Binary Set (BS: [])") {
    val item = Map("emptyBS" -> AttributeValue.fromBs(List.empty[SdkBytes].asJava))
    val result = roundtrip(item)
    assertEquals(result("emptyBS").bs().asScala.toList, List.empty)
  }

  test("Roundtrip empty item") {
    val item = Map.empty[String, AttributeValue]
    val result = roundtrip(item)
    assertEquals(result, item)
  }

  test("encodeItem produces valid DynamoDB JSON format") {
    val item = Map("id" -> AttributeValue.fromS("test"))
    val json = DynamoDBS3Export.encodeItem(item)
    assert(json.startsWith("{\"Item\":{"), s"Expected DynamoDB JSON wrapper, got: $json")
    assert(json.endsWith("}}"), s"Expected closing braces, got: $json")
  }
}
