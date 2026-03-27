package com.scylladb.migrator.alternator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

class DdbValueTest extends munit.FunSuite {

  test("Round-trip: String") {
    val av = AttributeValue.fromS("hello")
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: Number") {
    val av = AttributeValue.fromN("42.5")
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: Boolean") {
    val av = AttributeValue.fromBool(true)
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: Null") {
    val av = AttributeValue.fromNul(true)
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: Binary") {
    val av = AttributeValue.fromB(SdkBytes.fromUtf8String("bytes"))
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: String Set") {
    val av = AttributeValue.fromSs(java.util.List.of("a", "b", "c"))
    val roundTripped = DdbValue.toAttributeValue(DdbValue.from(av))
    assertEquals(roundTripped.ss().asScala.toSet, av.ss().asScala.toSet)
  }

  test("Round-trip: Number Set") {
    val av = AttributeValue.fromNs(java.util.List.of("1", "2", "3"))
    val roundTripped = DdbValue.toAttributeValue(DdbValue.from(av))
    assertEquals(roundTripped.ns().asScala.toSet, av.ns().asScala.toSet)
  }

  test("Round-trip: Binary Set") {
    val bs = java.util.List.of(
      SdkBytes.fromUtf8String("alpha"),
      SdkBytes.fromUtf8String("beta")
    )
    val av = AttributeValue.fromBs(bs)
    val roundTripped = DdbValue.toAttributeValue(DdbValue.from(av))
    assertEquals(roundTripped.bs().asScala.toSet, av.bs().asScala.toSet)
  }

  test("Round-trip: List") {
    val av = AttributeValue.fromL(
      java.util.List.of(
        AttributeValue.fromS("nested"),
        AttributeValue.fromN("99")
      )
    )
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: Map") {
    val av = AttributeValue.fromM(
      Map(
        "key1" -> AttributeValue.fromS("val1"),
        "key2" -> AttributeValue.fromN("100")
      ).asJava
    )
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

  test("Round-trip: nested Map containing List and Sets") {
    val inner = AttributeValue.fromM(
      Map(
        "tags" -> AttributeValue.fromSs(java.util.List.of("x", "y")),
        "scores" -> AttributeValue.fromL(
          java.util.List.of(
            AttributeValue.fromN("1"),
            AttributeValue.fromN("2")
          )
        )
      ).asJava
    )
    val av = AttributeValue.fromM(Map("nested" -> inner).asJava)
    assertEquals(DdbValue.toAttributeValue(DdbValue.from(av)), av)
  }

}
