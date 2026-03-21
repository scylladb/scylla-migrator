package com.scylladb.migrator.config

import io.circe.Json
import io.circe.syntax._

class TTLConfigTest extends munit.FunSuite {

  test("decode plain number defaults to set-if-missing") {
    val result = Json.fromLong(7776000L).as[TTLConfig]
    assertEquals(result, Right(TTLConfig(7776000L, TTLPolicy.SetIfMissing)))
  }

  test("decode string with set-if-missing policy") {
    val result = Json.fromString("7776000:set-if-missing").as[TTLConfig]
    assertEquals(result, Right(TTLConfig(7776000L, TTLPolicy.SetIfMissing)))
  }

  test("decode string with always policy") {
    val result = Json.fromString("3600:always").as[TTLConfig]
    assertEquals(result, Right(TTLConfig(3600L, TTLPolicy.Always)))
  }

  test("decode string with update-if-present policy") {
    val result = Json.fromString("86400:update-if-present").as[TTLConfig]
    assertEquals(result, Right(TTLConfig(86400L, TTLPolicy.UpdateIfPresent)))
  }

  test("decode rejects negative value") {
    val result = Json.fromLong(-1L).as[TTLConfig]
    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("must be positive")))
  }

  test("decode rejects zero value") {
    val result = Json.fromLong(0L).as[TTLConfig]
    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("must be positive")))
  }

  test("decode rejects value exceeding Int.MaxValue") {
    val result = Json.fromLong(Int.MaxValue.toLong + 1).as[TTLConfig]
    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("must not exceed")))
  }

  test("decode rejects unknown policy") {
    val result = Json.fromString("3600:bogus").as[TTLConfig]
    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("Unknown TTL policy")))
  }

  test("decode rejects non-numeric value in string") {
    val result = Json.fromString("abc:always").as[TTLConfig]
    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("Invalid ttl value")))
  }

  test("decode plain number as string defaults to set-if-missing") {
    val result = Json.fromString("3600").as[TTLConfig]
    assertEquals(result, Right(TTLConfig(3600L, TTLPolicy.SetIfMissing)))
  }

  test("decode rejects non-numeric string without colon") {
    val result = Json.fromString("bogus").as[TTLConfig]
    assert(result.isLeft)
  }

  test("encode set-if-missing as plain number") {
    val json = TTLConfig(7776000L, TTLPolicy.SetIfMissing).asJson
    assertEquals(json, Json.fromLong(7776000L))
  }

  test("encode always as string") {
    val json = TTLConfig(3600L, TTLPolicy.Always).asJson
    assertEquals(json, Json.fromString("3600:always"))
  }

  test("encode update-if-present as string") {
    val json = TTLConfig(86400L, TTLPolicy.UpdateIfPresent).asJson
    assertEquals(json, Json.fromString("86400:update-if-present"))
  }

  test("decode handles spaces around value and policy") {
    val result = Json.fromString(" 3600 : always ").as[TTLConfig]
    assertEquals(result, Right(TTLConfig(3600L, TTLPolicy.Always)))
  }
}
