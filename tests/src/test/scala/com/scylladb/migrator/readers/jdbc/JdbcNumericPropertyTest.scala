package com.scylladb.migrator.readers.jdbc

class JdbcNumericPropertyTest extends munit.FunSuite {

  private val spec = JdbcNumericProperty.Spec(
    name         = "maxAllowedPacket",
    minimum      = 1L,
    maximum      = Long.MaxValue,
    defaultValue = 1024L,
    expectation  = "a positive integer number of bytes"
  )

  test("parse returns the default when the key is absent") {
    assertEquals(JdbcNumericProperty.parse(Map.empty[String, String], spec), Right(1024L))
  }

  test("parse accepts case-insensitive lookups") {
    val result = JdbcNumericProperty.parse(Map("MAXALLOWEDPACKET" -> "4096"), spec)
    assertEquals(result, Right(4096L))
  }

  test("parse rejects non-numeric values with a stable error message") {
    val result = JdbcNumericProperty.parse(Map("maxAllowedPacket" -> "notanumber"), spec)
    assertEquals(
      result,
      Left("maxAllowedPacket must be a positive integer number of bytes, got: 'notanumber'")
    )
  }

  test("parse rejects injection attempts containing '&'") {
    val result = JdbcNumericProperty.parse(
      Map("maxAllowedPacket" -> "5000&allowLoadLocalInfile=true"),
      spec
    )
    assert(result.isLeft)
  }

  test("parse rejects values below the configured minimum") {
    val result = JdbcNumericProperty.parse(Map("maxAllowedPacket" -> "0"), spec)
    assert(result.isLeft)
  }

  test("parse trims surrounding whitespace before validating") {
    val result = JdbcNumericProperty.parse(Map("maxAllowedPacket" -> "  1000  "), spec)
    assertEquals(result, Right(1000L))
  }

  test("parse rejects embedded whitespace even after trim") {
    // The trim only strips leading/trailing whitespace. Embedded whitespace remains a
    // non-`\d+` pattern and falls to the error branch, preserving the injection defense.
    val result = JdbcNumericProperty.parse(Map("maxAllowedPacket" -> "10 00"), spec)
    assert(result.isLeft)
    assert(result.left.exists(_.contains("'10 00'")), s"Got: $result")
  }

  test("parse rejects duplicate entries with different casing") {
    val result = JdbcNumericProperty.parse(
      Map("maxAllowedPacket" -> "1024", "MAXALLOWEDPACKET" -> "2048"),
      spec
    )

    assert(result.isLeft)
    assert(result.left.exists(_.contains("duplicate entries")))
    assert(result.left.exists(_.contains("'maxAllowedPacket'")))
  }

  test("validateAll collects errors from every spec and ignores absent keys") {
    val socketTimeoutSpec = spec.copy(
      name         = "socketTimeout",
      minimum      = 0L,
      maximum      = Int.MaxValue.toLong,
      defaultValue = 0L,
      expectation  = "a non-negative integer number of milliseconds"
    )

    val errors = JdbcNumericProperty.validateAll(
      Some(
        Map(
          "maxAllowedPacket" -> "0",
          "socketTimeout"    -> "not_a_number"
        )
      ),
      Seq(spec, socketTimeoutSpec)
    )

    assertEquals(errors.size, 2)
    assert(errors.exists(_.contains("maxAllowedPacket")))
    assert(errors.exists(_.contains("socketTimeout")))
  }

  test("parseOrThrow surfaces the validation error as IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      JdbcNumericProperty.parseOrThrow(Map("maxAllowedPacket" -> "abc"), spec)
    }
    assert(ex.getMessage.contains("maxAllowedPacket"))
  }
}
