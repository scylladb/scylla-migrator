package com.scylladb.migrator.readers.jdbc

class JdbcUrlTest extends munit.FunSuite {

  test("requireSimpleDatabaseName accepts alphanumeric, underscore, dollar, hyphen") {
    JdbcUrl.requireSimpleDatabaseName("my_db")
    JdbcUrl.requireSimpleDatabaseName("my-db")
    JdbcUrl.requireSimpleDatabaseName("my$db")
    JdbcUrl.requireSimpleDatabaseName("db42")
  }

  test("requireSimpleDatabaseName rejects URL metacharacters") {
    intercept[IllegalArgumentException](JdbcUrl.requireSimpleDatabaseName("bad/name"))
    intercept[IllegalArgumentException](JdbcUrl.requireSimpleDatabaseName("bad?name"))
    intercept[IllegalArgumentException](JdbcUrl.requireSimpleDatabaseName("bad#name"))
    intercept[IllegalArgumentException](JdbcUrl.requireSimpleDatabaseName("bad&name"))
    intercept[IllegalArgumentException](JdbcUrl.requireSimpleDatabaseName(""))
  }

  test("requireValidHost rejects empty host") {
    intercept[IllegalArgumentException](JdbcUrl.requireValidHost(""))
  }

  test("requireValidHost rejects URL injection characters") {
    intercept[IllegalArgumentException](JdbcUrl.requireValidHost("evil.com/path"))
    intercept[IllegalArgumentException](JdbcUrl.requireValidHost("evil.com?q=1"))
    intercept[IllegalArgumentException](JdbcUrl.requireValidHost("evil.com#frag"))
    intercept[IllegalArgumentException](JdbcUrl.requireValidHost("user@evil.com"))
  }

  test("requireValidHost accepts hostnames, IPv4, IPv6 (bare and bracketed)") {
    JdbcUrl.requireValidHost("localhost")
    JdbcUrl.requireValidHost("db.example.com")
    JdbcUrl.requireValidHost("192.168.1.1")
    JdbcUrl.requireValidHost("::1")
    JdbcUrl.requireValidHost("[::1]")
    JdbcUrl.requireValidHost("::ffff:192.168.1.1")
  }

  test("bracketIPv6Host wraps bare IPv6 and leaves others alone") {
    assertEquals(JdbcUrl.bracketIPv6Host("::1"), "[::1]")
    assertEquals(JdbcUrl.bracketIPv6Host("[::1]"), "[::1]")
    assertEquals(JdbcUrl.bracketIPv6Host("localhost"), "localhost")
    assertEquals(JdbcUrl.bracketIPv6Host("192.168.1.1"), "192.168.1.1")
  }

  test("urlEncode encodes spaces and slashes") {
    assertEquals(JdbcUrl.urlEncode("America/Los_Angeles"), "America%2FLos_Angeles")
    assertEquals(JdbcUrl.urlEncode("a b"), "a+b")
  }

  test("encodeQueryParams produces an ordered &-joined string with encoded keys and values") {
    val encoded = JdbcUrl.encodeQueryParams(
      Seq(
        "zone"            -> "America/Los_Angeles",
        "useCursorFetch"  -> "true",
        "connectionTime " -> "5000"
      )
    )

    assertEquals(
      encoded,
      "zone=America%2FLos_Angeles&useCursorFetch=true&connectionTime+=5000"
    )
  }
}
