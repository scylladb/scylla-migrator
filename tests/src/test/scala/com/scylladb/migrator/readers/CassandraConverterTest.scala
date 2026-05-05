package com.scylladb.migrator.readers

import java.sql.Timestamp

class CassandraConverterTest extends munit.FunSuite {

  test("convertValue passes through an in-range java.sql.Timestamp unchanged") {
    val ts = new Timestamp(1_000_000_000_000L) // ~2001-09-09
    val result = Cassandra.convertValue(ts)
    assertEquals(result, ts)
  }

  test("convertValue clamps an over-range java.sql.Timestamp to MaxTimestampMillis") {
    val overRange = new Timestamp(9_999_999_999_999_999L) // far future, overflows millis->micros
    val result = Cassandra.convertValue(overRange)
    assertEquals(result, new Timestamp(Cassandra.MaxTimestampMillis))
  }

  test("convertValue clamps an under-range java.util.Date to MinTimestampMillis") {
    val underRange = new java.util.Date(Long.MinValue)
    val result = Cassandra.convertValue(underRange)
    assertEquals(result, new Timestamp(Cassandra.MinTimestampMillis))
  }

  test("convertValue passes through an in-range java.util.Date unchanged") {
    val d = new java.util.Date(0L) // epoch
    val result = Cassandra.convertValue(d)
    assertEquals(result, d)
  }

  test("convertValue passes through non-timestamp values unchanged") {
    assertEquals(Cassandra.convertValue("hello"), "hello")
    assertEquals(Cassandra.convertValue(42L), 42L)
    assertEquals(Cassandra.convertValue(null), null)
  }
}
