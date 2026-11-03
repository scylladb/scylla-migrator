package com.scylladb.migrator.writers

import com.datastax.spark.connector.types.CassandraOption

class ScyllaDecimalStrippingTest extends munit.FunSuite {

  private def dec(s: String) = new java.math.BigDecimal(s)

  test("top-level decimals are stripped") {
    assertEquals(Scylla.stripDecimalTrailingZeros(dec("1.500")), dec("1.5"))
    assertEquals(
      Scylla.stripDecimalTrailingZeros(CassandraOption.Value(dec("2.000"))),
      CassandraOption.Value(dec("2"))
    )
  }

  test("decimals nested in collections are stripped") {
    assertEquals(
      Scylla.stripDecimalTrailingZeros(Map("a" -> dec("1.10"), "b" -> dec("3.000"))),
      Map("a" -> dec("1.1"), "b" -> dec("3"))
    )
    assertEquals(
      Scylla.stripDecimalTrailingZeros(Map(dec("1.0") -> 7)),
      Map(dec("1") -> 7)
    )
    assertEquals(Scylla.stripDecimalTrailingZeros(Seq(dec("4.20"))), Seq(dec("4.2")))
    assertEquals(Scylla.stripDecimalTrailingZeros(Set(dec("5.50"))), Set(dec("5.5")))
    assertEquals(
      Scylla.stripDecimalTrailingZeros(CassandraOption.Value(Seq(dec("6.00")))),
      CassandraOption.Value(Seq(dec("6")))
    )
  }

  test("non-decimal values are unchanged") {
    assertEquals(Scylla.stripDecimalTrailingZeros("x"), "x")
    assertEquals(Scylla.stripDecimalTrailingZeros(Map("a" -> 1)), Map("a" -> 1))
    assertEquals(Scylla.stripDecimalTrailingZeros(CassandraOption.Unset), CassandraOption.Unset)
    assertEquals(Scylla.stripDecimalTrailingZeros(null), null)
  }
}
