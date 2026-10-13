package com.scylladb.migrator.readers

class AerospikeTtlTest extends munit.FunSuite {
  private val epochMillis = 1262304000000L
  private def at(seconds: Long): Long = epochMillis + seconds * 1000L

  test("zero expiration means never expires") {
    assertEquals(AerospikeRDD.remainingTtl(0, at(1000)), -1)
  }

  test("remaining TTL reaches zero if a queued record expires before conversion") {
    assertEquals(AerospikeRDD.remainingTtl(1000, at(998)), 2)
    assertEquals(AerospikeRDD.remainingTtl(1000, at(999) + 999), 1)
    assertEquals(AerospikeRDD.remainingTtl(1000, at(1000)), 0)
    assertEquals(AerospikeRDD.remainingTtl(1000, at(1001)), 0)
    assertEquals(AerospikeRDD.remainingTtl(1, at(1001)), 0)
  }

  test("unsigned expiration does not overflow across the signed integer boundary") {
    val expiration = Int.MaxValue.toLong + 2L
    assertEquals(AerospikeRDD.remainingTtl(expiration.toInt, at(expiration - 3L)), 3)
    assertEquals(AerospikeRDD.remainingTtl(expiration.toInt, at(expiration)), 0)
    assertEquals(AerospikeRDD.remainingTtl(expiration.toInt, at(expiration + 1L)), 0)
    assertEquals(AerospikeRDD.remainingTtl(-1, at(0xffffffffL - 1L)), 1)
    assertEquals(AerospikeRDD.remainingTtl(-1, at(0xffffffffL)), 0)
  }

  test("remaining TTL beyond the INT metadata range saturates instead of wrapping") {
    assertEquals(AerospikeRDD.remainingTtl(-1, at(1000)), Int.MaxValue)
  }
}
