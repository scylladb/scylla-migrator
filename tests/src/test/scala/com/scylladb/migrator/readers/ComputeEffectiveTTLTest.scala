package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ TTLConfig, TTLPolicy }

class ComputeEffectiveTTLTest extends munit.FunSuite {

  val nowMillis: Long = 1700000000000L

  // --- set-if-missing policy (default) ---

  test("set-if-missing: existing TTL is preserved") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = Some(nowMillis * 1000),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 500L)
  }

  test("set-if-missing: no TTL computes relative TTL") {
    val writetimeMicros = (nowMillis - 100 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 3500L)
  }

  test("set-if-missing: expired record gets TTL of 1 second") {
    val writetimeMicros = (nowMillis - 7200 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 1L)
  }

  test("set-if-missing: no TTL and no writetime returns 0") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = None,
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }

  test("set-if-missing: existing TTL of 0 is preserved") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(0),
      writetime = Some(nowMillis * 1000),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }

  test("set-if-missing: record written just now gets full TTL") {
    val writetimeMicros = nowMillis * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(86400, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 86400L)
  }

  test("set-if-missing: record at expiry boundary gets TTL of 1") {
    val writetimeMicros = (nowMillis - 3600 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.SetIfMissing)),
      nowMillis = nowMillis
    )
    assertEquals(result, 1L)
  }

  // --- always policy ---

  test("always: overrides existing TTL") {
    val writetimeMicros = (nowMillis - 100 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.Always)),
      nowMillis = nowMillis
    )
    assertEquals(result, 3500L)
  }

  test("always: applies to cells without TTL") {
    val writetimeMicros = (nowMillis - 100 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.Always)),
      nowMillis = nowMillis
    )
    assertEquals(result, 3500L)
  }

  test("always: no writetime returns 0") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = None,
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.Always)),
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }

  // --- update-if-present policy ---

  test("update-if-present: recomputes when source has TTL") {
    val writetimeMicros = (nowMillis - 100 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.UpdateIfPresent)),
      nowMillis = nowMillis
    )
    assertEquals(result, 3500L)
  }

  test("update-if-present: skips cells without TTL") {
    val writetimeMicros = (nowMillis - 100 * 1000) * 1000
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(writetimeMicros),
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.UpdateIfPresent)),
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }

  test("update-if-present: no writetime returns 0 even with source TTL") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = None,
      ttlConfig = Some(TTLConfig(3600, TTLPolicy.UpdateIfPresent)),
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }

  // --- no config ---

  test("no config: preserves existing TTL") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = Some(500),
      writetime = Some(nowMillis * 1000),
      ttlConfig = None,
      nowMillis = nowMillis
    )
    assertEquals(result, 500L)
  }

  test("no config: no TTL returns 0") {
    val result = Cassandra.computeEffectiveTTL(
      ttl       = None,
      writetime = Some(nowMillis * 1000),
      ttlConfig = None,
      nowMillis = nowMillis
    )
    assertEquals(result, 0L)
  }
}
