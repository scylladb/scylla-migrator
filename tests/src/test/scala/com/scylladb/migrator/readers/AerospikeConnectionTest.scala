package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeException, ResultCode }

class AerospikeConnectionTest extends munit.FunSuite {

  // --- AerospikeConnectionKey ---

  test("AerospikeConnectionKey equality") {
    val key1 = AerospikeConnectionKey(List("host1"), 3000, None, None, None, None, None, None, None)
    val key2 = AerospikeConnectionKey(List("host1"), 3000, None, None, None, None, None, None, None)
    val key3 = AerospikeConnectionKey(List("host2"), 3000, None, None, None, None, None, None, None)
    assertEquals(key1, key2)
    assertNotEquals(key1, key3)
  }

  test("AerospikeConnectionKey with hashed credentials") {
    val hash1 = AerospikeConnectionKey.hashCredentials(Some(("u", "p")))
    val hash2 = AerospikeConnectionKey.hashCredentials(Some(("u", "p")))
    val hash3 = AerospikeConnectionKey.hashCredentials(Some(("u", "other")))
    val key1 = AerospikeConnectionKey(List("h"), 3000, hash1, None, None, None, None, None, None)
    val key2 = AerospikeConnectionKey(List("h"), 3000, hash2, None, None, None, None, None, None)
    val key3 = AerospikeConnectionKey(List("h"), 3000, hash3, None, None, None, None, None, None)
    assertEquals(key1, key2)
    assertNotEquals(key1, key3)
  }

  test("AerospikeConnectionKey.hashCredentials returns None for None") {
    assertEquals(AerospikeConnectionKey.hashCredentials(None), None)
  }

  test("AerospikeConnectionKey.hashCredentials is deterministic") {
    val h1 = AerospikeConnectionKey.hashCredentials(Some(("user", "pass")))
    val h2 = AerospikeConnectionKey.hashCredentials(Some(("user", "pass")))
    assertEquals(h1, h2)
    assert(h1.get.matches("[0-9a-f]{64}"), "Expected SHA-256 hex digest")
  }

  // --- isRetryable ---

  test("isRetryable: TIMEOUT is retryable") {
    assert(Aerospike.isRetryable(new AerospikeException(ResultCode.TIMEOUT)))
  }

  test("isRetryable: SERVER_NOT_AVAILABLE is retryable") {
    assert(Aerospike.isRetryable(new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE)))
  }

  test("isRetryable: KEY_NOT_FOUND is not retryable") {
    assert(!Aerospike.isRetryable(new AerospikeException(ResultCode.KEY_NOT_FOUND_ERROR)))
  }

  test("isRetryable: DEVICE_OVERLOAD is retryable") {
    assert(Aerospike.isRetryable(new AerospikeException(ResultCode.DEVICE_OVERLOAD)))
  }

  test("isRetryable: PARAMETER_ERROR is not retryable") {
    assert(!Aerospike.isRetryable(new AerospikeException(ResultCode.PARAMETER_ERROR)))
  }

  // --- buildClient (TLS and credential configuration) ---

  test("buildClientPolicy: sets tlsPolicy when tlsName is provided") {
    val connConfig = AerospikeConnectionConfig(
      hosts            = List("localhost"),
      port             = 3000,
      connectTimeoutMs = None,
      socketTimeoutMs  = None,
      totalTimeoutMs   = None,
      tlsName          = Some("my-tls-name"),
      maxConnsPerNode  = None,
      connPoolsPerNode = None
    )
    val policy = Aerospike.buildClientPolicy(connConfig, Some(("testuser", "testpass")))
    assert(policy.tlsPolicy != null, "Expected tlsPolicy to be set")
    assertEquals(policy.user, "testuser")
    assertEquals(policy.password, "testpass")
  }

  test("buildClientPolicy: sets user and password when credentials are provided") {
    val connConfig = AerospikeConnectionConfig(
      hosts            = List("localhost"),
      port             = 13579,
      connectTimeoutMs = Some(100),
      socketTimeoutMs  = Some(5000),
      totalTimeoutMs   = Some(15000),
      tlsName          = None,
      maxConnsPerNode  = None,
      connPoolsPerNode = None
    )
    val policy = Aerospike.buildClientPolicy(connConfig, Some(("admin", "secret")))
    assertEquals(policy.user, "admin")
    assertEquals(policy.password, "secret")
    assertEquals(policy.timeout, 100)
    assertEquals(policy.readPolicyDefault.socketTimeout, 5000)
    assertEquals(policy.readPolicyDefault.totalTimeout, 15000)
    assertEquals(policy.scanPolicyDefault.socketTimeout, 5000)
    assertEquals(policy.scanPolicyDefault.totalTimeout, 15000)
  }

  test("buildClientPolicy: works without TLS or credentials") {
    val connConfig = AerospikeConnectionConfig(
      hosts            = List("localhost"),
      port             = 13579,
      connectTimeoutMs = Some(100),
      socketTimeoutMs  = None,
      totalTimeoutMs   = None,
      tlsName          = None,
      maxConnsPerNode  = None,
      connPoolsPerNode = None
    )
    val policy = Aerospike.buildClientPolicy(connConfig, None)
    assert(policy.tlsPolicy == null, "Expected no tlsPolicy")
    assert(policy.user == null, "Expected no user")
    assert(policy.password == null, "Expected no password")
  }

  // --- resolveCredentials ---

  test("resolveCredentialsFromEnvOr: returns fallback when env vars are not set") {
    assume(
      System.getenv("AEROSPIKE_USERNAME") == null,
      "AEROSPIKE_USERNAME is set; skipping"
    )
    val result = Aerospike.resolveCredentialsFromEnvOr(Some(("fallback-user", "fallback-pass")))
    assertEquals(result, Some(("fallback-user", "fallback-pass")))
  }

  test("resolveCredentialsFromEnvOr: returns None when no env vars and no fallback") {
    assume(
      System.getenv("AEROSPIKE_USERNAME") == null,
      "AEROSPIKE_USERNAME is set; skipping"
    )
    val result = Aerospike.resolveCredentialsFromEnvOr(None)
    assertEquals(result, None)
  }

  test("resolveCredentials: returns config credentials when env vars are not set") {
    // This test relies on AEROSPIKE_USERNAME not being set in the test environment.
    // If it is set, the test will be skipped.
    assume(
      System.getenv("AEROSPIKE_USERNAME") == null,
      "AEROSPIKE_USERNAME is set; skipping"
    )
    val source = com.scylladb.migrator.config.SourceSettings.Aerospike(
      hosts                   = Seq("h"),
      port                    = None,
      namespace               = "ns",
      set                     = "s",
      bins                    = None,
      splitCount              = None,
      schemaSampleSize        = None,
      queueSize               = None,
      credentials             = Some(com.scylladb.migrator.config.Credentials("u", "p")),
      connectTimeoutMs        = None,
      socketTimeoutMs         = None,
      tlsName                 = None,
      schema                  = None,
      pollTimeoutSeconds      = None,
      preserveTTL             = None,
      preserveGeneration      = None,
      totalTimeoutMs          = None,
      maxScanRetries          = None,
      schemaDiscoveryStrategy = None,
      maxPollRetries          = None,
      maxConnsPerNode         = None,
      connPoolsPerNode        = None
    )
    assertEquals(Aerospike.resolveCredentials(source), Some(("u", "p")))
  }
}
