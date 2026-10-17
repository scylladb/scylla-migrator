package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.policy.ClientPolicy
import com.scylladb.migrator.config.SourceSettings
import io.circe.Json

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

  private val sourceJson = Json.obj(
    "type"      -> Json.fromString("aerospike"),
    "keyMode"   -> Json.fromString("string"),
    "hosts"     -> Json.arr(Json.fromString("unreachable.invalid")),
    "namespace" -> Json.fromString("ns"),
    "set"       -> Json.fromString("set"),
    "schema"    -> Json.obj()
  )

  private def decodeSource(json: Json): SourceSettings.Aerospike =
    json.as[SourceSettings].toOption.get.asInstanceOf[SourceSettings.Aerospike]

  test("decoded absent retry budgets retain the runtime defaults and scan exhaustion boundary") {
    val config = Aerospike.buildReadConfig(decodeSource(sourceJson), splitCount = 17)
    assertEquals(config.splitCount, 17)
    assertEquals(config.maxScanRetries, 3)
    assertEquals(config.maxPollRetries, 5)
    val error = new AerospikeException(ResultCode.TIMEOUT)
    assert(AerospikeRDD.shouldRetryScan(error, 2, config.maxScanRetries, cancelled = false))
    assert(!AerospikeRDD.shouldRetryScan(error, 3, config.maxScanRetries, cancelled = false))
  }

  test("decoded explicit retry budgets reach runtime, including disabled scan retries") {
    val error = new AerospikeException(ResultCode.TIMEOUT)
    for (scanRetries <- Seq(0, 2)) {
      val source = decodeSource(
        sourceJson.mapObject(
          _.add("maxScanRetries", Json.fromInt(scanRetries))
            .add("maxPollRetries", Json.fromInt(7))
        )
      )
      val config = Aerospike.buildReadConfig(source, splitCount = 1)
      assertEquals(config.maxScanRetries, scanRetries)
      assertEquals(config.maxPollRetries, 7)
      assertEquals(
        AerospikeRDD.shouldRetryScan(error, 0, config.maxScanRetries, cancelled = false),
        scanRetries > 0
      )
      if (scanRetries > 0)
        assert(
          AerospikeRDD
            .shouldRetryScan(error, scanRetries - 1, config.maxScanRetries, cancelled = false)
        )
      assert(
        !AerospikeRDD.shouldRetryScan(error, scanRetries, config.maxScanRetries, cancelled = false)
      )
    }
  }

  test("invalid retry budgets fail source decoding before a scan can start") {
    for ((field, value) <- Seq("maxScanRetries" -> -1, "maxPollRetries" -> 0)) {
      val decoded = sourceJson.mapObject(_.add(field, Json.fromInt(value))).as[SourceSettings]
      val error = decoded.swap.toOption.get
      assert(error.getMessage.contains(field), error.getMessage)
    }
  }

  test("connection defaults retain SDK connection limits and resolve executor timeouts") {
    val source = decodeSource(sourceJson)
    assertEquals(source.maxConnsPerNode, None)
    assertEquals(source.connPoolsPerNode, None)
    val config = Aerospike.connectionConfig(source)
    assertEquals(config.socketTimeoutMs, Some(10000))
    assertEquals(config.totalTimeoutMs, Some(30000))
    val policy = Aerospike.buildClientPolicy(config, None)
    val defaults = new ClientPolicy()
    assertEquals(policy.maxConnsPerNode, defaults.maxConnsPerNode)
    assertEquals(policy.connPoolsPerNode, defaults.connPoolsPerNode)
    assertEquals(policy.scanPolicyDefault.socketTimeout, 10000)
    assertEquals(policy.scanPolicyDefault.totalTimeout, 30000)
  }

  test("configured connection limits and timeouts reach the executor client policy") {
    val source = decodeSource(
      sourceJson.mapObject(
        _.add("maxConnsPerNode", Json.fromInt(120))
          .add("connPoolsPerNode", Json.fromInt(4))
          .add("connectTimeoutMs", Json.fromInt(1234))
          .add("socketTimeoutMs", Json.fromInt(4000))
          .add("totalTimeoutMs", Json.fromInt(17000))
      )
    )
    assertEquals(source.maxConnsPerNode, Some(120))
    assertEquals(source.connPoolsPerNode, Some(4))
    val config = Aerospike.connectionConfig(source)
    val policy = Aerospike.buildClientPolicy(config, None)
    assertEquals(policy.maxConnsPerNode, 120)
    assertEquals(policy.connPoolsPerNode, 4)
    assertEquals(policy.timeout, 1234)
    assertEquals(policy.readPolicyDefault.socketTimeout, 4000)
    assertEquals(policy.readPolicyDefault.totalTimeout, 17000)
    assertEquals(policy.scanPolicyDefault.socketTimeout, 4000)
    assertEquals(policy.scanPolicyDefault.totalTimeout, 17000)
  }

  test("socket-derived total timeout is resolved and capped before executor client creation") {
    val source = decodeSource(sourceJson)
    val derived = Aerospike.connectionConfig(source.copy(socketTimeoutMs = Some(4000)))
    assertEquals(derived.totalTimeoutMs, Some(12000))
    val capped = Aerospike.connectionConfig(source.copy(socketTimeoutMs = Some(Int.MaxValue)))
    assertEquals(capped.totalTimeoutMs, Some(Int.MaxValue))
  }

  test("nonpositive connection limits are rejected while parsing") {
    for {
      field <- Seq("maxConnsPerNode", "connPoolsPerNode")
      value <- Seq(0, -1)
    }
      assert(sourceJson.mapObject(_.add(field, Json.fromInt(value))).as[SourceSettings].isLeft)
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
      keyMode            = com.scylladb.migrator.config.AerospikeKeyMode.StringKey,
      hosts              = Seq("h"),
      port               = None,
      namespace          = "ns",
      set                = "s",
      splitCount         = None,
      queueSize          = None,
      credentials        = Some(com.scylladb.migrator.config.Credentials("u", "p")),
      connectTimeoutMs   = None,
      socketTimeoutMs    = None,
      tlsName            = None,
      schema             = scala.collection.immutable.ListMap.empty,
      pollTimeoutSeconds = None,
      preserveTTL        = None,
      preserveGeneration = None,
      totalTimeoutMs     = None,
      maxScanRetries     = None,
      maxPollRetries     = None,
      maxConnsPerNode    = None,
      connPoolsPerNode   = None
    )
    assertEquals(Aerospike.resolveCredentials(source), Some(("u", "p")))
  }
}
