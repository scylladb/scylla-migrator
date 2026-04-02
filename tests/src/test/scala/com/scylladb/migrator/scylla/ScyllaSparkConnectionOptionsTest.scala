package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.{ Credentials, SSLOptions, TargetSettings }

class ScyllaSparkConnectionOptionsTest extends munit.FunSuite {

  private val baseTarget = TargetSettings.Scylla(
    host                          = "scylla-host",
    port                          = 9042,
    localDC                       = None,
    credentials                   = None,
    sslOptions                    = None,
    keyspace                      = "ks",
    table                         = "tbl",
    connections                   = None,
    stripTrailingZerosForDecimals = false,
    writeTTLInS                   = None,
    writeWritetimestampInuS       = None,
    consistencyLevel              = "LOCAL_QUORUM"
  )

  test("base case: no credentials, no SSL, no localDC, no connections") {
    val opts = ScyllaSparkConnectionOptions.fromTargetSettings(baseTarget)
    assertEquals(opts("keyspace"), "ks")
    assertEquals(opts("table"), "tbl")
    assertEquals(opts("spark.cassandra.connection.host"), "scylla-host")
    assertEquals(opts("spark.cassandra.connection.port"), "9042")
    assert(!opts.contains("spark.cassandra.auth.username"))
    assert(!opts.contains("spark.cassandra.auth.password"))
    assert(!opts.contains("spark.cassandra.connection.ssl.enabled"))
    assert(!opts.contains("spark.cassandra.connection.localDC"))
    assert(!opts.contains("spark.cassandra.connection.localConnectionsPerExecutor"))
  }

  test("full case: all options provided") {
    val target = baseTarget.copy(
      localDC     = Some("dc1"),
      credentials = Some(Credentials("user", "pass")),
      sslOptions = Some(
        SSLOptions(
          clientAuthEnabled  = true,
          enabled            = true,
          enabledAlgorithms  = Some(Set("TLS_RSA_WITH_AES_128_CBC_SHA")),
          keyStorePassword   = Some("ksPass"),
          keyStorePath       = Some("/path/ks"),
          keyStoreType       = Some("PKCS12"),
          protocol           = Some("TLSv1.2"),
          trustStorePassword = Some("tsPass"),
          trustStorePath     = Some("/path/ts"),
          trustStoreType     = Some("PKCS12")
        )
      ),
      connections = Some(16)
    )
    val opts = ScyllaSparkConnectionOptions.fromTargetSettings(target)
    assertEquals(opts("spark.cassandra.auth.username"), "user")
    assertEquals(opts("spark.cassandra.auth.password"), "pass")
    assertEquals(opts("spark.cassandra.connection.localDC"), "dc1")
    assertEquals(opts("spark.cassandra.connection.ssl.enabled"), "true")
    assertEquals(opts("spark.cassandra.connection.ssl.clientAuth.enabled"), "true")
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.path"), "/path/ts")
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.password"), "tsPass")
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.type"), "PKCS12")
    assertEquals(opts("spark.cassandra.connection.ssl.keyStore.path"), "/path/ks")
    assertEquals(opts("spark.cassandra.connection.ssl.keyStore.password"), "ksPass")
    assertEquals(opts("spark.cassandra.connection.ssl.keyStore.type"), "PKCS12")
    assertEquals(opts("spark.cassandra.connection.ssl.protocol"), "TLSv1.2")
    assertEquals(
      opts("spark.cassandra.connection.ssl.enabledAlgorithms"),
      "TLS_RSA_WITH_AES_128_CBC_SHA"
    )
    assertEquals(opts("spark.cassandra.connection.localConnectionsPerExecutor"), "16")
    assertEquals(opts("spark.cassandra.connection.remoteConnectionsPerExecutor"), "16")
  }

  test("partial SSL: some SSL fields None are absent from the result map") {
    val target = baseTarget.copy(
      sslOptions = Some(
        SSLOptions(
          clientAuthEnabled  = false,
          enabled            = true,
          enabledAlgorithms  = None,
          keyStorePassword   = None,
          keyStorePath       = None,
          keyStoreType       = None,
          protocol           = None,
          trustStorePassword = Some("tsPass"),
          trustStorePath     = Some("/path/ts"),
          trustStoreType     = None
        )
      )
    )
    val opts = ScyllaSparkConnectionOptions.fromTargetSettings(target)
    assertEquals(opts("spark.cassandra.connection.ssl.enabled"), "true")
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.path"), "/path/ts")
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.password"), "tsPass")
    // Defaults are applied for type, protocol, algorithms
    assertEquals(opts("spark.cassandra.connection.ssl.trustStore.type"), "JKS")
    assertEquals(opts("spark.cassandra.connection.ssl.keyStore.type"), "JKS")
    assertEquals(opts("spark.cassandra.connection.ssl.protocol"), "TLS")
    // Optional fields that are None should be absent
    assert(!opts.contains("spark.cassandra.connection.ssl.keyStore.path"))
    assert(!opts.contains("spark.cassandra.connection.ssl.keyStore.password"))
    // No null values in the map
    assert(opts.values.forall(_ != null), "Map must not contain null values")
  }

  test("credentials present adds auth keys") {
    val target = baseTarget.copy(credentials = Some(Credentials("admin", "secret")))
    val opts = ScyllaSparkConnectionOptions.fromTargetSettings(target)
    assertEquals(opts("spark.cassandra.auth.username"), "admin")
    assertEquals(opts("spark.cassandra.auth.password"), "secret")
  }

  test("credentials absent omits auth keys") {
    val opts = ScyllaSparkConnectionOptions.fromTargetSettings(baseTarget)
    assert(!opts.contains("spark.cassandra.auth.username"))
    assert(!opts.contains("spark.cassandra.auth.password"))
  }
}
