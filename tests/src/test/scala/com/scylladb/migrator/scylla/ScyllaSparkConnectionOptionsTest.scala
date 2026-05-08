package com.scylladb.migrator.scylla

import com.datastax.spark.connector.datasource.CassandraSourceUtil
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CloudConfig, Credentials, SSLOptions, TargetSettings }
import org.apache.spark.SparkConf

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

  private def scoped(
    key: String,
    cluster: String = ScyllaSparkConnectionOptions.MySQLValidatorCluster
  ) =
    s"$cluster/$key"

  test("base case: no credentials, no SSL, no localDC, no connections") {
    val readerOpts = ScyllaSparkConnectionOptions.readerOptionsFromTargetSettings(baseTarget)
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(baseTarget)
    assertEquals(readerOpts("keyspace"), "ks")
    assertEquals(readerOpts("table"), "tbl")
    assertEquals(readerOpts("cluster"), ScyllaSparkConnectionOptions.MySQLValidatorCluster)
    assertEquals(sessionConf(scoped("spark.cassandra.connection.host")), "scylla-host")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.port")), "9042")
    assert(!sessionConf.contains("spark.cassandra.connection.host"))
    assert(!readerOpts.contains("spark.cassandra.connection.host"))
    assert(!readerOpts.contains("spark.cassandra.auth.username"))
    assert(!sessionConf.contains(scoped("spark.cassandra.auth.password")))
    assert(!sessionConf.contains(scoped("spark.cassandra.connection.ssl.enabled")))
    assert(!sessionConf.contains(scoped("spark.cassandra.connection.localDC")))
    assert(!sessionConf.contains(scoped("spark.cassandra.connection.localConnectionsPerExecutor")))
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
    val readerOpts = ScyllaSparkConnectionOptions.readerOptionsFromTargetSettings(target)
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(target)
    assertEquals(readerOpts("cluster"), ScyllaSparkConnectionOptions.MySQLValidatorCluster)
    assertEquals(sessionConf(scoped("spark.cassandra.auth.username")), "user")
    assertEquals(sessionConf(scoped("spark.cassandra.auth.password")), "pass")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.localDC")), "dc1")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.enabled")), "true")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.clientAuth.enabled")), "true")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.path")), "/path/ts")
    assertEquals(
      sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.password")),
      "tsPass"
    )
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.type")), "PKCS12")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.keyStore.path")), "/path/ks")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.keyStore.password")), "ksPass")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.keyStore.type")), "PKCS12")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.protocol")), "TLSv1.2")
    assertEquals(
      sessionConf(scoped("spark.cassandra.connection.ssl.enabledAlgorithms")),
      "TLS_RSA_WITH_AES_128_CBC_SHA"
    )
    assertEquals(
      sessionConf(scoped("spark.cassandra.connection.localConnectionsPerExecutor")),
      "16"
    )
    assertEquals(
      sessionConf(scoped("spark.cassandra.connection.remoteConnectionsPerExecutor")),
      "16"
    )
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
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(target)
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.enabled")), "true")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.path")), "/path/ts")
    assertEquals(
      sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.password")),
      "tsPass"
    )
    // Defaults are applied for type, protocol, algorithms
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.trustStore.type")), "JKS")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.keyStore.type")), "JKS")
    assertEquals(sessionConf(scoped("spark.cassandra.connection.ssl.protocol")), "TLS")
    // Optional fields that are None should be absent
    assert(!sessionConf.contains(scoped("spark.cassandra.connection.ssl.keyStore.path")))
    assert(!sessionConf.contains(scoped("spark.cassandra.connection.ssl.keyStore.password")))
    // No null values in the map
    assert(sessionConf.values.forall(_ != null), "Map must not contain null values")
  }

  test("credentials present adds auth keys") {
    val target = baseTarget.copy(credentials = Some(Credentials("admin", "secret")))
    val readerOpts = ScyllaSparkConnectionOptions.readerOptionsFromTargetSettings(target)
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(target)
    assert(!readerOpts.contains("spark.cassandra.auth.username"))
    assert(!readerOpts.contains("spark.cassandra.auth.password"))
    assertEquals(sessionConf(scoped("spark.cassandra.auth.username")), "admin")
    assertEquals(sessionConf(scoped("spark.cassandra.auth.password")), "secret")
  }

  test("credentials absent omits auth keys") {
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(baseTarget)
    assert(!sessionConf.contains(scoped("spark.cassandra.auth.username")))
    assert(!sessionConf.contains(scoped("spark.cassandra.auth.password")))
  }

  test("connector consolidates alias-scoped session config for non-default cluster") {
    val readerOpts = ScyllaSparkConnectionOptions.readerOptionsFromTargetSettings(baseTarget)
    val sessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(baseTarget)
    val cluster = readerOpts("cluster")
    val consolidated = CassandraSourceUtil.consolidateConfs(
      sparkConf   = new SparkConf(false),
      sqlConf     = sessionConf,
      cluster     = cluster,
      keyspace    = readerOpts("keyspace"),
      userOptions = readerOpts
    )

    assertEquals(consolidated.get("spark.cassandra.connection.host"), "scylla-host")
    assertEquals(consolidated.get("spark.cassandra.connection.port"), "9042")
    assertEquals(consolidated.get("spark.cassandra.query.retry.count"), "-1")
  }

  test("cloud target is rejected with IllegalArgumentException") {
    val cloudTarget = baseTarget.copy(
      host  = "",
      port  = 0,
      cloud = Some(CloudConfig("/opt/bundle.zip"))
    )
    intercept[IllegalArgumentException] {
      ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(cloudTarget)
    }
  }

  test("helper reuses the production target connector session defaults") {
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

    val sharedSessionDefaults =
      Connectors.sparkSessionOptions(Connectors.targetSessionOptions(target))
    val helperSessionConf = ScyllaSparkConnectionOptions.sessionConfFromTargetSettings(target)

    assertEquals(
      helperSessionConf - scoped("spark.cassandra.input.consistency.level"),
      sharedSessionDefaults.map { case (key, value) => scoped(key) -> value }
    )
  }
}
