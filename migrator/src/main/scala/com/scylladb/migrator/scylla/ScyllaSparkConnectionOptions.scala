package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.{ SSLOptions, TargetSettings }

/** Shared construction of Spark Cassandra Connector config options from [[TargetSettings.Scylla]].
  * This ensures consistent defaults (SSL store types, protocol, enabled cipher suites, etc.)
  * regardless of whether the options are used by the migrator, the Cassandra-to-Scylla validator,
  * or the MySQL-to-Scylla validator.
  *
  * The defaults here mirror those in [[com.scylladb.migrator.Connectors]] which builds
  * `CassandraConnectorConf` objects with the same values.
  */
object ScyllaSparkConnectionOptions {

  // Spark Cassandra Connector config key constants
  private val ConnectionHost = "spark.cassandra.connection.host"
  private val ConnectionPort = "spark.cassandra.connection.port"
  private val ConnectionLocalDC = "spark.cassandra.connection.localDC"
  private val AuthUsername = "spark.cassandra.auth.username"
  private val AuthPassword = "spark.cassandra.auth.password"
  private val SslEnabled = "spark.cassandra.connection.ssl.enabled"
  private val SslClientAuthEnabled = "spark.cassandra.connection.ssl.clientAuth.enabled"
  private val SslTrustStorePath = "spark.cassandra.connection.ssl.trustStore.path"
  private val SslTrustStorePassword = "spark.cassandra.connection.ssl.trustStore.password"
  private val SslTrustStoreType = "spark.cassandra.connection.ssl.trustStore.type"
  private val SslKeyStorePath = "spark.cassandra.connection.ssl.keyStore.path"
  private val SslKeyStorePassword = "spark.cassandra.connection.ssl.keyStore.password"
  private val SslKeyStoreType = "spark.cassandra.connection.ssl.keyStore.type"
  private val SslProtocol = "spark.cassandra.connection.ssl.protocol"
  private val SslEnabledAlgorithms = "spark.cassandra.connection.ssl.enabledAlgorithms"
  private val LocalConnectionsPerExecutor = "spark.cassandra.connection.localConnectionsPerExecutor"
  private val RemoteConnectionsPerExecutor =
    "spark.cassandra.connection.remoteConnectionsPerExecutor"
  private val QueryRetryCount = "spark.cassandra.query.retry.count"
  private val InputConsistencyLevel = "spark.cassandra.input.consistency.level"

  /** Build a `Map[String, String]` suitable for passing to
    * `spark.read.format("org.apache.spark.sql.cassandra").options(...)`.
    *
    * Note: `stripTrailingZerosForDecimals` is intentionally not propagated here because it is not a
    * Spark Cassandra Connector option. It is handled by the migrator writer. For validation,
    * decimal comparison differences are accounted for by the `floatingPointTolerance` setting.
    */
  def fromTargetSettings(t: TargetSettings.Scylla): Map[String, String] = {
    val base = Map(
      "keyspace"      -> t.keyspace,
      "table"         -> t.table,
      ConnectionHost  -> t.host,
      ConnectionPort  -> t.port.toString,
      QueryRetryCount -> "-1"
    )
    val withCreds = t.credentials.fold(base) { creds =>
      base ++ Map(
        AuthUsername -> creds.username,
        AuthPassword -> creds.password
      )
    }
    val withDC = t.localDC.fold(withCreds) { dc =>
      withCreds + (ConnectionLocalDC -> dc)
    }
    val withSSL = t.sslOptions.fold(withDC) { ssl =>
      val requiredSslEntries = Map(
        SslEnabled           -> ssl.enabled.toString,
        SslClientAuthEnabled -> ssl.clientAuthEnabled.toString,
        SslTrustStoreType    -> ssl.trustStoreType.getOrElse(SSLOptions.DefaultTrustStoreType),
        SslKeyStoreType      -> ssl.keyStoreType.getOrElse(SSLOptions.DefaultKeyStoreType),
        SslProtocol          -> ssl.protocol.getOrElse(SSLOptions.DefaultProtocol),
        SslEnabledAlgorithms -> ssl.enabledAlgorithms
          .getOrElse(SSLOptions.DefaultEnabledAlgorithms)
          .mkString(",")
      )
      val optionalSslEntries: Map[String, String] = List(
        ssl.trustStorePath.map(SslTrustStorePath -> _),
        ssl.trustStorePassword.map(SslTrustStorePassword -> _),
        ssl.keyStorePath.map(SslKeyStorePath -> _),
        ssl.keyStorePassword.map(SslKeyStorePassword -> _)
      ).flatten.toMap
      withDC ++ requiredSslEntries ++ optionalSslEntries
    }
    val withCL = withSSL + (InputConsistencyLevel -> t.consistencyLevel)
    t.connections.fold(withCL) { c =>
      withCL ++ Map(
        LocalConnectionsPerExecutor  -> c.toString,
        RemoteConnectionsPerExecutor -> c.toString
      )
    }
  }
}
