package com.scylladb.migrator

import java.net.{ InetAddress, InetSocketAddress }

import com.datastax.spark.connector.cql.{
  CassandraConnector,
  CassandraConnectorConf,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{ Credentials, SSLOptions, SourceSettings, TargetSettings }
import org.apache.spark.SparkConf

object Connectors {
  private[migrator] case class SessionOptions(
    host: String,
    port: Int,
    credentials: Option[Credentials],
    sslOptions: Option[SSLOptions],
    localDC: Option[String],
    connections: Option[Int],
    queryRetryCount: Int = -1
  )

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

  private[migrator] def sourceSessionOptions(
    sourceSettings: SourceSettings.Cassandra
  ): SessionOptions =
    SessionOptions(
      host            = sourceSettings.host,
      port            = sourceSettings.port,
      credentials     = sourceSettings.credentials,
      sslOptions      = sourceSettings.sslOptions,
      localDC         = sourceSettings.localDC,
      connections     = sourceSettings.connections,
      queryRetryCount = -1
    )

  private[migrator] def targetSessionOptions(
    targetSettings: TargetSettings.Scylla
  ): SessionOptions =
    SessionOptions(
      host            = targetSettings.host,
      port            = targetSettings.port,
      credentials     = targetSettings.credentials,
      sslOptions      = targetSettings.sslOptions,
      localDC         = targetSettings.localDC,
      connections     = targetSettings.connections,
      queryRetryCount = -1
    )

  private[migrator] def sparkSessionOptions(options: SessionOptions): Map[String, String] = {
    val base = Map(
      ConnectionHost  -> options.host,
      ConnectionPort  -> options.port.toString,
      QueryRetryCount -> options.queryRetryCount.toString
    )
    val withCredentials = options.credentials.fold(base) { credentials =>
      base ++ Map(
        AuthUsername -> credentials.username,
        AuthPassword -> credentials.password
      )
    }
    val withLocalDc = options.localDC.fold(withCredentials) { dc =>
      withCredentials + (ConnectionLocalDC -> dc)
    }
    val withSsl = options.sslOptions.fold(withLocalDc) { sslOptions =>
      val requiredSslEntries = Map(
        SslEnabled           -> sslOptions.enabled.toString,
        SslClientAuthEnabled -> sslOptions.clientAuthEnabled.toString,
        SslTrustStoreType ->
          sslOptions.trustStoreType.getOrElse(SSLOptions.DefaultTrustStoreType),
        SslKeyStoreType -> sslOptions.keyStoreType.getOrElse(SSLOptions.DefaultKeyStoreType),
        SslProtocol     -> sslOptions.protocol.getOrElse(SSLOptions.DefaultProtocol),
        SslEnabledAlgorithms ->
          sslOptions.enabledAlgorithms.getOrElse(SSLOptions.DefaultEnabledAlgorithms).mkString(",")
      )
      val optionalSslEntries: Map[String, String] = List(
        sslOptions.trustStorePath.map(SslTrustStorePath -> _),
        sslOptions.trustStorePassword.map(SslTrustStorePassword -> _),
        sslOptions.keyStorePath.map(SslKeyStorePath -> _),
        sslOptions.keyStorePassword.map(SslKeyStorePassword -> _)
      ).flatten.toMap
      withLocalDc ++ requiredSslEntries ++ optionalSslEntries
    }
    options.connections.fold(withSsl) { connections =>
      withSsl ++ Map(
        LocalConnectionsPerExecutor  -> connections.toString,
        RemoteConnectionsPerExecutor -> connections.toString
      )
    }
  }

  private def authConf(credentials: Option[Credentials]) =
    credentials match {
      case None                                  => NoAuthConf
      case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
    }

  private def cassandraSSLConf(sslOptions: Option[SSLOptions]) =
    sslOptions match {
      case None => CassandraConnectorConf.DefaultCassandraSSLConf
      case Some(sslOptions) =>
        CassandraConnectorConf.CassandraSSLConf(
          enabled            = sslOptions.enabled,
          clientAuthEnabled  = sslOptions.clientAuthEnabled,
          trustStorePath     = sslOptions.trustStorePath,
          trustStorePassword = sslOptions.trustStorePassword,
          trustStoreType   = sslOptions.trustStoreType.getOrElse(SSLOptions.DefaultTrustStoreType),
          protocol         = sslOptions.protocol.getOrElse(SSLOptions.DefaultProtocol),
          keyStorePath     = sslOptions.keyStorePath,
          keyStorePassword = sslOptions.keyStorePassword,
          enabledAlgorithms =
            sslOptions.enabledAlgorithms.getOrElse(SSLOptions.DefaultEnabledAlgorithms),
          keyStoreType = sslOptions.keyStoreType.getOrElse(SSLOptions.DefaultKeyStoreType)
        )
    }

  private def connector(
    sparkConf: SparkConf,
    options: SessionOptions
  ) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts            = Set(new InetSocketAddress(options.host, options.port)),
          authConf         = authConf(options.credentials),
          cassandraSSLConf = cassandraSSLConf(options.sslOptions)
        ),
        localDC                      = options.localDC,
        localConnectionsPerExecutor  = options.connections,
        remoteConnectionsPerExecutor = options.connections,
        queryRetryCount              = options.queryRetryCount
      )
    )

  def sourceConnector(sparkConf: SparkConf, sourceSettings: SourceSettings.Cassandra) =
    connector(sparkConf, sourceSessionOptions(sourceSettings))

  def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) =
    connector(sparkConf, targetSessionOptions(targetSettings))
}
