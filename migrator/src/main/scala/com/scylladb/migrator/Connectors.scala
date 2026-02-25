package com.scylladb.migrator

import java.net.{ InetAddress, InetSocketAddress }

import com.datastax.spark.connector.cql.{
  CassandraConnector,
  CassandraConnectorConf,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{ Credentials, SourceSettings, TargetSettings }
import org.apache.spark.SparkConf

object Connectors {
  def sourceConnector(sparkConf: SparkConf, sourceSettings: SourceSettings.Cassandra) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = Set(new InetSocketAddress(sourceSettings.host, sourceSettings.port)),
          authConf = sourceSettings.credentials match {
            case None                                  => NoAuthConf
            case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
          },
          cassandraSSLConf = sourceSettings.sslOptions match {
            case None => CassandraConnectorConf.DefaultCassandraSSLConf
            case Some(sslOptions) =>
              CassandraConnectorConf.CassandraSSLConf(
                enabled = sslOptions.enabled,
                trustStorePath = sslOptions.trustStorePath,
                trustStorePassword = sslOptions.trustStorePassword,
                trustStoreType = sslOptions.trustStoreType.getOrElse("JKS"),
                protocol = sslOptions.protocol.getOrElse("TLS"),
                enabledAlgorithms = sslOptions.enabledAlgorithms.getOrElse(
                  Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
                ),
                clientAuthEnabled = sslOptions.clientAuthEnabled,
                keyStorePath = sslOptions.keyStorePath,
                keyStorePassword = sslOptions.keyStorePassword,
                keyStoreType = sslOptions.keyStoreType.getOrElse("JKS")
              )
          }
        ),
        localDC = sourceSettings.localDC,
        localConnectionsPerExecutor = sourceSettings.connections,
        remoteConnectionsPerExecutor = sourceSettings.connections,
        queryRetryCount = -1
      )
    )

  def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = Set(new InetSocketAddress(targetSettings.host, targetSettings.port)),
          authConf = targetSettings.credentials match {
            case None                                  => NoAuthConf
            case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
          },
          cassandraSSLConf = targetSettings.sslOptions match {
            case None => CassandraConnectorConf.DefaultCassandraSSLConf
            case Some(sslOptions) =>
              CassandraConnectorConf.CassandraSSLConf(
                enabled = sslOptions.enabled,
                clientAuthEnabled = sslOptions.clientAuthEnabled,
                trustStorePath = sslOptions.trustStorePath,
                trustStorePassword = sslOptions.trustStorePassword,
                trustStoreType = sslOptions.trustStoreType.getOrElse("JKS"),
                protocol = sslOptions.protocol.getOrElse("TLS"),
                keyStorePath = sslOptions.keyStorePath,
                keyStorePassword = sslOptions.keyStorePassword,
                enabledAlgorithms = sslOptions.enabledAlgorithms.getOrElse(
                  Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
                ),
                keyStoreType = sslOptions.keyStoreType.getOrElse("JKS")
              )
          }
        ),
        localDC = targetSettings.localDC,
        localConnectionsPerExecutor = targetSettings.connections,
        remoteConnectionsPerExecutor = targetSettings.connections,
        queryRetryCount = -1
      )
    )
}
