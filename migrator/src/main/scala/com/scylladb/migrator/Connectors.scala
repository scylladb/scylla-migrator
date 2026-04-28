package com.scylladb.migrator

import java.net.InetSocketAddress

import com.datastax.spark.connector.cql.{
  CassandraConnector,
  CassandraConnectorConf,
  CloudBasedContactInfo,
  ContactInfo,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{
  CloudConfig,
  Credentials,
  SSLOptions,
  SourceSettings,
  TargetSettings
}
import org.apache.spark.SparkConf

object Connectors {

  /** Build a `CassandraConnectorConf` from a Cassandra source.
    *
    * Exposed as `private[migrator]` so unit tests can assert on the resulting `contactInfo` (cloud
    * bundle vs IP+SSL) without spinning up a real `CassandraConnector` (which would try to open a
    * session at construction time in some code paths).
    */
  private[migrator] def buildSourceConf(
    sparkConf: SparkConf,
    sourceSettings: SourceSettings.Cassandra
  ): CassandraConnectorConf = {
    val authConf = toAuthConf(sourceSettings.credentials)
    val contact = sourceSettings.cloud match {
      case Some(cloud) =>
        cloudContactInfo(cloud, authConf)
      case None =>
        ipContactInfo(
          sourceSettings.host,
          sourceSettings.port,
          authConf,
          sourceSettings.sslOptions
        )
    }
    CassandraConnectorConf(sparkConf).copy(
      contactInfo = contact,
      localDC     = if (sourceSettings.cloud.isDefined) None else sourceSettings.localDC,
      localConnectionsPerExecutor  = sourceSettings.connections,
      remoteConnectionsPerExecutor = sourceSettings.connections,
      queryRetryCount              = -1
    )
  }

  /** Mirror of [[buildSourceConf]] for Scylla targets. */
  private[migrator] def buildTargetConf(
    sparkConf: SparkConf,
    targetSettings: TargetSettings.Scylla
  ): CassandraConnectorConf = {
    val authConf = toAuthConf(targetSettings.credentials)
    val contact = targetSettings.cloud match {
      case Some(cloud) =>
        cloudContactInfo(cloud, authConf)
      case None =>
        ipContactInfo(
          targetSettings.host,
          targetSettings.port,
          authConf,
          targetSettings.sslOptions
        )
    }
    CassandraConnectorConf(sparkConf).copy(
      contactInfo = contact,
      localDC     = if (targetSettings.cloud.isDefined) None else targetSettings.localDC,
      localConnectionsPerExecutor  = targetSettings.connections,
      remoteConnectionsPerExecutor = targetSettings.connections,
      queryRetryCount              = -1
    )
  }

  def sourceConnector(sparkConf: SparkConf, sourceSettings: SourceSettings.Cassandra) =
    new CassandraConnector(buildSourceConf(sparkConf, sourceSettings))

  def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) =
    new CassandraConnector(buildTargetConf(sparkConf, targetSettings))

  private def toAuthConf(credentials: Option[Credentials]) = credentials match {
    case None                                  => NoAuthConf
    case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
  }

  /** Build a [[CloudBasedContactInfo]]. The driver opens the bundle on every node that creates a
    * session, so the path must be reachable from every executor — see [[CloudConfig]] for
    * deployment guidance.
    */
  private def cloudContactInfo(
    cloud: CloudConfig,
    authConf: com.datastax.spark.connector.cql.AuthConf
  ): ContactInfo =
    CloudBasedContactInfo(cloud.secureBundlePath, authConf)

  private def ipContactInfo(
    host: String,
    port: Int,
    authConf: com.datastax.spark.connector.cql.AuthConf,
    sslOptions: Option[SSLOptions]
  ): ContactInfo =
    IpBasedContactInfo(
      hosts    = Set(new InetSocketAddress(host, port)),
      authConf = authConf,
      cassandraSSLConf = sslOptions match {
        case None => CassandraConnectorConf.DefaultCassandraSSLConf
        case Some(s) =>
          CassandraConnectorConf.CassandraSSLConf(
            enabled            = s.enabled,
            trustStorePath     = s.trustStorePath,
            trustStorePassword = s.trustStorePassword,
            trustStoreType     = s.trustStoreType.getOrElse("JKS"),
            protocol           = s.protocol.getOrElse("TLS"),
            enabledAlgorithms = s.enabledAlgorithms.getOrElse(
              Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
            ),
            clientAuthEnabled = s.clientAuthEnabled,
            keyStorePath      = s.keyStorePath,
            keyStorePassword  = s.keyStorePassword,
            keyStoreType      = s.keyStoreType.getOrElse("JKS")
          )
      }
    )
}
