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
          }
        ),
        localConnectionsPerExecutor  = sourceSettings.connections,
        remoteConnectionsPerExecutor = sourceSettings.connections,
        queryRetryCount              = -1
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
          }
        ),
        localConnectionsPerExecutor  = targetSettings.connections,
        remoteConnectionsPerExecutor = targetSettings.connections,
        queryRetryCount              = -1
      )
    )
}
