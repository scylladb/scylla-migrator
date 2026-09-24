package com.scylladb.migrator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.spark.connector.cql.{
  CassandraConnectionFactory,
  CassandraConnectorConf,
  DefaultConnectionFactory,
  IpBasedContactInfo,
  MultiplexingSchemaListener
}
import com.scylladb.migrator.config.ClientRoutesSettings
import org.apache.spark.SparkEnv

/** Opens connector sessions with the driver's client routes enabled, so that every node is reached
  * through its private endpoint. Pooling, timeouts, SSL, retry and load balancing settings come
  * from [[DefaultConnectionFactory.connectorConfigBuilder]], as for regular connections.
  *
  * The factory is serialized with its `CassandraConnectorConf` and used again on executors. Being a
  * case class, it takes part in the conf's equality, so connectors that differ only by their routes
  * never share a cached session.
  */
final case class ClientRoutesConnectionFactory(routes: ClientRoutesSettings)
    extends CassandraConnectionFactory {

  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val ipConf = conf.contactInfo match {
      case ip: IpBasedContactInfo => ip
      case other =>
        throw new IllegalArgumentException(
          s"Client routes require a host/port contact point, got ${other.getClass.getSimpleName}."
        )
    }
    val configLoader = DefaultConnectionFactory
      .connectorConfigBuilder(conf, DriverConfigLoader.programmaticBuilder())
      .build()
    val appId =
      Option(SparkEnv.get).flatMap(_.conf.getOption("spark.app.id")).getOrElse("NoAppID")
    val builder = CqlSession.builder()
    ipConf.authConf.authProvider
      .fold(builder)(builder.withAuthProvider)
      .withConfigLoader(configLoader)
      .withClientRoutesConfig(routes.toDriverConfig)
      .withApplicationName(s"Spark-Cassandra-Connector-$appId")
      .withSchemaChangeListener(new MultiplexingSchemaListener())
      .build()
  }
}
