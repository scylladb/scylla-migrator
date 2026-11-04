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
  ClientRoutesSettings,
  CloudConfig,
  ContactPoints,
  Credentials,
  SSLOptions,
  SourceSettings,
  TargetSettings
}
import org.apache.spark.SparkConf

object Connectors {
  private[migrator] case class SessionOptions(
    host: String,
    port: Int,
    credentials: Option[Credentials],
    sslOptions: Option[SSLOptions],
    localDC: Option[String],
    connections: Option[Int],
    queryRetryCount: Int = -1,
    clientRoutes: Option[ClientRoutesSettings] = None
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
  private val ClientRoutesEndpoints = "spark.cassandra.connection.clientRoutes.endpoints"
  private val ClientRoutesShardAwareness = "spark.cassandra.connection.clientRoutes.shardAwareness"

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
      queryRetryCount = -1,
      clientRoutes    = sourceSettings.clientRoutes
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
      queryRetryCount = -1,
      clientRoutes    = targetSettings.clientRoutes
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
    val withConnections = options.connections.fold(withSsl) { connections =>
      withSsl ++ Map(
        LocalConnectionsPerExecutor  -> connections.toString,
        RemoteConnectionsPerExecutor -> connections.toString
      )
    }
    options.clientRoutes.fold(withConnections) { routes =>
      withConnections ++ Map(
        ClientRoutesEndpoints      -> clientRoutesEndpointsValue(routes),
        ClientRoutesShardAwareness -> routes.shardAwareness.toString
      )
    }
  }

  /** The connector's `connectionId[=connectionAddr],...` syntax, which cannot express connection
    * IDs that contain `,` or `=`.
    */
  private def clientRoutesEndpointsValue(routes: ClientRoutesSettings): String =
    routes.endpoints
      .map { endpoint =>
        require(
          !endpoint.connectionId.exists(c => c == ',' || c == '='),
          s"Connection ID '${endpoint.connectionId}' cannot be passed as a Spark option: it " +
            "contains ',' or '='."
        )
        endpoint.connectionAddr.fold(endpoint.connectionId)(addr =>
          s"${endpoint.connectionId}=$addr"
        )
      }
      .mkString(",")

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
    val auth = authConf(sourceSettings.credentials)
    val contact = sourceSettings.cloud match {
      case Some(cloud) =>
        cloudContactInfo(cloud, auth)
      case None =>
        ipContactInfo(
          sourceSettings.host,
          sourceSettings.port,
          auth,
          sourceSettings.sslOptions
        )
    }
    withClientRoutes(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = contact,
        localDC     = if (sourceSettings.cloud.isDefined) None else sourceSettings.localDC,
        localConnectionsPerExecutor  = sourceSettings.connections,
        remoteConnectionsPerExecutor = sourceSettings.connections,
        queryRetryCount              = -1
      ),
      sourceSettings.clientRoutes
    )
  }

  /** Mirror of [[buildSourceConf]] for Scylla targets. */
  private[migrator] def buildTargetConf(
    sparkConf: SparkConf,
    targetSettings: TargetSettings.Scylla
  ): CassandraConnectorConf = {
    val auth = authConf(targetSettings.credentials)
    val contact = targetSettings.cloud match {
      case Some(cloud) =>
        cloudContactInfo(cloud, auth)
      case None =>
        ipContactInfo(
          targetSettings.host,
          targetSettings.port,
          auth,
          targetSettings.sslOptions
        )
    }
    withClientRoutes(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = contact,
        localDC     = if (targetSettings.cloud.isDefined) None else targetSettings.localDC,
        localConnectionsPerExecutor  = targetSettings.connections,
        remoteConnectionsPerExecutor = targetSettings.connections,
        queryRetryCount              = -1
      ),
      targetSettings.clientRoutes
    )
  }

  def sourceConnector(
    sparkConf: SparkConf,
    sourceSettings: SourceSettings.Cassandra
  ): CassandraConnector =
    new CassandraConnector(buildSourceConf(sparkConf, sourceSettings))

  def targetConnector(
    sparkConf: SparkConf,
    targetSettings: TargetSettings.Scylla
  ): CassandraConnector =
    new CassandraConnector(buildTargetConf(sparkConf, targetSettings))

  /** The connector verifies the routes when the Spark driver opens its first session, which the
    * readers, writers and validators all do before scheduling tasks.
    */
  private def withClientRoutes(
    conf: CassandraConnectorConf,
    clientRoutes: Option[ClientRoutesSettings]
  ): CassandraConnectorConf =
    clientRoutes.fold(conf)(routes => conf.copy(clientRoutes = Some(routes.toConnectorConf)))

  /** Map optional credentials to the connector's auth model. Missing credentials become
    * [[NoAuthConf]] rather than a parse-time error — Astra will reject unauthenticated sessions at
    * connect time, and non-Astra cloud bundles may use mTLS-only auth embedded in the bundle.
    */
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

  /** Build a [[CloudBasedContactInfo]]. The driver opens the bundle on every node that creates a
    * session, so the path must be reachable from every executor — see [[CloudConfig]] for
    * deployment guidance.
    *
    * Absolute local paths (`/opt/...`) are normalized to `file:///opt/...` URLs because the
    * connector's `DefaultConnectionFactory.maybeGetLocalFile` falls back to `new URL(path)`, which
    * requires a scheme — bare absolute paths would throw `MalformedURLException`.
    */
  private[migrator] def cloudContactInfo(
    cloud: CloudConfig,
    auth: com.datastax.spark.connector.cql.AuthConf
  ): ContactInfo = {
    val resolvedPath =
      if (cloud.secureBundlePath.startsWith("/"))
        new java.io.File(cloud.secureBundlePath).toURI.toString
      else cloud.secureBundlePath
    CloudBasedContactInfo(resolvedPath, auth)
  }

  private def ipContactInfo(
    host: String,
    port: Int,
    auth: com.datastax.spark.connector.cql.AuthConf,
    sslOptions: Option[SSLOptions]
  ): ContactInfo = {
    val contactPoints = ContactPoints
      .parse("Contact point", host, port)
      .fold(errors => throw new IllegalArgumentException(errors.mkString(" ")), identity)
    IpBasedContactInfo(
      hosts    = contactPoints.map(point => new InetSocketAddress(point.host, point.port)).toSet,
      authConf = auth,
      cassandraSSLConf = cassandraSSLConf(sslOptions)
    )
  }
}
