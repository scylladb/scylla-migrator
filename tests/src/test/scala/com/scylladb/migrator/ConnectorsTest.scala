package com.scylladb.migrator

import com.datastax.spark.connector.cql.{
  CloudBasedContactInfo,
  DefaultConnectionFactory,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{
  ClientRouteEndpoint,
  ClientRoutesSettings,
  CloudConfig,
  Credentials,
  SourceSettings,
  TargetSettings
}
import org.apache.spark.SparkConf

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

class ConnectorsTest extends munit.FunSuite {

  // ---------------------------------------------------------------------------
  // Source: cassandra
  // ---------------------------------------------------------------------------

  test("source: cloud bundle produces CloudBasedContactInfo with creds, no localDC") {
    val source = baseCassandraSource.copy(
      host        = "",
      port        = 0,
      localDC     = None, // already None; just being explicit
      sslOptions  = None,
      credentials = Some(Credentials("client-id", "client-secret")),
      cloud       = Some(CloudConfig("/opt/bundle.zip"))
    )

    val conf = Connectors.buildSourceConf(new SparkConf(false), source)

    conf.contactInfo match {
      case CloudBasedContactInfo(path, PasswordAuthConf(u, p)) =>
        assertEquals(path, "file:/opt/bundle.zip")
        assertEquals(u, "client-id")
        assertEquals(p, "client-secret")
      case other =>
        fail(s"Expected CloudBasedContactInfo, got $other")
    }
    assertEquals(conf.localDC, None)
  }

  test("source: cloud bundle without credentials uses NoAuthConf") {
    val source = baseCassandraSource.copy(
      host        = "",
      port        = 0,
      credentials = None,
      cloud       = Some(CloudConfig("/opt/bundle.zip"))
    )

    val conf = Connectors.buildSourceConf(new SparkConf(false), source)

    conf.contactInfo match {
      case CloudBasedContactInfo(path, NoAuthConf) =>
        assertEquals(path, "file:/opt/bundle.zip")
      case other =>
        fail(s"Expected CloudBasedContactInfo with NoAuthConf, got $other")
    }
  }

  test("source: legacy host/port produces IpBasedContactInfo and preserves localDC") {
    val source = baseCassandraSource.copy(
      host        = "1.2.3.4",
      port        = 9042,
      localDC     = Some("dc1"),
      cloud       = None,
      credentials = Some(Credentials("u", "p"))
    )

    val conf = Connectors.buildSourceConf(new SparkConf(false), source)

    conf.contactInfo match {
      case ip: IpBasedContactInfo =>
        val one = ip.hosts.head
        assertEquals(one.getHostString, "1.2.3.4")
        assertEquals(one.getPort, 9042)
        ip.authConf match {
          case PasswordAuthConf(u, p) =>
            assertEquals(u, "u"); assertEquals(p, "p")
          case other => fail(s"Expected PasswordAuthConf, got $other")
        }
      case other => fail(s"Expected IpBasedContactInfo, got $other")
    }
    assertEquals(conf.localDC, Some("dc1"))
  }

  // ---------------------------------------------------------------------------
  // Target: scylla
  // ---------------------------------------------------------------------------

  test("target: cloud bundle produces CloudBasedContactInfo, drops localDC") {
    val target = baseScyllaTarget.copy(
      host        = "",
      port        = 0,
      sslOptions  = None,
      credentials = Some(Credentials("u", "p")),
      cloud       = Some(CloudConfig("/opt/bundle.zip"))
    )

    val conf = Connectors.buildTargetConf(new SparkConf(false), target)

    conf.contactInfo match {
      case CloudBasedContactInfo(path, PasswordAuthConf(u, p)) =>
        assertEquals(path, "file:/opt/bundle.zip")
        assertEquals(u, "u")
        assertEquals(p, "p")
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
    assertEquals(conf.localDC, None)
  }

  test("target: legacy host/port produces IpBasedContactInfo") {
    val target = baseScyllaTarget.copy(
      host    = "5.6.7.8",
      port    = 9042,
      localDC = Some("dc1"),
      cloud   = None
    )

    val conf = Connectors.buildTargetConf(new SparkConf(false), target)

    conf.contactInfo match {
      case ip: IpBasedContactInfo =>
        val one = ip.hosts.head
        assertEquals(one.getHostString, "5.6.7.8")
        assertEquals(one.getPort, 9042)
      case other => fail(s"Expected IpBasedContactInfo, got $other")
    }
    assertEquals(conf.localDC, Some("dc1"))
  }

  // ---------------------------------------------------------------------------
  // ipContactInfo: IPv6 bracket stripping
  // ---------------------------------------------------------------------------

  test("source: bracketed IPv6 host has brackets stripped for InetSocketAddress") {
    val source = baseCassandraSource.copy(
      host  = "[2001:db8::1]",
      port  = 9042,
      cloud = None
    )

    val conf = Connectors.buildSourceConf(new SparkConf(false), source)

    conf.contactInfo match {
      case ip: IpBasedContactInfo =>
        val one = ip.hosts.head
        assert(!one.getHostString.contains("["), s"brackets not stripped: ${one.getHostString}")
        assertEquals(one.getPort, 9042)
      case other => fail(s"Expected IpBasedContactInfo, got $other")
    }
  }

  // ---------------------------------------------------------------------------
  // Client routes
  // ---------------------------------------------------------------------------

  private val routes = ClientRoutesSettings(
    List(ClientRouteEndpoint("conn-dc1"), ClientRouteEndpoint("conn-dc2", Some("10.0.0.5")))
  )

  test("source: clientRoutes installs the client routes connection factory") {
    val source = baseCassandraSource.copy(
      host         = "pl-endpoint.example.com",
      localDC      = Some("dc1"),
      clientRoutes = Some(routes)
    )

    val conf = Connectors.buildSourceConf(new SparkConf(false), source)

    assertEquals(conf.connectionFactory, ClientRoutesConnectionFactory(routes))
    assertEquals(conf.localDC, Some("dc1"))
    conf.contactInfo match {
      case ip: IpBasedContactInfo =>
        assertEquals(ip.hosts.head.getHostString, "pl-endpoint.example.com")
        assertEquals(ip.hosts.head.getPort, 9042)
      case other => fail(s"Expected IpBasedContactInfo, got $other")
    }
  }

  test("target: clientRoutes installs the client routes connection factory") {
    val target = baseScyllaTarget.copy(localDC = Some("dc1"), clientRoutes = Some(routes))

    val conf = Connectors.buildTargetConf(new SparkConf(false), target)

    assertEquals(conf.connectionFactory, ClientRoutesConnectionFactory(routes))
    assertEquals(conf.localDC, Some("dc1"))
  }

  test("without clientRoutes the default connection factory is kept") {
    assertEquals(
      Connectors.buildSourceConf(new SparkConf(false), baseCassandraSource).connectionFactory,
      DefaultConnectionFactory
    )
    assertEquals(
      Connectors.buildTargetConf(new SparkConf(false), baseScyllaTarget).connectionFactory,
      DefaultConnectionFactory
    )
  }

  test("confs that differ only by client routes do not share a session cache key") {
    val target = baseScyllaTarget.copy(localDC = Some("dc1"))
    val plain = Connectors.buildTargetConf(new SparkConf(false), target)
    val routed = Connectors.buildTargetConf(
      new SparkConf(false),
      target.copy(clientRoutes = Some(routes))
    )
    val otherRoutes = Connectors.buildTargetConf(
      new SparkConf(false),
      target.copy(clientRoutes = Some(ClientRoutesSettings(List(ClientRouteEndpoint("conn-x")))))
    )

    assertNotEquals(plain, routed)
    assertNotEquals(routed, otherRoutes)
    assertEquals(
      routed,
      Connectors.buildTargetConf(new SparkConf(false), target.copy(clientRoutes = Some(routes)))
    )
  }

  test("client routes connection factory survives Java serialization") {
    val factory = ClientRoutesConnectionFactory(routes.copy(shardAwareness = true))
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(factory)
    out.close()
    val restored = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)).readObject()
    assertEquals(restored, factory)
  }

  // ---------------------------------------------------------------------------
  // cloudContactInfo: path normalization
  // ---------------------------------------------------------------------------

  test("cloudContactInfo: absolute path is normalized to file: URL via File.toURI") {
    val info = Connectors.cloudContactInfo(CloudConfig("/opt/bundle.zip"), NoAuthConf)
    info match {
      case CloudBasedContactInfo(path, _) =>
        assertEquals(path, "file:/opt/bundle.zip")
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
  }

  test("cloudContactInfo: absolute path with spaces is percent-encoded") {
    val info = Connectors.cloudContactInfo(CloudConfig("/opt/my bundle.zip"), NoAuthConf)
    info match {
      case CloudBasedContactInfo(path, _) =>
        assertEquals(path, "file:/opt/my%20bundle.zip")
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
  }

  test("cloudContactInfo: https:// URL is passed through unchanged") {
    val url = "https://storage.example.com/bundle.zip"
    val info = Connectors.cloudContactInfo(CloudConfig(url), NoAuthConf)
    info match {
      case CloudBasedContactInfo(path, _) =>
        assertEquals(path, url)
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
  }

  test("cloudContactInfo: s3:// URL is passed through unchanged") {
    val url = "s3://my-bucket/bundle.zip"
    val info = Connectors.cloudContactInfo(CloudConfig(url), NoAuthConf)
    info match {
      case CloudBasedContactInfo(path, _) =>
        assertEquals(path, url)
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
  }

  test("cloudContactInfo: bare filename (--files) is passed through unchanged") {
    val filename = "secure-connect-prod.zip"
    val info = Connectors.cloudContactInfo(CloudConfig(filename), NoAuthConf)
    info match {
      case CloudBasedContactInfo(path, _) =>
        assertEquals(path, filename)
      case other => fail(s"Expected CloudBasedContactInfo, got $other")
    }
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val baseCassandraSource = SourceSettings.Cassandra(
    host               = "host",
    port               = 9042,
    localDC            = None,
    credentials        = None,
    sslOptions         = None,
    keyspace           = "ks",
    table              = "tbl",
    splitCount         = None,
    connections        = None,
    fetchSize          = 1000,
    preserveTimestamps = false,
    where              = None,
    consistencyLevel   = "LOCAL_QUORUM",
    cloud              = None
  )

  private val baseScyllaTarget = TargetSettings.Scylla(
    host                          = "host",
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
    consistencyLevel              = "LOCAL_QUORUM",
    dropNullPrimaryKeys           = None,
    cloud                         = None
  )
}
