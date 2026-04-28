package com.scylladb.migrator

import com.datastax.spark.connector.cql.{
  CloudBasedContactInfo,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{ CloudConfig, Credentials, SourceSettings, TargetSettings }
import org.apache.spark.SparkConf

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
        assertEquals(path, "/opt/bundle.zip")
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
        assertEquals(path, "/opt/bundle.zip")
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
        assertEquals(path, "/opt/bundle.zip")
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
