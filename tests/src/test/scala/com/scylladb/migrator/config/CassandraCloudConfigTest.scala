package com.scylladb.migrator.config

import io.circe.{ yaml, DecodingFailure }
import io.circe.syntax._

class CassandraCloudConfigTest extends munit.FunSuite {

  // ---------------------------------------------------------------------------
  // Source: cassandra
  // ---------------------------------------------------------------------------

  test("source cassandra: parses with cloud bundle, no host/port") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: /opt/migrator/secure-connect-mycluster.zip
        |credentials:
        |  username: client-id
        |  password: client-secret
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(
      parsed.cloud.map(_.secureBundlePath),
      Some("/opt/migrator/secure-connect-mycluster.zip")
    )
    assertEquals(parsed.host, "")
    assertEquals(parsed.port, 0)
    assertEquals(parsed.credentials.map(_.username), Some("client-id"))
    assertEquals(parsed.localDC, None)
    assertEquals(parsed.sslOptions, None)
  }

  test("source cassandra: rejects cloud + host (mutual exclusion)") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |host: some-host
        |port: 9042
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("mutually exclusive"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: rejects neither cloud nor host") {
    val config =
      """type: cassandra
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("required"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: rejects host without port") {
    val config =
      """type: cassandra
        |host: 127.0.0.1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("host") && ex.getMessage.contains("port"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: rejects port without host") {
    val config =
      """type: cassandra
        |port: 9042
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("host") && ex.getMessage.contains("port"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: rejects cloud + sslOptions") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |sslOptions:
        |  clientAuthEnabled: false
        |  enabled: true
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("sslOptions"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: rejects cloud + localDC") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |localDC: dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("localDC"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud with host:null (YAML implicit null) is accepted") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |host: null
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(parsed.cloud.map(_.secureBundlePath), Some("/opt/bundle.zip"))
    assertEquals(parsed.host, "")
    assertEquals(parsed.port, 0)
  }

  test("source cassandra: cloud with empty secureBundlePath is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: ""
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("secureBundlePath must not be empty"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud with whitespace-only secureBundlePath is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: "  "
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("secureBundlePath must not be empty"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud secureBundlePath is trimmed before storing") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: " /opt/bundle.zip "
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(parsed.cloud.map(_.secureBundlePath), Some("/opt/bundle.zip"))
  }

  test("source cassandra: cloud with bare filename (--files pattern) is accepted") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(parsed.cloud.map(_.secureBundlePath), Some("secure-connect.zip"))
  }

  test("source cassandra: cloud with relative path containing slash is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: some/path/bundle.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("absolute local path") || ex.getMessage.contains("bare filename"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud with https:// secureBundlePath is accepted") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: https://storage.example.com/secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(
      parsed.cloud.map(_.secureBundlePath),
      Some("https://storage.example.com/secure-connect.zip")
    )
  }

  test("source cassandra: cloud with s3:// secureBundlePath is accepted") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: s3://my-bucket/bundles/secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(
      parsed.cloud.map(_.secureBundlePath),
      Some("s3://my-bucket/bundles/secure-connect.zip")
    )
  }

  test("source cassandra: cloud with file:// secureBundlePath is accepted") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: file:/opt/migrator/secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(
      parsed.cloud.map(_.secureBundlePath),
      Some("file:/opt/migrator/secure-connect.zip")
    )
  }

  test("source cassandra: cloud with plain HTTP secureBundlePath is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: http://example.com/secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("plain HTTP"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud with URL user-info is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: https://user:pass@example.com/secure-connect.zip
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("user-info"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: cloud with URL query string is rejected") {
    val config =
      """type: cassandra
        |cloud:
        |  secureBundlePath: https://example.com/secure-connect.zip?token=secret
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseSourceCassandra(config))
    assert(
      ex.getMessage.contains("query string"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("source cassandra: legacy host/port still parses") {
    val config =
      """type: cassandra
        |host: 127.0.0.1
        |port: 9042
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin

    val parsed = parseSourceCassandra(config)
    assertEquals(parsed.cloud, None)
    assertEquals(parsed.host, "127.0.0.1")
    assertEquals(parsed.port, 9042)
  }

  test("source cassandra: round-trip preserves cloud and omits host/port") {
    val original = SourceSettings.Cassandra(
      host               = "",
      port               = 0,
      localDC            = None,
      credentials        = Some(Credentials("client-id", "client-secret")),
      sslOptions         = None,
      keyspace           = "ks",
      table              = "tbl",
      splitCount         = Some(256),
      connections        = Some(8),
      fetchSize          = 1000,
      preserveTimestamps = true,
      where              = None,
      consistencyLevel   = "LOCAL_QUORUM",
      cloud              = Some(CloudConfig("/opt/bundle.zip"))
    )

    val rendered = (original: SourceSettings).asJson.noSpaces
    assert(!rendered.contains("\"host\""), s"unexpected host in: $rendered")
    assert(!rendered.contains("\"port\""), s"unexpected port in: $rendered")
    assert(rendered.contains("\"cloud\""))

    val roundTrip = io.circe.parser
      .parse(rendered)
      .flatMap(_.as[SourceSettings])
      .toOption
      .collect { case c: SourceSettings.Cassandra => c }

    assertEquals(roundTrip, Some(original))
  }

  // ---------------------------------------------------------------------------
  // Target: scylla
  // ---------------------------------------------------------------------------

  test("target scylla: parses with cloud bundle") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |credentials:
        |  username: u
        |  password: p
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val parsed = parseTargetScylla(config)
    assertEquals(parsed.cloud.map(_.secureBundlePath), Some("/opt/bundle.zip"))
    assertEquals(parsed.host, "")
    assertEquals(parsed.port, 0)
  }

  test("target scylla: cloud with host:null is accepted") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |host: null
        |credentials:
        |  username: u
        |  password: p
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val parsed = parseTargetScylla(config)
    assertEquals(parsed.cloud.map(_.secureBundlePath), Some("/opt/bundle.zip"))
    assertEquals(parsed.host, "")
    assertEquals(parsed.port, 0)
  }

  test("target scylla: rejects cloud + host") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |host: scylla
        |port: 9042
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseTargetScylla(config))
    assert(ex.getMessage.contains("mutually exclusive"))
  }

  test("target scylla: rejects neither cloud nor host") {
    val config =
      """type: scylla
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseTargetScylla(config))
    assert(ex.getMessage.contains("required"))
  }

  test("target scylla: rejects host without port") {
    val config =
      """type: scylla
        |host: scylla
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseTargetScylla(config))
    assert(
      ex.getMessage.contains("host") && ex.getMessage.contains("port"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("target scylla: rejects port without host") {
    val config =
      """type: scylla
        |port: 9042
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin

    val ex = intercept[DecodingFailure](parseTargetScylla(config))
    assert(
      ex.getMessage.contains("host") && ex.getMessage.contains("port"),
      s"unexpected error: ${ex.getMessage}"
    )
  }

  test("target scylla: encoder filters null optional fields") {
    import io.circe.syntax._
    val target: TargetSettings = TargetSettings.Scylla(
      host                          = "h",
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
    val json = target.asJson
    val obj = json.asObject.get
    assert(!obj.contains("localDC"), s"localDC should be absent: ${json.noSpaces}")
    assert(!obj.contains("sslOptions"), s"sslOptions should be absent: ${json.noSpaces}")
    assert(!obj.contains("writeTTLInS"), s"writeTTLInS should be absent: ${json.noSpaces}")
    assert(
      !obj.contains("dropNullPrimaryKeys"),
      s"dropNullPrimaryKeys should be absent: ${json.noSpaces}"
    )
    assert(!obj.contains("connections"), s"connections should be absent: ${json.noSpaces}")
  }

  test("target scylla: round-trip preserves cloud and omits host/port") {
    val original = TargetSettings.Scylla(
      host                          = "",
      port                          = 0,
      localDC                       = None,
      credentials                   = Some(Credentials("u", "p")),
      sslOptions                    = None,
      keyspace                      = "ks",
      table                         = "tbl",
      connections                   = Some(16),
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "LOCAL_QUORUM",
      dropNullPrimaryKeys           = None,
      cloud                         = Some(CloudConfig("/opt/bundle.zip"))
    )

    val rendered = (original: TargetSettings).asJson.noSpaces
    assert(!rendered.contains("\"host\""), s"unexpected host in: $rendered")
    assert(!rendered.contains("\"port\""), s"unexpected port in: $rendered")
    assert(rendered.contains("\"cloud\""))

    val roundTrip = io.circe.parser
      .parse(rendered)
      .flatMap(_.as[TargetSettings])
      .toOption
      .collect { case s: TargetSettings.Scylla => s }

    assertEquals(roundTrip, Some(original))
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def parseSourceCassandra(yamlContent: String): SourceSettings.Cassandra =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                        => throw error
      case Right(c: SourceSettings.Cassandra) => c
      case Right(other)                       => fail(s"Expected Cassandra, got $other")
    }

  private def parseTargetScylla(yamlContent: String): TargetSettings.Scylla =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Left(error)                     => throw error
      case Right(s: TargetSettings.Scylla) => s
      case Right(other)                    => fail(s"Expected Scylla, got $other")
    }
}
