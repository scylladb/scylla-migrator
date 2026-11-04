package com.scylladb.migrator.config

import com.datastax.oss.driver.api.core.config.ClientRouteProxy
import com.datastax.spark.connector.cql.{ ClientRouteEndpointConf, ClientRoutesConf }
import io.circe.{ yaml, DecodingFailure }
import io.circe.syntax._

import scala.jdk.CollectionConverters._

class ClientRoutesConfigTest extends munit.FunSuite {

  // ---------------------------------------------------------------------------
  // Source: cassandra
  // ---------------------------------------------------------------------------

  test("source: parses clientRoutes with several connection IDs") {
    val parsed = parseSource(
      sourceYaml(
        """clientRoutes:
          |  endpoints:
          |    - connectionId: conn-dc1
          |    - connectionId: conn-dc2
          |      connectionAddr: vpce-123.example.com
          |""".stripMargin
      )
    )

    assertEquals(
      parsed.clientRoutes,
      Some(
        ClientRoutesSettings(
          List(
            ClientRouteEndpoint("conn-dc1"),
            ClientRouteEndpoint("conn-dc2", Some("vpce-123.example.com"))
          ),
          shardAwareness = false
        )
      )
    )
    assertEquals(parsed.host, "pl-endpoint.example.com")
    assertEquals(parsed.port, 9042)
    assertEquals(parsed.localDC, Some("dc1"))
  }

  test("source: shardAwareness can be enabled") {
    val parsed = parseSource(
      sourceYaml(
        """clientRoutes:
          |  endpoints:
          |    - connectionId: conn-dc1
          |  shardAwareness: true
          |""".stripMargin
      )
    )
    assertEquals(parsed.clientRoutes.map(_.shardAwareness), Some(true))
  }

  test("source: connectionId and connectionAddr are trimmed") {
    val parsed = parseSource(
      sourceYaml(
        """clientRoutes:
          |  endpoints:
          |    - connectionId: " conn-dc1 "
          |      connectionAddr: " 10.0.0.5 "
          |""".stripMargin
      )
    )
    assertEquals(
      parsed.clientRoutes.map(_.endpoints),
      Some(List(ClientRouteEndpoint("conn-dc1", Some("10.0.0.5"))))
    )
  }

  test("source: accepts clientRoutes without localDC (the connector infers it)") {
    val config =
      """type: scylla
        |host: pl-endpoint.example.com
        |port: 9042
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin
    val parsed = parseSource(config)
    assertEquals(parsed.localDC, None)
    assertEquals(parsed.clientRoutes.map(_.endpoints), Some(List(ClientRouteEndpoint("conn-dc1"))))
  }

  test("source: rejects clientRoutes together with cloud") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin
    assertDecodingError(parseSource(config), "mutually exclusive with 'cloud'")
  }

  test("source: rejects clientRoutes without host/port") {
    val config =
      """type: scylla
        |localDC: dc1
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |preserveTimestamps: true
        |fetchSize: 1000
        |""".stripMargin
    assertDecodingError(parseSource(config), "'host' and 'port' are required")
  }

  test("source: rejects an empty endpoint list") {
    assertDecodingError(
      parseSource(sourceYaml("clientRoutes:\n  endpoints: []\n")),
      "at least one connection ID"
    )
  }

  test("source: rejects a blank connectionId") {
    assertDecodingError(
      parseSource(
        sourceYaml(
          """clientRoutes:
            |  endpoints:
            |    - connectionId: "  "
            |""".stripMargin
        )
      ),
      "clientRoutes.endpoints[0].connectionId must not be empty"
    )
  }

  test("source: rejects duplicate connection IDs") {
    assertDecodingError(
      parseSource(
        sourceYaml(
          """clientRoutes:
            |  endpoints:
            |    - connectionId: conn-dc1
            |    - connectionId: conn-dc1
            |      connectionAddr: other.example.com
            |""".stripMargin
        )
      ),
      "duplicate connectionId(s): conn-dc1"
    )
  }

  test("source: rejects a blank connectionAddr") {
    assertDecodingError(
      parseSource(
        sourceYaml(
          """clientRoutes:
            |  endpoints:
            |    - connectionId: conn-dc1
            |      connectionAddr: ""
            |""".stripMargin
        )
      ),
      "clientRoutes.endpoints[0].connectionAddr must not be empty"
    )
  }

  test("source: rejects a connectionAddr that includes a port") {
    assertDecodingError(
      parseSource(
        sourceYaml(
          """clientRoutes:
            |  endpoints:
            |    - connectionId: conn-dc1
            |      connectionAddr: vpce.example.com:9042
            |""".stripMargin
        )
      ),
      "without a port"
    )
  }

  test("source: rejects a connectionAddr with invalid characters") {
    assertDecodingError(
      parseSource(
        sourceYaml(
          """clientRoutes:
            |  endpoints:
            |    - connectionId: conn-dc1
            |      connectionAddr: https://vpce.example.com
            |""".stripMargin
        )
      ),
      "clientRoutes.endpoints[0]"
    )
  }

  test("source: round-trip preserves clientRoutes and host/port") {
    val original = SourceSettings.Cassandra(
      host               = "pl-endpoint.example.com",
      port               = 9042,
      localDC            = Some("dc1"),
      credentials        = Some(Credentials("u", "p")),
      sslOptions         = None,
      keyspace           = "ks",
      table              = "tbl",
      splitCount         = None,
      connections        = None,
      fetchSize          = 1000,
      preserveTimestamps = true,
      where              = None,
      consistencyLevel   = "LOCAL_QUORUM",
      clientRoutes = Some(
        ClientRoutesSettings(
          List(ClientRouteEndpoint("conn-dc1"), ClientRouteEndpoint("conn-dc2", Some("10.0.0.5"))),
          shardAwareness = true
        )
      )
    )

    val rendered = (original: SourceSettings).asJson.noSpaces
    assert(rendered.contains("\"clientRoutes\""), rendered)
    assert(!rendered.contains("\"connectionAddr\":null"), rendered)

    val roundTrip = io.circe.parser
      .parse(rendered)
      .flatMap(_.as[SourceSettings])
      .toOption
      .collect { case c: SourceSettings.Cassandra => c }
    assertEquals(roundTrip, Some(original))
  }

  test("source: encoder omits clientRoutes when unset") {
    val rendered = (parseSource(sourceYaml("")): SourceSettings).asJson.noSpaces
    assert(!rendered.contains("clientRoutes"), rendered)
  }

  // ---------------------------------------------------------------------------
  // Target: scylla
  // ---------------------------------------------------------------------------

  test("target: parses clientRoutes") {
    val parsed = parseTarget(
      targetYaml(
        """clientRoutes:
          |  endpoints:
          |    - connectionId: conn-dc1
          |""".stripMargin
      )
    )
    assertEquals(
      parsed.clientRoutes,
      Some(ClientRoutesSettings(List(ClientRouteEndpoint("conn-dc1"))))
    )
    assertEquals(parsed.localDC, Some("dc1"))
  }

  test("target: host may list several private endpoint addresses") {
    val config =
      """type: scylla
        |host: [pe-az1.example.com, pe-az2.example.com]
        |port: 9142
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1-az1
        |    - connectionId: conn-dc1-az2
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin
    val parsed = parseTarget(config)
    assertEquals(parsed.host, "pe-az1.example.com,pe-az2.example.com")
    assertEquals(parsed.port, 9142)
    assertEquals(parsed.clientRoutes.map(_.endpoints.size), Some(2))
  }

  test("target: accepts clientRoutes without localDC (the connector infers it)") {
    val config =
      """type: scylla
        |host: pl-endpoint.example.com
        |port: 9042
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin
    val parsed = parseTarget(config)
    assertEquals(parsed.localDC, None)
    assertEquals(parsed.clientRoutes.map(_.endpoints), Some(List(ClientRouteEndpoint("conn-dc1"))))
  }

  test("target: rejects clientRoutes together with cloud") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |clientRoutes:
        |  endpoints:
        |    - connectionId: conn-dc1
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin
    assertDecodingError(parseTarget(config), "mutually exclusive with 'cloud'")
  }

  test("target: round-trip preserves clientRoutes") {
    val original = TargetSettings.Scylla(
      host                          = "pl-endpoint.example.com",
      port                          = 9042,
      localDC                       = Some("dc1"),
      credentials                   = None,
      sslOptions                    = None,
      keyspace                      = "ks",
      table                         = "tbl",
      connections                   = Some(16),
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "LOCAL_QUORUM",
      clientRoutes = Some(ClientRoutesSettings(List(ClientRouteEndpoint("conn-dc1"))))
    )

    val roundTrip = io.circe.parser
      .parse((original: TargetSettings).asJson.noSpaces)
      .flatMap(_.as[TargetSettings])
      .toOption
      .collect { case s: TargetSettings.Scylla => s }
    assertEquals(roundTrip, Some(original))
  }

  // ---------------------------------------------------------------------------
  // Driver mapping
  // ---------------------------------------------------------------------------

  test("toConnectorConf maps endpoints and shard awareness") {
    val connectorConf = ClientRoutesSettings(
      List(ClientRouteEndpoint("conn-dc1"), ClientRouteEndpoint("conn-dc2", Some("10.0.0.5"))),
      shardAwareness = true
    ).toConnectorConf

    assertEquals(
      connectorConf,
      ClientRoutesConf(
        Seq(
          ClientRouteEndpointConf("conn-dc1"),
          ClientRouteEndpointConf("conn-dc2", Some("10.0.0.5"))
        ),
        shardAwareness = true
      )
    )
    assert(connectorConf.verify, "the connector must verify the routes")
    assertEquals(
      connectorConf.toDriver.getEndpoints.asScala.toList,
      List(new ClientRouteProxy("conn-dc1", null), new ClientRouteProxy("conn-dc2", "10.0.0.5"))
    )
  }

  test("target: accepts one connection ID per availability zone of several datacenters") {
    val connectionIds =
      for {
        dc <- List("dc1", "dc2")
        az <- List("az1", "az2", "az3")
      } yield s"conn-$dc-$az"
    val parsed = parseTarget(
      targetYaml(
        connectionIds
          .map(id => s"    - connectionId: $id\n")
          .mkString("clientRoutes:\n  endpoints:\n", "", "")
      )
    )

    assertEquals(
      parsed.clientRoutes.map(_.endpoints.map(_.connectionId)),
      Some(connectionIds)
    )
    assertEquals(
      parsed.clientRoutes.get.toConnectorConf.toDriver.getEndpoints.asScala.toList
        .map(_.getConnectionId),
      connectionIds
    )
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def sourceYaml(extra: String): String =
    """type: scylla
      |host: pl-endpoint.example.com
      |port: 9042
      |localDC: dc1
      |keyspace: ks
      |table: tbl
      |consistencyLevel: LOCAL_QUORUM
      |preserveTimestamps: true
      |fetchSize: 1000
      |""".stripMargin + extra

  private def targetYaml(extra: String): String =
    """type: scylla
      |host: pl-endpoint.example.com
      |port: 9042
      |localDC: dc1
      |keyspace: ks
      |table: tbl
      |consistencyLevel: LOCAL_QUORUM
      |stripTrailingZerosForDecimals: false
      |""".stripMargin + extra

  private def assertDecodingError(parse: => Any, expected: String): Unit = {
    val error = intercept[DecodingFailure](parse)
    assert(error.getMessage.contains(expected), s"unexpected error: ${error.getMessage}")
  }

  private def parseSource(yamlContent: String): SourceSettings.Cassandra =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                        => throw error
      case Right(c: SourceSettings.Cassandra) => c
      case Right(other)                       => fail(s"Expected Cassandra, got $other")
    }

  private def parseTarget(yamlContent: String): TargetSettings.Scylla =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Left(error)                     => throw error
      case Right(s: TargetSettings.Scylla) => s
      case Right(other)                    => fail(s"Expected Scylla, got $other")
    }
}
