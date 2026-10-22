package com.scylladb.migrator.config

import io.circe.yaml
import io.circe.syntax._

class ContactPointsTest extends munit.FunSuite {

  test("source cassandra: single host is unchanged") {
    val parsed = parseSourceCassandra(sourceYaml("host: 10.0.0.1"))
    assertEquals(parsed.host, "10.0.0.1")
  }

  test("source cassandra: YAML list of hosts is normalized to comma-separated") {
    val parsed = parseSourceCassandra(sourceYaml("host: [10.0.0.1, 10.0.0.2, node-3]"))
    assertEquals(parsed.host, "10.0.0.1,10.0.0.2,node-3")
  }

  test("source cassandra: comma-separated hosts are accepted") {
    val parsed = parseSourceCassandra(sourceYaml("host: \"10.0.0.1, 10.0.0.2\""))
    assertEquals(ContactPoints.split(parsed.host), List("10.0.0.1", "10.0.0.2"))
  }

  test("target scylla: YAML block list of hosts is accepted") {
    val parsed = parseTargetScylla(
      targetYaml(
        """host:
          |  - 10.0.0.1
          |  - "[2001:db8::1]"""".stripMargin
      )
    )
    assertEquals(parsed.host, "10.0.0.1,[2001:db8::1]")
  }

  test("target scylla: an invalid entry in the list is rejected") {
    val result = parseTarget(targetYaml("host: [10.0.0.1, \"bad/host\"]"))
    assert(result.isLeft, s"expected failure, got $result")
    assert(result.left.exists(_.getMessage.contains("Invalid Scylla target host")), result.toString)
  }

  test("target scylla: an empty list is rejected") {
    val result = parseTarget(targetYaml("host: []"))
    assert(result.left.exists(_.getMessage.contains("must not be empty")), result.toString)
  }

  test("target scylla: cloud + host list is still mutually exclusive") {
    val config =
      """type: scylla
        |cloud:
        |  secureBundlePath: /opt/bundle.zip
        |host: [10.0.0.1, 10.0.0.2]
        |keyspace: ks
        |table: tbl
        |consistencyLevel: LOCAL_QUORUM
        |stripTrailingZerosForDecimals: false
        |""".stripMargin
    val result = parseTarget(config)
    assert(result.left.exists(_.getMessage.contains("mutually exclusive")), result.toString)
  }

  test("target scylla: host list survives a savepoint round-trip") {
    val original = parseTargetScylla(targetYaml("host: [10.0.0.1, 10.0.0.2]"))
    val rendered = (original: TargetSettings).asJson.spaces2
    val roundTrip = parseTargetScylla(rendered)
    assertEquals(roundTrip, original)
  }

  test("source cassandra: DNS names and IPs can be mixed") {
    val parsed = parseSourceCassandra(
      sourceYaml("host: [node-0.scylla.example.com, node-1.scylla.example.com, 10.0.0.3]")
    )
    assertEquals(
      ContactPoints.split(parsed.host),
      List("node-0.scylla.example.com", "node-1.scylla.example.com", "10.0.0.3")
    )
  }

  test("split: trims whitespace and drops empty entries") {
    assertEquals(ContactPoints.split(" a , b,,c "), List("a", "b", "c"))
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def sourceYaml(hostLine: String): String =
    s"""type: cassandra
       |$hostLine
       |port: 9042
       |keyspace: ks
       |table: tbl
       |consistencyLevel: LOCAL_QUORUM
       |preserveTimestamps: true
       |fetchSize: 1000
       |""".stripMargin

  private def targetYaml(hostLines: String): String =
    s"""type: scylla
       |$hostLines
       |port: 9042
       |keyspace: ks
       |table: tbl
       |consistencyLevel: LOCAL_QUORUM
       |stripTrailingZerosForDecimals: false
       |""".stripMargin

  private def parseTarget(yamlContent: String) =
    yaml.parser.parse(yamlContent).flatMap(_.as[TargetSettings])

  private def parseSourceCassandra(yamlContent: String): SourceSettings.Cassandra =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                        => throw error
      case Right(c: SourceSettings.Cassandra) => c
      case Right(other)                       => fail(s"Expected Cassandra, got $other")
    }

  private def parseTargetScylla(yamlContent: String): TargetSettings.Scylla =
    parseTarget(yamlContent) match {
      case Left(error)                     => throw error
      case Right(s: TargetSettings.Scylla) => s
      case Right(other)                    => fail(s"Expected Scylla, got $other")
    }
}
