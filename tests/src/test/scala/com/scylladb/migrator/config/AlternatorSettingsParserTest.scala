package com.scylladb.migrator.config

import io.circe.yaml

class AlternatorSettingsParserTest extends munit.FunSuite {

  test("parse full alternator settings") {
    val config =
      """datacenter: dc1
        |rack: rack1
        |activeRefreshIntervalMs: 500
        |idleRefreshIntervalMs: 30000
        |compression: true
        |optimizeHeaders: true
        |""".stripMargin

    val parsed = parseAlternatorSettings(config)
    assertEquals(parsed.datacenter, Some("dc1"))
    assertEquals(parsed.rack, Some("rack1"))
    assertEquals(parsed.activeRefreshIntervalMs, Some(500L))
    assertEquals(parsed.idleRefreshIntervalMs, Some(30000L))
    assertEquals(parsed.compression, Some(true))
    assertEquals(parsed.optimizeHeaders, Some(true))
  }

  test("all fields are optional") {
    val config = ""
    val parsed = parseAlternatorSettings(config)
    assertEquals(parsed.datacenter, None)
    assertEquals(parsed.rack, None)
    assertEquals(parsed.activeRefreshIntervalMs, None)
    assertEquals(parsed.idleRefreshIntervalMs, None)
    assertEquals(parsed.compression, None)
    assertEquals(parsed.optimizeHeaders, None)
  }

  test("partial alternator settings - datacenter only") {
    val config = "datacenter: us-east-1"
    val parsed = parseAlternatorSettings(config)
    assertEquals(parsed.datacenter, Some("us-east-1"))
    assertEquals(parsed.rack, None)
  }

  test("partial alternator settings - datacenter and rack") {
    val config =
      """datacenter: dc1
        |rack: rack2
        |""".stripMargin
    val parsed = parseAlternatorSettings(config)
    assertEquals(parsed.datacenter, Some("dc1"))
    assertEquals(parsed.rack, Some("rack2"))
    assertEquals(parsed.compression, None)
  }

  test("compression and optimizeHeaders default to None when not set") {
    val config =
      """datacenter: dc1
        |""".stripMargin
    val parsed = parseAlternatorSettings(config)
    assertEquals(parsed.compression, None)
    assertEquals(parsed.optimizeHeaders, None)
  }

  test("round-trip serialization") {
    val original = AlternatorSettings(
      datacenter              = Some("dc1"),
      rack                    = Some("rack1"),
      activeRefreshIntervalMs = Some(1000L),
      idleRefreshIntervalMs   = Some(60000L),
      compression             = Some(true),
      optimizeHeaders         = Some(false)
    )
    import io.circe.syntax._
    val json = original.asJson
    val decoded = json.as[AlternatorSettings]
    assertEquals(decoded, Right(original))
  }

  private def parseAlternatorSettings(yamlContent: String): AlternatorSettings =
    // circe-yaml parses empty string as null, handle that
    if (yamlContent.trim.isEmpty)
      AlternatorSettings()
    else
      yaml.parser
        .parse(yamlContent)
        .flatMap(_.as[AlternatorSettings]) match {
        case Left(error)  => throw error
        case Right(value) => value
      }

}
