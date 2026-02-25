package com.scylladb.migrator.config

import io.circe.yaml

class DynamoDBSourceSettingWithAlternatorTest extends munit.FunSuite {

  test("DynamoDB source without alternator section") {
    val config =
      """type: dynamodb
        |table: MyTable
        |region: us-east-1
        |""".stripMargin

    val parsed = parseDynamoDBSourceSettings(config)
    assertEquals(parsed.alternator, None)
    assertEquals(parsed.table, "MyTable")
  }

  test("DynamoDB source with alternator section") {
    val config =
      """type: dynamodb
        |table: MyTable
        |region: us-east-1
        |endpoint:
        |  host: http://localhost
        |  port: 8000
        |alternator:
        |  datacenter: dc1
        |  rack: rack1
        |  activeRefreshIntervalMs: 500
        |  idleRefreshIntervalMs: 30000
        |  compression: true
        |  optimizeHeaders: true
        |""".stripMargin

    val parsed = parseDynamoDBSourceSettings(config)
    assert(parsed.alternator.isDefined)
    val alt = parsed.alternator.get
    assertEquals(alt.datacenter, Some("dc1"))
    assertEquals(alt.rack, Some("rack1"))
    assertEquals(alt.activeRefreshIntervalMs, Some(500L))
    assertEquals(alt.idleRefreshIntervalMs, Some(30000L))
    assertEquals(alt.compression, Some(true))
    assertEquals(alt.optimizeHeaders, Some(true))
  }

  test("DynamoDB source with empty alternator section") {
    val config =
      """type: dynamodb
        |table: MyTable
        |region: us-east-1
        |alternator: {}
        |""".stripMargin

    val parsed = parseDynamoDBSourceSettings(config)
    assert(parsed.alternator.isDefined)
    val alt = parsed.alternator.get
    assertEquals(alt.datacenter, None)
    assertEquals(alt.rack, None)
  }

  test("DynamoDB source with partial alternator section") {
    val config =
      """type: dynamodb
        |table: MyTable
        |region: us-east-1
        |alternator:
        |  datacenter: dc1
        |  compression: false
        |""".stripMargin

    val parsed = parseDynamoDBSourceSettings(config)
    assert(parsed.alternator.isDefined)
    val alt = parsed.alternator.get
    assertEquals(alt.datacenter, Some("dc1"))
    assertEquals(alt.rack, None)
    assertEquals(alt.compression, Some(false))
    assertEquals(alt.optimizeHeaders, None)
  }

  private def parseDynamoDBSourceSettings(yamlContent: String): SourceSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Right(source: SourceSettings.DynamoDB) => source
      case Left(error)                            => throw error
      case Right(other) => fail(s"Expected DynamoDB source settings, got ${other}")
    }

}
