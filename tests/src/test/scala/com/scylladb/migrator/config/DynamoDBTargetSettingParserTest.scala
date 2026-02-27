package com.scylladb.migrator.config

import io.circe.yaml

class DynamoDBTargetSettingParserTest extends munit.FunSuite {

  test("skipInitialSnapshotTransfer is optional") {
    val config =
      """type: dynamodb
        |table: Dummy
        |scanSegments: 1
        |readThroughput: 1
        |throughputReadPercent: 1.0
        |maxMapTasks: 1
        |streamChanges: false
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.skipInitialSnapshotTransfer, None)
  }

  test("explicit skipInitialSnapshotTransfer is taken into account") {
    val config =
      """type: dynamodb
        |table: Dummy
        |scanSegments: 1
        |readThroughput: 1
        |throughputReadPercent: 1.0
        |maxMapTasks: 1
        |streamChanges: false
        |skipInitialSnapshotTransfer: true
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.skipInitialSnapshotTransfer, Some(true))
  }

  test("alternator section is optional") {
    val config =
      """type: dynamodb
        |table: Dummy
        |scanSegments: 1
        |readThroughput: 1
        |throughputReadPercent: 1.0
        |maxMapTasks: 1
        |streamChanges: false
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.alternator, None)
  }

  test("alternator section with all fields") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |alternator:
        |  datacenter: dc1
        |  rack: rack1
        |  activeRefreshIntervalMs: 500
        |  idleRefreshIntervalMs: 30000
        |  compression: true
        |  optimizeHeaders: true
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assert(parsedSettings.alternator.isDefined)
    val alt = parsedSettings.alternator.get
    assertEquals(alt.datacenter, Some("dc1"))
    assertEquals(alt.rack, Some("rack1"))
    assertEquals(alt.activeRefreshIntervalMs, Some(500L))
    assertEquals(alt.idleRefreshIntervalMs, Some(30000L))
    assertEquals(alt.compression, Some(true))
    assertEquals(alt.optimizeHeaders, Some(true))
  }

  test("alternator section with partial fields") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |alternator:
        |  datacenter: us-east-1
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assert(parsedSettings.alternator.isDefined)
    val alt = parsedSettings.alternator.get
    assertEquals(alt.datacenter, Some("us-east-1"))
    assertEquals(alt.rack, None)
    assertEquals(alt.compression, None)
  }

  private def parseDynamoDBTargetSettings(yamlContent: String): TargetSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Right(dynamoDB: TargetSettings.DynamoDB) => dynamoDB
      case other => fail(s"Failed to parse type TargetSettings.DynamoDB. Got ${other}.")
    }

}
