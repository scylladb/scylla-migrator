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

  private def parseDynamoDBTargetSettings(yamlContent: String): TargetSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .right
      .flatMap(_.as[TargetSettings])
      .right
      .get match {
      case dynamoDB: TargetSettings.DynamoDB => dynamoDB
      case other                             => fail(s"Failed to parse type TargetSettings.DynamoDB. Got ${other}.")
    }

}
