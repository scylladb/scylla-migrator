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

  test("alternator maxItemsPerBatch is optional and defaults to None") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assert(parsedSettings.alternator.isDefined)
    assertEquals(parsedSettings.alternator.get.maxItemsPerBatch, None)
  }

  test("alternator maxItemsPerBatch is parsed when set") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |alternator:
        |  maxItemsPerBatch: 100
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.alternator.get.maxItemsPerBatch, Some(100))
  }

  test("removeConsumedCapacity decodes as None when omitted from YAML") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.removeConsumedCapacity, None)
  }

  test("removeConsumedCapacity decodes as Some(true) when explicitly set") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |removeConsumedCapacity: true
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.removeConsumedCapacity, Some(true))
  }

  test("removeConsumedCapacity decodes as Some(false) when explicitly disabled") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |removeConsumedCapacity: false
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.removeConsumedCapacity, Some(false))
  }

  private def parseDynamoDBTargetSettings(yamlContent: String): TargetSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Right(dynamoDB: TargetSettings.DynamoDB) => dynamoDB
      case other => fail(s"Failed to parse type TargetSettings.DynamoDB. Got ${other}.")
    }

}
