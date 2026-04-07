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

<<<<<<< HEAD
  test("alternator maxItemsPerBatch is optional and defaults to None") {
=======
  test("removeConsumedCapacity decodes as None when omitted from YAML") {
>>>>>>> 3d1fea0 (address the case of missing removeConsumedCapacity in the yaml (#256))
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
<<<<<<< HEAD
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assert(parsedSettings.alternator.isDefined)
    assertEquals(parsedSettings.alternator.get.maxItemsPerBatch, None)
  }

  test("alternator maxItemsPerBatch is parsed when set") {
=======
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.removeConsumedCapacity, None)
  }

  test("removeConsumedCapacity decodes as Some(true) when explicitly set") {
>>>>>>> 3d1fea0 (address the case of missing removeConsumedCapacity in the yaml (#256))
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
<<<<<<< HEAD
        |alternator:
        |  maxItemsPerBatch: 100
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.alternator.get.maxItemsPerBatch, Some(100))
=======
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
>>>>>>> 3d1fea0 (address the case of missing removeConsumedCapacity in the yaml (#256))
  }

  private def parseDynamoDBTargetSettings(yamlContent: String): TargetSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Right(dynamoDB: TargetSettings.DynamoDB) => dynamoDB
      case other => fail(s"Failed to parse type TargetSettings.DynamoDB. Got ${other}.")
    }

}
