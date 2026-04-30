package com.scylladb.migrator.config

import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken }
import io.circe.yaml

class MigratorConfigTest extends munit.FunSuite {

  test("full MigratorConfig with Alternator types round-trips through YAML") {
    val config = MigratorConfig(
      source = SourceSettings.Alternator(
        alternatorEndpoint     = DynamoDBEndpoint("http://10.0.0.1", 8000),
        region                 = Some("us-east-1"),
        credentials            = None,
        table                  = "SrcTable",
        scanSegments           = Some(4),
        readThroughput         = Some(100),
        throughputReadPercent  = Some(0.5f),
        maxMapTasks            = Some(2),
        removeConsumedCapacity = true,
        alternatorConfig = AlternatorSettings(
          datacenter              = Some("dc1"),
          rack                    = Some("rack1"),
          activeRefreshIntervalMs = Some(5000L)
        )
      ),
      target = TargetSettings.Alternator(
        alternatorEndpoint          = DynamoDBEndpoint("http://10.0.0.2", 8000),
        region                      = None,
        credentials                 = None,
        table                       = "DstTable",
        writeThroughput             = Some(200),
        throughputWritePercent      = Some(0.8f),
        streamChanges               = StreamChangesSetting.Disabled,
        skipInitialSnapshotTransfer = None,
        removeConsumedCapacity      = true,
        billingMode                 = None,
        alternatorConfig            = AlternatorSettings(datacenter = Some("dc2"))
      ),
      renames          = Some(List(Rename("oldCol", "newCol"))),
      savepoints       = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.source, config.source)
    assertEquals(roundTripped.target, config.target)
    assertEquals(roundTripped.renames, config.renames)
    assertEquals(roundTripped.savepoints, config.savepoints)
  }

  test("alternator source with streamChanges true is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: true
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with alternator target and streamChanges true is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.2
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb-s3-export source with streamChanges true is rejected") {
    val config =
      """source:
        |  type: dynamodb-s3-export
        |  bucket: foobar
        |  manifestKey: manifest.json
        |  tableDescription:
        |    attributeDefinitions:
        |      - name: id
        |        type: S
        |    keySchema:
        |      - name: id
        |        type: HASH
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with streamChanges false is accepted") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
  }

  test("dynamodb source to alternator target parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
  }

  test("alternator source to alternator target parses successfully") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.2
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.Alternator])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
  }

  test("alternator source to dynamodb-s3-export target is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb-s3-export
        |  path: /tmp/export
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("dynamodb-s3-export")),
      s"Expected error about unsupported alternator -> dynamodb-s3-export, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source to alternator target with streaming parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
    assertEquals(
      cfg.target.asInstanceOf[TargetSettings.Alternator].streamChanges,
      StreamChangesSetting.DynamoDBStreams
    )
  }

  test("dynamodb-s3-export source with streamChanges false is accepted") {
    val config =
      """source:
        |  type: dynamodb-s3-export
        |  bucket: foobar
        |  manifestKey: manifest.json
        |  tableDescription:
        |    attributeDefinitions:
        |      - name: id
        |        type: S
        |    keySchema:
        |      - name: id
        |        type: HASH
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
  }

  test("dynamodb source to dynamodb target parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.DynamoDB])
  }

  test("dynamodb source to dynamodb target with streaming parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: true
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.DynamoDB])
    assertEquals(
      cfg.target.asInstanceOf[TargetSettings.DynamoDB].streamChanges,
      StreamChangesSetting.DynamoDBStreams
    )
  }

  test("MigratorConfig roundtrip with skipTokenRanges containing LongToken and BigIntToken") {
    val config = MigratorConfig(
      source = SourceSettings.DynamoDB(
        endpoint              = None,
        region                = Some("us-east-1"),
        credentials           = None,
        table                 = "SrcTable",
        scanSegments          = None,
        readThroughput        = None,
        throughputReadPercent = None,
        maxMapTasks           = None
      ),
      target = TargetSettings.DynamoDB(
        endpoint                    = None,
        region                      = None,
        credentials                 = None,
        table                       = "DstTable",
        writeThroughput             = None,
        throughputWritePercent      = None,
        streamChanges               = StreamChangesSetting.Disabled,
        skipInitialSnapshotTransfer = None
      ),
      renames    = None,
      savepoints = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges = Some(
        Set(
          (LongToken(0L), LongToken(100L)),
          (
            BigIntToken(BigInt("123456789012345678901234567890")),
            BigIntToken(BigInt("987654321098765432109876543210"))
          )
        )
      ),
      skipSegments     = Some(Set(1, 3, 5)),
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.skipTokenRanges, config.skipTokenRanges)
    assertEquals(roundTripped.skipSegments, config.skipSegments)
  }
}
