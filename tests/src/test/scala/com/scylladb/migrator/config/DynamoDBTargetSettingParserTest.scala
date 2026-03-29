package com.scylladb.migrator.config

import io.circe.yaml
import io.circe.syntax._
import software.amazon.awssdk.services.dynamodb.model.BillingMode

class DynamoDBTargetSettingParserTest extends munit.FunSuite {

  test("skipInitialSnapshotTransfer is optional") {
    val config =
      """type: dynamodb
        |table: Dummy
        |writeThroughput: 1
        |throughputWritePercent: 1.0
        |streamChanges: false
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.skipInitialSnapshotTransfer, None)
  }

  test("explicit skipInitialSnapshotTransfer is taken into account") {
    val config =
      """type: dynamodb
        |table: Dummy
        |writeThroughput: 1
        |throughputWritePercent: 1.0
        |streamChanges: false
        |skipInitialSnapshotTransfer: true
        |""".stripMargin

    val parsedSettings = parseDynamoDBTargetSettings(config)
    assertEquals(parsedSettings.skipInitialSnapshotTransfer, Some(true))
  }

  test("'dynamo' type alias parses as DynamoDB target") {
    val config =
      """type: dynamo
        |table: Dummy
        |streamChanges: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(_: TargetSettings.DynamoDB) => () // OK
      case other =>
        fail(s"Expected TargetSettings.DynamoDB, got: ${other}")
    }
  }

  test("dynamodb target with nested alternator key fails fast") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("nested 'alternator' key")),
      s"Expected error message about nested alternator key, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with nested alternator block fails fast") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("does not use a nested 'alternator' block")),
      s"Expected error about nested alternator block, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("valid alternator target config") {
    val config =
      """type: alternator
        |table: MyTable
        |writeThroughput: 200
        |throughputWritePercent: 0.8
        |streamChanges: true
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |datacenter: dc1
        |rack: rack1
        |activeRefreshIntervalMs: 5000
        |idleRefreshIntervalMs: 30000
        |compression: true
        |optimizeHeaders: true
        |maxConnections: 50
        |connectionMaxIdleTimeMs: 60000
        |connectionTimeToLiveMs: 120000
        |connectionAcquisitionTimeoutMs: 10000
        |connectionTimeoutMs: 5000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(alt: TargetSettings.Alternator) =>
        assertEquals(alt.table, "MyTable")
        assertEquals(alt.writeThroughput, Some(200))
        assertEquals(alt.throughputWritePercent, Some(0.8f))
        assertEquals(alt.streamChanges, true)
        assertEquals(alt.endpoint, Some(DynamoDBEndpoint("http://10.0.0.1", 8000)))
        assertEquals(alt.alternatorConfig.datacenter, Some("dc1"))
        assertEquals(alt.alternatorConfig.rack, Some("rack1"))
        assertEquals(alt.alternatorConfig.activeRefreshIntervalMs, Some(5000L))
        assertEquals(alt.alternatorConfig.idleRefreshIntervalMs, Some(30000L))
        assertEquals(alt.alternatorConfig.compression, Some(true))
        assertEquals(alt.alternatorConfig.optimizeHeaders, Some(true))
        assertEquals(alt.alternatorConfig.maxConnections, Some(50))
        assertEquals(alt.alternatorConfig.connectionMaxIdleTimeMs, Some(60000L))
        assertEquals(alt.alternatorConfig.connectionTimeToLiveMs, Some(120000L))
        assertEquals(alt.alternatorConfig.connectionAcquisitionTimeoutMs, Some(10000L))
        assertEquals(alt.alternatorConfig.connectionTimeoutMs, Some(5000L))
      case other =>
        fail(s"Expected TargetSettings.Alternator, got: ${other}")
    }
  }

  test("alternator target without endpoint fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("requires an 'endpoint'")),
      s"Expected error about missing endpoint, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target roundtrip encode then decode") {
    val original = TargetSettings.Alternator(
      alternatorEndpoint          = DynamoDBEndpoint("http://10.0.0.1", 8000),
      region                      = Some("us-east-1"),
      credentials                 = Some(AWSCredentials("myKey", "mySecret", None)),
      table                       = "RoundtripTable",
      writeThroughput             = Some(200),
      throughputWritePercent      = Some(0.8f),
      streamChanges               = true,
      skipInitialSnapshotTransfer = Some(false),
      removeConsumedCapacity      = false,
      billingMode                 = Some(BillingMode.PROVISIONED),
      alternatorConfig = AlternatorSettings(
        datacenter                     = Some("dc1"),
        rack                           = Some("rack1"),
        activeRefreshIntervalMs        = Some(5000L),
        idleRefreshIntervalMs          = Some(30000L),
        compression                    = Some(true),
        optimizeHeaders                = Some(false),
        maxConnections                 = Some(50),
        connectionMaxIdleTimeMs        = Some(60000L),
        connectionTimeToLiveMs         = Some(120000L),
        connectionAcquisitionTimeoutMs = Some(10000L),
        connectionTimeoutMs            = Some(5000L)
      )
    )

    val json = (original: TargetSettings).asJson
    val decoded = json.as[TargetSettings]

    decoded match {
      case Right(roundtripped: TargetSettings.Alternator) =>
        assertEquals(roundtripped, original)
      case other =>
        fail(s"Roundtrip failed. Got: ${other}")
    }
  }

  test("dynamodb target roundtrip encode then decode") {
    val original = TargetSettings.DynamoDB(
      endpoint                    = Some(DynamoDBEndpoint("localhost", 8000)),
      region                      = Some("us-east-1"),
      credentials                 = None,
      table                       = "TestTable",
      writeThroughput             = Some(200),
      throughputWritePercent      = Some(0.8f),
      streamChanges               = false,
      skipInitialSnapshotTransfer = None,
      billingMode                 = Some(BillingMode.PAY_PER_REQUEST)
    )
    val json = (original: TargetSettings).asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }

  test("alternator target with rack but no datacenter fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("datacenter")),
      s"Expected error about missing datacenter, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with negative connectionTimeoutMs fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |connectionTimeoutMs: -1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("connectionTimeoutMs")),
      s"Expected error about connectionTimeoutMs, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with removeConsumedCapacity fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |removeConsumedCapacity: true
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("removeConsumedCapacity")),
      s"Expected error about removeConsumedCapacity, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with assumeRole fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |credentials:
        |  accessKey: foo
        |  secretKey: bar
        |  assumeRole:
        |    arn: arn:aws:iam::123456789012:role/MyRole
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("assumeRole")),
      s"Expected error about assumeRole, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with multiple validation errors reports all") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |rack: rack1
        |maxConnections: -5
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(msg.contains("datacenter"), s"Expected error about datacenter, got: $msg")
    assert(msg.contains("maxConnections"), s"Expected error about maxConnections, got: $msg")
  }

  test("alternator target without endpoint and with invalid settings reports all errors") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(msg.contains("endpoint"), s"Expected error about endpoint, got: $msg")
    assert(msg.contains("datacenter"), s"Expected error about datacenter, got: $msg")
  }

  test("alternator target defaults removeConsumedCapacity to true") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(alt: TargetSettings.Alternator) =>
        assertEquals(alt.removeConsumedCapacity, true)
      case other =>
        fail(s"Expected TargetSettings.Alternator, got: ${other}")
    }
  }

  test("dynamodb target with Alternator-only fields fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |datacenter: dc1
        |compression: true
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(
      msg.contains("Alternator-only fields"),
      s"Expected error about Alternator-only fields, got: $msg"
    )
    assert(msg.contains("datacenter"), s"Expected 'datacenter' in error, got: $msg")
    assert(msg.contains("compression"), s"Expected 'compression' in error, got: $msg")
  }

  test("dynamodb target with single Alternator-only field fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("rack")),
      s"Expected error mentioning 'rack', got: ${result.left.map(_.getMessage)}"
    )
  }

  test("'dynamo' alias with Alternator-only fields fails") {
    val config =
      """type: dynamo
        |table: Dummy
        |streamChanges: false
        |datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("Alternator-only fields")),
      s"Expected error about Alternator-only fields, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target minimal roundtrip (all optionals None)") {
    val original = TargetSettings.DynamoDB(
      endpoint                    = None,
      region                      = None,
      credentials                 = None,
      table                       = "MinimalTable",
      writeThroughput             = None,
      throughputWritePercent      = None,
      streamChanges               = false,
      skipInitialSnapshotTransfer = None
    )
    val json = (original: TargetSettings).asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }

  test("alternator target minimal roundtrip (all optionals None)") {
    val original = TargetSettings.Alternator(
      alternatorEndpoint          = DynamoDBEndpoint("http://10.0.0.1", 8000),
      region                      = None,
      credentials                 = None,
      table                       = "MinimalTable",
      writeThroughput             = None,
      throughputWritePercent      = None,
      streamChanges               = false,
      skipInitialSnapshotTransfer = None,
      removeConsumedCapacity      = true,
      billingMode                 = None,
      alternatorConfig            = AlternatorSettings()
    )
    val json = (original: TargetSettings).asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }

  test("AlternatorSettings field names do not overlap with base Alternator target field names") {
    // Derive field names from the case class to stay in sync automatically.
    // "alternatorConfig" and "alternatorEndpoint" are internal fields (not in YAML),
    // replaced with "endpoint" and "type" (synthetic YAML fields).
    val baseTargetKeys = TargetSettings
      .Alternator(
        alternatorEndpoint          = DynamoDBEndpoint("http://placeholder", 0),
        region                      = None,
        credentials                 = None,
        table                       = "",
        writeThroughput             = None,
        throughputWritePercent      = None,
        streamChanges               = false,
        skipInitialSnapshotTransfer = None
      )
      .productElementNames
      .toSet - "alternatorConfig" - "alternatorEndpoint" + "endpoint" + "type"
    val overlap = AlternatorSettings.fieldNames.intersect(baseTargetKeys)
    assert(
      overlap.isEmpty,
      s"AlternatorSettings field names must not overlap with base Alternator target field names, but found: ${overlap.mkString(", ")}"
    )
  }

  test("alternator target with endpoint missing protocol prefix fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: 10.0.0.1
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("protocol prefix")),
      s"Expected error about protocol prefix, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with protocol-prefix endpoint parses") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |endpoint:
        |  host: http://scylla
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(d: TargetSettings.DynamoDB) =>
        assertEquals(d.endpoint.map(_.host), Some("http://scylla"))
      case other =>
        fail(s"Expected TargetSettings.DynamoDB, got: ${other}")
    }
  }

  test("dynamodb target with zero writeThroughput fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |streamChanges: false
        |writeThroughput: 0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("writeThroughput")),
      s"Expected error about writeThroughput, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with empty table fails") {
    val config =
      """type: dynamodb
        |table: ""
        |streamChanges: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("'table'")),
      s"Expected error about 'table', got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with whitespace-only table fails") {
    val config =
      """type: dynamodb
        |table: "   "
        |streamChanges: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("'table'")),
      s"Expected error about 'table', got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with negative throughputWritePercent fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: -0.5
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputWritePercent")),
      s"Expected error about throughputWritePercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with explicit removeConsumedCapacity false") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |removeConsumedCapacity: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(alt: TargetSettings.Alternator) =>
        assertEquals(alt.removeConsumedCapacity, false)
      case other =>
        fail(s"Expected TargetSettings.Alternator, got: ${other}")
    }
  }

  test("alternator target with throughputWritePercent above 1.5 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: 2.0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputWritePercent")),
      s"Expected error about throughputWritePercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with bare hostname endpoint succeeds") {
    val config =
      """type: dynamodb
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: my-dynamodb-host
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(d: TargetSettings.DynamoDB) =>
        assertEquals(d.endpoint, Some(DynamoDBEndpoint("my-dynamodb-host", 8000)))
      case other =>
        fail(s"Expected TargetSettings.DynamoDB, got: ${other}")
    }
  }

  test("unknown target type fails") {
    val config =
      """type: unknown
        |table: Dummy
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("Invalid target type")),
      s"Expected error about invalid target type, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb target with missing streamChanges fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator target with https endpoint parses successfully") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: https://my-alternator
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    result match {
      case Right(alt: TargetSettings.Alternator) =>
        assertEquals(alt.endpoint, Some(DynamoDBEndpoint("https://my-alternator", 8000)))
      case other =>
        fail(s"Expected TargetSettings.Alternator, got: ${other}")
    }
  }

  test("alternator target with https endpoint roundtrip") {
    val original = TargetSettings.Alternator(
      alternatorEndpoint          = DynamoDBEndpoint("https://my-alternator", 8000),
      region                      = None,
      credentials                 = None,
      table                       = "HttpsTable",
      writeThroughput             = None,
      throughputWritePercent      = None,
      streamChanges               = false,
      skipInitialSnapshotTransfer = None,
      removeConsumedCapacity      = true,
      billingMode                 = None,
      alternatorConfig            = AlternatorSettings()
    )
    val json = (original: TargetSettings).asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }

  test("target throughputWritePercent boundary 0.1 passes") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: 0.1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isRight, s"Expected success but got: ${result}")
  }

  test("target throughputWritePercent boundary 1.5 passes") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: 1.5
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isRight, s"Expected success but got: ${result}")
  }

  test("target throughputWritePercent boundary 0.09 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: 0.09
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputWritePercent")),
      s"Expected error about throughputWritePercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("target throughputWritePercent boundary 1.51 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |streamChanges: false
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputWritePercent: 1.51
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[TargetSettings])

    assert(result.isLeft, s"Expected failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputWritePercent")),
      s"Expected error about throughputWritePercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  private def parseDynamoDBTargetSettings(yamlContent: String): TargetSettings.DynamoDB =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[TargetSettings]) match {
      case Right(dynamoDB: TargetSettings.DynamoDB) => dynamoDB
      case other => fail(s"Failed to parse type TargetSettings.DynamoDB. Got ${other}.")
    }

}
