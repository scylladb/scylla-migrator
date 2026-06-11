package com.scylladb.migrator.config

import com.scylladb.migrator.config.SourceSettings.DynamoDBS3Export.{
  AttributeDefinition,
  AttributeType,
  KeySchema,
  KeyType
}
import io.circe.{ yaml, DecodingFailure }
import io.circe.syntax._

class DynamoDBSourceSettingParserTest extends munit.FunSuite {

  test("valid dynamodb-s3-export config") {
    val config =
      """type: dynamodb-s3-export
        |bucket: foobar
        |manifestKey: my-export/AWSDynamoDB/01715094384115-f0e55399/manifest-summary.json
        |tableDescription:
        |  attributeDefinitions:
        |    - name: id
        |      type: S
        |    - name: foo
        |      type: N
        |    - name: bar
        |      type: B
        |  keySchema:
        |    - name: id
        |      type: HASH
        |    - name: foo
        |      type: RANGE
        |""".stripMargin

    val expectedSettings = SourceSettings.DynamoDBS3Export(
      bucket      = "foobar",
      manifestKey = "my-export/AWSDynamoDB/01715094384115-f0e55399/manifest-summary.json",
      tableDescription = SourceSettings.DynamoDBS3Export.TableDescription(
        attributeDefinitions = Seq(
          AttributeDefinition("id", AttributeType.S),
          AttributeDefinition("foo", AttributeType.N),
          AttributeDefinition("bar", AttributeType.B)
        ),
        keySchema = Seq(
          KeySchema("id", KeyType.Hash),
          KeySchema("foo", KeyType.Range)
        )
      ),
      endpoint           = None,
      region             = None,
      credentials        = None,
      usePathStyleAccess = None
    )

    val parsedSettings = parseSourceSettings(config)
    assertEquals(parsedSettings, expectedSettings)
  }

  test("invalid dynamodb-s3-export config") {
    val config =
      """type: dynamodb-s3-export
        |bucket: foobar
        |manifestKey: my-export/AWSDynamoDB/01715094384115-f0e55399/manifest-summary.json
        |tableDescription:
        |  attributeDefinitions:
        |    - name: id
        |      type: number
        |  keySchema:
        |    - name: id
        |      type: HASH
        |""".stripMargin

    interceptMessage[DecodingFailure](
      "DecodingFailure at .tableDescription.attributeDefinitions[0].type: Unknown attribute type number"
    ) {
      parseSourceSettings(config)
    }
  }

  test("dynamodb-s3-export config with streamSource block (ARCH-1)") {
    // This pins the wire-format for chained S3-export + streamChanges. The nested block must
    // decode into a `SourceSettings.DynamoDB` whose fields are picked up (table/region) — the
    // orchestrator uses those when enabling the streaming destination on the still-live table.
    val config =
      """type: dynamodb-s3-export
        |bucket: foobar
        |manifestKey: AWSDynamoDB/01715094384115-f0e55399/manifest-summary.json
        |tableDescription:
        |  attributeDefinitions:
        |    - name: id
        |      type: S
        |  keySchema:
        |    - name: id
        |      type: HASH
        |streamSource:
        |  table: MyLiveTable
        |  region: us-east-1
        |""".stripMargin

    val parsed = parseSourceSettings(config)
    assert(
      parsed.streamSource.isDefined,
      s"streamSource must round-trip out of YAML; got ${parsed.streamSource}"
    )
    val live = parsed.streamSource.get
    assertEquals(live.table, "MyLiveTable")
    assertEquals(live.region, Some("us-east-1"))
    // Fields not specified in YAML default to None — sanity-check one so the decoder isn't
    // silently reusing state from the outer S3-export block.
    assertEquals(live.credentials, None)
    assertEquals(live.endpoint, None)
  }

  test("dynamodb-s3-export without streamSource keeps streamSource = None (default)") {
    // Backward-compat pin: existing YAML files that do not mention streamSource must continue
    // decoding unchanged.
    val config =
      """type: dynamodb-s3-export
        |bucket: foobar
        |manifestKey: AWSDynamoDB/01715094384115-f0e55399/manifest-summary.json
        |tableDescription:
        |  attributeDefinitions:
        |    - name: id
        |      type: S
        |  keySchema:
        |    - name: id
        |      type: HASH
        |""".stripMargin

    val parsed = parseSourceSettings(config)
    assertEquals(parsed.streamSource, None)
  }

  test("'dynamo' type alias parses as DynamoDB source") {
    val config =
      """type: dynamo
        |table: Dummy
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(_: SourceSettings.DynamoDB) => () // OK
      case other =>
        fail(s"Expected SourceSettings.DynamoDB, got: ${other}")
    }
  }

  test("dynamodb source with nested alternator key fails fast") {
    val config =
      """type: dynamodb
        |table: Dummy
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("nested 'alternator' key")),
      s"Expected error message about nested alternator key, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with nested alternator block fails fast") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |alternator:
        |  datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("does not use a nested 'alternator' block")),
      s"Expected error about nested alternator block, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("valid alternator source config") {
    val config =
      """type: alternator
        |table: MyTable
        |scanSegments: 4
        |readThroughput: 100
        |throughputReadPercent: 0.5
        |maxMapTasks: 2
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
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(alt: SourceSettings.Alternator) =>
        assertEquals(alt.table, "MyTable")
        assertEquals(alt.scanSegments, Some(4))
        assertEquals(alt.readThroughput, Some(100))
        assertEquals(alt.throughputReadPercent, Some(0.5f))
        assertEquals(alt.maxMapTasks, Some(2))
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
        fail(s"Expected SourceSettings.Alternator, got: ${other}")
    }
  }

  test("alternator source without endpoint fails") {
    val config =
      """type: alternator
        |table: MyTable
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("requires an 'endpoint'")),
      s"Expected error about missing endpoint, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source roundtrip encode then decode") {
    val original = SourceSettings.Alternator(
      alternatorEndpoint     = DynamoDBEndpoint("http://10.0.0.1", 8000),
      region                 = Some("us-east-1"),
      credentials            = Some(AWSCredentials("myKey", "mySecret", None)),
      table                  = "RoundtripTable",
      scanSegments           = Some(4),
      readThroughput         = Some(100),
      throughputReadPercent  = Some(0.5f),
      maxMapTasks            = Some(2),
      removeConsumedCapacity = false,
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

    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]

    decoded match {
      case Right(roundtripped: SourceSettings.Alternator) =>
        assertEquals(roundtripped, original)
      case other =>
        fail(s"Roundtrip failed. Got: ${other}")
    }
  }

  test("dynamodb source roundtrip encode then decode") {
    val original = SourceSettings.DynamoDB(
      endpoint              = Some(DynamoDBEndpoint("localhost", 8000)),
      region                = Some("us-east-1"),
      credentials           = None,
      table                 = "TestTable",
      scanSegments          = Some(4),
      readThroughput        = Some(100),
      throughputReadPercent = Some(0.5f),
      maxMapTasks           = Some(2)
    )
    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]
    assertEquals(decoded, Right(original))
  }

  test("alternator source with rack but no datacenter fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("datacenter")),
      s"Expected error about missing datacenter, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with zero maxConnections fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |maxConnections: 0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("maxConnections")),
      s"Expected error about maxConnections, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source with removeConsumedCapacity fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |removeConsumedCapacity: true
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("removeConsumedCapacity")),
      s"Expected error about removeConsumedCapacity, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with assumeRole fails") {
    val config =
      """type: alternator
        |table: MyTable
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
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("assumeRole")),
      s"Expected error about assumeRole, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with multiple validation errors reports all") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |rack: rack1
        |maxConnections: 0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(msg.contains("datacenter"), s"Expected error about datacenter, got: $msg")
    assert(msg.contains("maxConnections"), s"Expected error about maxConnections, got: $msg")
  }

  test("alternator source without endpoint and with invalid settings reports all errors") {
    val config =
      """type: alternator
        |table: MyTable
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(msg.contains("endpoint"), s"Expected error about endpoint, got: $msg")
    assert(msg.contains("datacenter"), s"Expected error about datacenter, got: $msg")
  }

  test("alternator source defaults removeConsumedCapacity to true") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(alt: SourceSettings.Alternator) =>
        assertEquals(alt.removeConsumedCapacity, true)
      case other =>
        fail(s"Expected SourceSettings.Alternator, got: ${other}")
    }
  }

  test("dynamodb source with Alternator-only fields fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |datacenter: dc1
        |compression: true
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    val msg = result.swap.map(_.getMessage).getOrElse("")
    assert(
      msg.contains("Alternator-only fields"),
      s"Expected error about Alternator-only fields, got: $msg"
    )
    assert(msg.contains("datacenter"), s"Expected 'datacenter' in error, got: $msg")
    assert(msg.contains("compression"), s"Expected 'compression' in error, got: $msg")
  }

  test("dynamodb source with single Alternator-only field fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |rack: rack1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

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
        |datacenter: dc1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("Alternator-only fields")),
      s"Expected error about Alternator-only fields, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source minimal roundtrip (all optionals None)") {
    val original = SourceSettings.DynamoDB(
      endpoint              = None,
      region                = None,
      credentials           = None,
      table                 = "MinimalTable",
      scanSegments          = None,
      readThroughput        = None,
      throughputReadPercent = None,
      maxMapTasks           = None
    )
    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]
    assertEquals(decoded, Right(original))
  }

  test("alternator source minimal roundtrip (all optionals None)") {
    val original = SourceSettings.Alternator(
      alternatorEndpoint     = DynamoDBEndpoint("http://10.0.0.1", 8000),
      region                 = None,
      credentials            = None,
      table                  = "MinimalTable",
      scanSegments           = None,
      readThroughput         = None,
      throughputReadPercent  = None,
      maxMapTasks            = None,
      removeConsumedCapacity = true,
      alternatorConfig       = AlternatorSettings()
    )
    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]
    assertEquals(decoded, Right(original))
  }

  test("AlternatorSettings field names do not overlap with base Alternator source field names") {
    // Derive field names from the case class to stay in sync automatically.
    // "alternatorConfig" and "alternatorEndpoint" are internal fields (not in YAML),
    // replaced with "endpoint" and "type" (synthetic YAML fields).
    val baseSourceKeys = SourceSettings
      .Alternator(
        alternatorEndpoint    = DynamoDBEndpoint("http://placeholder", 0),
        region                = None,
        credentials           = None,
        table                 = "",
        scanSegments          = None,
        readThroughput        = None,
        throughputReadPercent = None,
        maxMapTasks           = None
      )
      .productElementNames
      .toSet - "alternatorConfig" - "alternatorEndpoint" + "endpoint" + "type"
    val overlap = AlternatorSettings.fieldNames.intersect(baseSourceKeys)
    assert(
      overlap.isEmpty,
      s"AlternatorSettings field names must not overlap with base Alternator source field names, but found: ${overlap.mkString(", ")}"
    )
  }

  test("AlternatorSettings Hadoop config round-trip") {
    val original = AlternatorSettings(
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

    val jobConf = new org.apache.hadoop.mapred.JobConf()
    com.scylladb.migrator.DynamoUtils.writeAlternatorSettingsToConf(jobConf, original)
    val roundTripped =
      com.scylladb.migrator.DynamoUtils.readAlternatorSettingsFromConf(jobConf)

    assertEquals(roundTripped, original)
  }

  test("AlternatorSettings Hadoop config round-trip with all fields empty") {
    val original = AlternatorSettings()

    val jobConf = new org.apache.hadoop.mapred.JobConf()
    com.scylladb.migrator.DynamoUtils.writeAlternatorSettingsToConf(jobConf, original)
    val roundTripped =
      com.scylladb.migrator.DynamoUtils.readAlternatorSettingsFromConf(jobConf)

    assertEquals(roundTripped, original)
  }

  test("alternator source with endpoint missing protocol prefix fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: 10.0.0.1
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("protocol prefix")),
      s"Expected error about protocol prefix, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source with protocol-prefix endpoint parses") {
    val config =
      """type: dynamodb
        |table: Dummy
        |endpoint:
        |  host: http://scylla
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(d: SourceSettings.DynamoDB) =>
        assertEquals(d.endpoint.map(_.host), Some("http://scylla"))
      case other =>
        fail(s"Expected SourceSettings.DynamoDB, got: ${other}")
    }
  }

  test("dynamodb source with zero scanSegments fails") {
    val config =
      """type: dynamodb
        |table: Dummy
        |scanSegments: 0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("scanSegments")),
      s"Expected error about scanSegments, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source with empty table fails") {
    val config =
      """type: dynamodb
        |table: ""
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("'table'")),
      s"Expected error about 'table', got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source with whitespace-only table fails") {
    val config =
      """type: dynamodb
        |table: "   "
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("'table'")),
      s"Expected error about 'table', got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with negative readThroughput fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |readThroughput: -1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("readThroughput")),
      s"Expected error about readThroughput, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with explicit removeConsumedCapacity false") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |removeConsumedCapacity: false
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(alt: SourceSettings.Alternator) =>
        assertEquals(alt.removeConsumedCapacity, false)
      case other =>
        fail(s"Expected SourceSettings.Alternator, got: ${other}")
    }
  }

  test("alternator source with throughputReadPercent above 1.5 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputReadPercent: 2.0
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputReadPercent")),
      s"Expected error about throughputReadPercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source with bare hostname endpoint succeeds") {
    val config =
      """type: dynamodb
        |table: MyTable
        |endpoint:
        |  host: my-dynamodb-host
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(d: SourceSettings.DynamoDB) =>
        assertEquals(d.endpoint, Some(DynamoDBEndpoint("my-dynamodb-host", 8000)))
      case other =>
        fail(s"Expected SourceSettings.DynamoDB, got: ${other}")
    }
  }

  test("unknown source type fails") {
    val config =
      """type: unknown
        |table: Dummy
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("Unknown source type")),
      s"Expected error about unknown source type, got: ${result.left.map(_.getMessage)}"
    )
  }

  private def parseSourceSettings(yamlContent: String): SourceSettings.DynamoDBS3Export =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                                    => throw error
      case Right(source: SourceSettings.DynamoDBS3Export) => source
      case Right(other) => fail(s"Failed to parse source settings. Got ${other}.")
    }

  test("alternator source with https endpoint parses successfully") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: https://my-alternator
        |  port: 8000
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    result match {
      case Right(alt: SourceSettings.Alternator) =>
        assertEquals(alt.endpoint, Some(DynamoDBEndpoint("https://my-alternator", 8000)))
      case other =>
        fail(s"Expected SourceSettings.Alternator, got: ${other}")
    }
  }

  test("alternator source with https endpoint roundtrip") {
    val original = SourceSettings.Alternator(
      alternatorEndpoint     = DynamoDBEndpoint("https://my-alternator", 8000),
      region                 = None,
      credentials            = None,
      table                  = "HttpsTable",
      scanSegments           = None,
      readThroughput         = None,
      throughputReadPercent  = None,
      maxMapTasks            = None,
      removeConsumedCapacity = true,
      alternatorConfig       = AlternatorSettings()
    )
    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]
    assertEquals(decoded, Right(original))
  }

  test("source throughputReadPercent boundary 0.1 passes") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputReadPercent: 0.1
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isRight, s"Expected success but got: ${result}")
  }

  test("source throughputReadPercent boundary 1.5 passes") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputReadPercent: 1.5
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isRight, s"Expected success but got: ${result}")
  }

  test("source throughputReadPercent boundary 0.09 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputReadPercent: 0.09
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputReadPercent")),
      s"Expected error about throughputReadPercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("source throughputReadPercent boundary 1.51 fails") {
    val config =
      """type: alternator
        |table: MyTable
        |endpoint:
        |  host: http://10.0.0.1
        |  port: 8000
        |throughputReadPercent: 1.51
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])

    assert(result.isLeft, s"Expected failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("throughputReadPercent")),
      s"Expected error about throughputReadPercent, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb-s3-export source roundtrip with all optional fields") {
    val original = SourceSettings.DynamoDBS3Export(
      bucket      = "my-bucket",
      manifestKey = "exports/manifest.json",
      tableDescription = SourceSettings.DynamoDBS3Export.TableDescription(
        attributeDefinitions = Seq(
          AttributeDefinition("id", AttributeType.S),
          AttributeDefinition("sort", AttributeType.N)
        ),
        keySchema = Seq(
          KeySchema("id", KeyType.Hash),
          KeySchema("sort", KeyType.Range)
        ),
        billingMode = Some(software.amazon.awssdk.services.dynamodb.model.BillingMode.PROVISIONED),
        provisionedThroughput = Some(
          SourceSettings.DynamoDBS3Export.ProvisionedThroughputConfig(
            readCapacityUnits  = 100L,
            writeCapacityUnits = 50L
          )
        )
      ),
      endpoint           = Some(DynamoDBEndpoint("localhost", 8000)),
      region             = Some("us-west-2"),
      credentials        = Some(AWSCredentials("key", "secret", None)),
      usePathStyleAccess = Some(true)
    )

    val json = (original: SourceSettings).asJson
    val decoded = json.as[SourceSettings]

    decoded match {
      case Right(roundtripped: SourceSettings.DynamoDBS3Export) =>
        assertEquals(roundtripped, original)
      case other =>
        fail(s"Roundtrip failed. Got: ${other}")
    }
  }

}
