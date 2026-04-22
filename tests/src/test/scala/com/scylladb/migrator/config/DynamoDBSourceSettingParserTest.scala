package com.scylladb.migrator.config

import com.scylladb.migrator.config.SourceSettings.DynamoDBS3Export.{
  AttributeDefinition,
  AttributeType,
  KeySchema,
  KeyType
}
import io.circe.{ yaml, DecodingFailure }

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

  private def parseSourceSettings(yamlContent: String): SourceSettings.DynamoDBS3Export =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                                    => throw error
      case Right(source: SourceSettings.DynamoDBS3Export) => source
      case Right(other) => fail(s"Failed to parse source settings. Got ${other}.")
    }

}
