package com.scylladb.migrator.config

import io.circe.{ parser, DecodingFailure }

import java.time.Instant

/** Parser-level contract tests for [[StreamChangesSetting]].
  *
  * These tests pin:
  *   - The legacy boolean encoding still decodes (backward compat guarantee to existing users).
  *   - The new object form with `type: kinesis` produces the expected sealed-trait case.
  *   - Invalid ARNs and unknown `type` tokens fail fast with actionable messages rather than
  *     leaking through to the runtime.
  */
class StreamChangesSettingParserTest extends munit.FunSuite {

  /** Parse a JSON document as a `StreamChangesSetting`, asserting that the parse itself succeeds
    * (but leaving decoder success/failure to the caller to inspect).
    */
  private def parseSetting(json: String): StreamChangesSetting =
    parser
      .parse(json)
      .flatMap(_.as[StreamChangesSetting]) match {
      case Right(setting) => setting
      case Left(err)      => fail(s"Failed to decode StreamChangesSetting. Got ${err}")
    }

  test("boolean `true` decodes as DynamoDBStreams (legacy YAML still works)") {
    assertEquals(parseSetting("true"), StreamChangesSetting.DynamoDBStreams)
  }

  test("boolean `false` decodes as Disabled") {
    assertEquals(parseSetting("false"), StreamChangesSetting.Disabled)
  }

  test("null decodes as Disabled (field absent is a snapshot-only migration)") {
    assertEquals(parseSetting("null"), StreamChangesSetting.Disabled)
  }

  test("object form `{type: disabled}` decodes as Disabled") {
    assertEquals(parseSetting("""{"type": "disabled"}"""), StreamChangesSetting.Disabled)
  }

  test("object form `{type: dynamodb-streams}` decodes as DynamoDBStreams") {
    assertEquals(
      parseSetting("""{"type": "dynamodb-streams"}"""),
      StreamChangesSetting.DynamoDBStreams
    )
  }

  test("object form `{type: kinesis, streamArn: <arn>}` decodes with no optional fields") {
    val setting = parseSetting(
      """{"type": "kinesis", "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"}"""
    )
    setting match {
      case StreamChangesSetting.KinesisDataStreams(arn, ts, appName) =>
        assertEquals(arn, "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream")
        assertEquals(ts, None)
        assertEquals(appName, None)
      case other =>
        fail(s"Expected KinesisDataStreams, got $other")
    }
  }

  test("aliased type `kinesis-data-streams` is also accepted") {
    // The alias exists because some docs / operators may spell it out in full; both should
    // decode identically.
    val setting = parseSetting(
      """{"type": "kinesis-data-streams", "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"}"""
    )
    assert(setting.isInstanceOf[StreamChangesSetting.KinesisDataStreams])
  }

  test("Kinesis form decodes ISO-8601 initialTimestamp and appName") {
    val setting = parseSetting(
      """
        |{
        |  "type": "kinesis",
        |  "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
        |  "initialTimestamp": "2024-01-01T00:00:00Z",
        |  "appName": "migrator-custom-app"
        |}
        |""".stripMargin
    )
    setting match {
      case StreamChangesSetting.KinesisDataStreams(arn, Some(ts), Some(app)) =>
        assertEquals(arn, "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream")
        assertEquals(ts, Instant.parse("2024-01-01T00:00:00Z"))
        assertEquals(app, "migrator-custom-app")
      case other =>
        fail(s"Expected KinesisDataStreams with all fields populated, got $other")
    }
  }

  test("bare stream name (without 'arn:aws:kinesis:...:stream/...' prefix) is rejected") {
    // Pins the contract documented in StreamChangesSetting.KinesisDataStreams.streamArn scaladoc:
    // DynamoDB's EnableKinesisStreamingDestination API requires a full ARN, and the migrator has
    // no account-id / region context to synthesize one from a bare name, so we fail fast at
    // decode time with an actionable error rather than deep inside the AWS SDK at runtime.
    val result = parser
      .parse("""{"type": "kinesis", "streamArn": "my-stream"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft, s"Expected Left, got $result")
    result.left.foreach { err =>
      assert(err.isInstanceOf[DecodingFailure])
      assert(
        err.getMessage.contains("must be a full Kinesis ARN"),
        s"Expected message about full Kinesis ARN, got: ${err.getMessage}"
      )
    }
  }

  test("ARN for a non-Kinesis AWS service is rejected") {
    // Protects against pasting the wrong ARN — e.g. an SNS topic ARN — which would succeed at
    // decode time but fail deep inside EnableKinesisStreamingDestination with a confusing
    // "stream not found" error.
    val result = parser
      .parse("""{"type": "kinesis", "streamArn": "arn:aws:sns:us-east-1:123:topic/my-topic"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft)
  }

  test("ARN with missing account id (substring-only match) is rejected (SEC-3)") {
    // Before the regex tightening, the decoder accepted `arn:aws:kinesis:`-prefixed strings as
    // long as they contained `:stream/`. That let through `arn:aws:kinesis::stream/foo` (empty
    // account) which then failed deep inside EnableKinesisStreamingDestination with a 403. Now
    // the regex requires exactly 12 digits in the account slot.
    val bad = parser
      .parse("""{"type": "kinesis", "streamArn": "arn:aws:kinesis::stream/s"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(bad.isLeft, s"ARN without an account id must be rejected; got $bad")
  }

  test("ARN with a 3-digit account id is rejected (must be exactly 12 digits) (SEC-3)") {
    val bad = parser
      .parse("""{"type": "kinesis", "streamArn": "arn:aws:kinesis:us-east-1:123:stream/s"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(bad.isLeft, s"Short account id must be rejected; got $bad")
  }

  test("ARN with a made-up partition 'aws2' is rejected (SEC-3)") {
    val bad = parser
      .parse(
        """{"type": "kinesis", "streamArn": "arn:aws2:kinesis:us-east-1:123456789012:stream/s"}"""
      )
      .flatMap(_.as[StreamChangesSetting])
    assert(bad.isLeft, s"Unknown partition must be rejected; got $bad")
  }

  test("ARN in aws-cn (China) partition is accepted (SEC-3)") {
    val ok = parser
      .parse(
        """{"type": "kinesis", "streamArn": "arn:aws-cn:kinesis:cn-north-1:123456789012:stream/s"}"""
      )
      .flatMap(_.as[StreamChangesSetting])
    assert(ok.isRight, s"aws-cn partition must be accepted; got $ok")
  }

  test("ARN in aws-us-gov (GovCloud) partition is accepted (SEC-3)") {
    val ok = parser
      .parse(
        """{"type": "kinesis", "streamArn": "arn:aws-us-gov:kinesis:us-gov-east-1:123456789012:stream/s"}"""
      )
      .flatMap(_.as[StreamChangesSetting])
    assert(ok.isRight, s"aws-us-gov partition must be accepted; got $ok")
  }

  test("ARN with whitespace in the stream name is rejected (SEC-3)") {
    // Paste-error: "... :stream/my stream" — the regex disallows whitespace in the tail.
    val bad = parser
      .parse(
        """{"type": "kinesis", "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my stream"}"""
      )
      .flatMap(_.as[StreamChangesSetting])
    assert(bad.isLeft, s"Stream name with whitespace must be rejected; got $bad")
  }

  test("unknown `type` string is rejected with a helpful list of valid values") {
    val result = parser
      .parse("""{"type": "kafka", "streamArn": "whatever"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft)
    result.left.foreach { err =>
      val msg = err.getMessage
      assert(msg.contains("kafka"), s"error should mention the bad type; got: $msg")
      assert(msg.contains("kinesis"), s"error should list the valid 'kinesis' type; got: $msg")
    }
  }

  test("scalar non-boolean values are rejected") {
    // A naive user might write `streamChanges: "yes"` (a string) or `streamChanges: 1`. The
    // decoder must refuse rather than interpreting them.
    val badString = parser.parse(""""yes"""").flatMap(_.as[StreamChangesSetting])
    assert(badString.isLeft)
    val badNumber = parser.parse("1").flatMap(_.as[StreamChangesSetting])
    assert(badNumber.isLeft)
  }

  test("Kinesis form missing streamArn is rejected") {
    val result = parser
      .parse("""{"type": "kinesis"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft)
  }

  test("round-trip: encoder for legacy cases emits boolean JSON (not object)") {
    // Preserves round-trip shape for pre-existing YAML; switching to the object form on output
    // would surprise diff-happy operators who regenerate configs from the migrator.
    import io.circe.syntax._
    assertEquals(
      (StreamChangesSetting.Disabled: StreamChangesSetting).asJson.noSpaces,
      "false"
    )
    assertEquals(
      (StreamChangesSetting.DynamoDBStreams: StreamChangesSetting).asJson.noSpaces,
      "true"
    )
  }

  test("round-trip: encoder for Kinesis emits the object form with type='kinesis'") {
    import io.circe.syntax._
    val setting: StreamChangesSetting = StreamChangesSetting.KinesisDataStreams(
      streamArn = "arn:aws:kinesis:us-east-1:123456789012:stream/s",
      appName   = Some("my-app")
    )
    val json = setting.asJson.noSpaces
    assert(json.contains(""""type":"kinesis""""))
    assert(json.contains(""""streamArn":"arn:aws:kinesis:us-east-1:123456789012:stream/s""""))
    assert(json.contains(""""appName":"my-app""""))
    // No initialTimestamp since it was None — verify the encoder doesn't emit it as `null`.
    assert(!json.contains("initialTimestamp"), s"did not expect initialTimestamp in: $json")
  }

  // -------- LOGIC-4: future-timestamp validation ------------------------------------------

  test("initialTimestamp more than 5 minutes in the future is rejected (LOGIC-4)") {
    // A timestamp 10 years from now is almost certainly a typo (2026 -> 2035). Accepting it
    // would park the KCL consumer on AT_TIMESTAMP until 2035 and no records would flow.
    val farFuture = Instant.now().plusSeconds(60L * 60L * 24L * 365L * 10L) // 10 years
    val json = s"""
                  |{
                  |  "type": "kinesis",
                  |  "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
                  |  "initialTimestamp": "${farFuture.toString}"
                  |}
                  |""".stripMargin

    val result = parser.parse(json).flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft, s"10-years-future initialTimestamp must be rejected; got $result")
    result.left.foreach { err =>
      val msg = err.getMessage
      assert(
        msg.contains("in the future"),
        s"error should say 'in the future'; got: $msg"
      )
    }
  }

  test("initialTimestamp 2 minutes in the future is accepted (within clock-skew tolerance)") {
    // NTP-skewed drivers often tick a minute or two ahead of AWS. The decoder must tolerate
    // ordinary skew up to 5 minutes so operators whose clocks are slightly ahead don't get
    // their configs rejected for nothing.
    val closeFuture = Instant.now().plusSeconds(2 * 60)
    val json = s"""
                  |{
                  |  "type": "kinesis",
                  |  "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
                  |  "initialTimestamp": "${closeFuture.toString}"
                  |}
                  |""".stripMargin

    val result = parser.parse(json).flatMap(_.as[StreamChangesSetting])
    assert(result.isRight, s"2-minutes-future initialTimestamp must be accepted; got $result")
  }

  test("initialTimestamp far in the past is accepted (stream retention is AWS-checked, not here)") {
    // The decoder only refuses absurd FUTURE timestamps — past timestamps beyond stream
    // retention are best rejected by AWS at runtime because only AWS knows the retention.
    val farPast = Instant.parse("2020-01-01T00:00:00Z")
    val json = s"""
                  |{
                  |  "type": "kinesis",
                  |  "streamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
                  |  "initialTimestamp": "${farPast.toString}"
                  |}
                  |""".stripMargin

    val result = parser.parse(json).flatMap(_.as[StreamChangesSetting])
    assert(result.isRight, s"Far-past initialTimestamp must decode OK; got $result")
  }

  // -------- LOGIC-8 support: arnRegion helper ---------------------------------------------

  test("KinesisDataStreams.arnRegion extracts the region for cross-check (LOGIC-8)") {
    // The orchestrator (AlternatorMigrator) cross-checks this against source.region so a
    // mismatch surfaces as an actionable startup error, not an empty stream at runtime.
    val kds = StreamChangesSetting.KinesisDataStreams(
      streamArn = "arn:aws:kinesis:eu-west-3:123456789012:stream/my-stream"
    )
    assertEquals(kds.arnRegion, "eu-west-3")
  }

  test("KinesisDataStreams.arnRegion works for aws-cn ARNs (LOGIC-8)") {
    val kds = StreamChangesSetting.KinesisDataStreams(
      streamArn = "arn:aws-cn:kinesis:cn-north-1:123456789012:stream/my-stream"
    )
    assertEquals(kds.arnRegion, "cn-north-1")
  }
}
