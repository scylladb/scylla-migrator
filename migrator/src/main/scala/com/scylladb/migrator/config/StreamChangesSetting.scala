package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, HCursor, Json }
import io.circe.syntax._

import java.time.{ Duration => JDuration, Instant }

/** How, if at all, the migrator should consume change events after the initial snapshot.
  *
  * Historically this was a plain `Boolean` on `TargetSettings.DynamoDB`:
  *
  *   - `true` -> consume DynamoDB Streams (24h retention)
  *   - `false` -> snapshot only
  *
  * The new `kinesis` variant consumes Amazon Kinesis Data Streams for DynamoDB, which supports up
  * to 1 year of retention and `AT_TIMESTAMP` replay — meaning snapshot transfers longer than 24h no
  * longer drop writes. The YAML decoder preserves the legacy boolean spelling so existing
  * configurations keep working unchanged.
  */
sealed trait StreamChangesSetting {

  /** `true` when any streaming replication (DDB Streams or Kinesis) should run after the snapshot.
    * Kept to make the legacy `if (target.streamChanges) …` branch in `AlternatorMigrator` a
    * one-line translation rather than a scattered refactor.
    */
  def isEnabled: Boolean
}

object StreamChangesSetting {

  /** Snapshot-only: the migrator exits once the snapshot has been written. */
  case object Disabled extends StreamChangesSetting {
    val isEnabled: Boolean = false
  }

  /** Consume the source table's DynamoDB Stream (24h retention). This is the pre-existing behaviour
    * and remains the default for the legacy `streamChanges: true` form.
    */
  case object DynamoDBStreams extends StreamChangesSetting {
    val isEnabled: Boolean = true
  }

  /** Consume a pre-existing Kinesis Data Stream that the source DynamoDB table publishes change
    * records into.
    *
    * @param streamArn
    *   Full ARN of the Kinesis Data Stream (e.g.
    *   `arn:aws:kinesis:us-east-1:123456789012:stream/my-stream`). Bare stream names are rejected
    *   because DynamoDB's `EnableKinesisStreamingDestination` API requires an ARN and the migrator
    *   has no account-id / region context with which to synthesize one. The stream must be
    *   pre-created by the user (shard count, retention, KMS encryption are user decisions); the
    *   migrator only enables the DDB-side streaming destination for it.
    * @param initialTimestamp
    *   Optional ISO-8601 instant used as the KCL `AT_TIMESTAMP` initial position. When empty,
    *   `AlternatorMigrator` defaults this to the moment snapshot transfer starts (T0) so writes
    *   that arrive during a long-running snapshot are still replayed afterwards. An
    *   `initialTimestamp` set more than 5 minutes in the future is rejected at decode time — it is
    *   almost always a clock-skew or typo (e.g. `2026` instead of `2025`) and silently accepting it
    *   means the KCL consumer sits on `AT_TIMESTAMP` for hours before producing anything.
    * @param appName
    *   Optional override for the KCL application name (a.k.a. lease-table name). When empty,
    *   defaults to `migrator_<src.table>_<arn-hash-8>` so concurrent migrators that fan out one
    *   source to two different Kinesis destinations get distinct lease tables. Override this only
    *   when you have a pre-existing lease table you want to reuse.
    */
  case class KinesisDataStreams(
    streamArn: String,
    initialTimestamp: Option[Instant] = None,
    appName: Option[String] = None
  ) extends StreamChangesSetting {
    val isEnabled: Boolean = true

    /** The region embedded in [[streamArn]], lifted out so `AlternatorMigrator` can cross-check it
      * against `source.region` at orchestration time (LOGIC-8). Non-empty by construction because
      * [[validateArn]] refuses ARNs that don't contain a region.
      */
    lazy val arnRegion: String = KinesisArn.regionOf(streamArn)

    /** The bare stream-name segment embedded in [[streamArn]] (the portion after `stream/`). Used
      * by `DynamoStreamReplication` to feed Spark's `KinesisInputDStream.Builder.streamName`, which
      * ultimately binds to KCL 1.x `DescribeStream` — AWS rejects a full ARN there because the
      * service-side `StreamName` parameter is validated against `[a-zA-Z0-9_.-]{1,128}`. The full
      * [[streamArn]] is still used for `EnableKinesisStreamingDestination` (DynamoDB SDK v2) which
      * requires an ARN. Non-empty by construction because [[validateArn]] enforces a non-empty tail
      * after `stream/`.
      */
    lazy val arnName: String = KinesisArn.nameOf(streamArn)
  }

  /** Decode the raw `streamChanges` YAML/JSON value into a [[StreamChangesSetting]].
    *
    * Accepts four shapes for backward compatibility + the new Kinesis form:
    *
    *   - boolean `true` -> [[DynamoDBStreams]]
    *   - boolean `false` -> [[Disabled]]
    *   - `null` (i.e. field absent) -> [[Disabled]]
    *   - object `{type: dynamodb-streams}` -> [[DynamoDBStreams]]
    *   - object `{type: disabled}` -> [[Disabled]]
    *   - object `{type: kinesis | kinesis-data-streams, streamArn, initialTimestamp?, appName?}` ->
    *     [[KinesisDataStreams]]
    *
    * Non-boolean scalars and unknown `type` tokens produce a `DecodingFailure` with an actionable
    * message rather than a silent fallback.
    */
  implicit val decoder: Decoder[StreamChangesSetting] = Decoder.instance { cursor =>
    val json = cursor.value
    if (json.isNull) {
      Right(Disabled)
    } else if (json.isBoolean) {
      json.asBoolean match {
        case Some(true)  => Right(DynamoDBStreams)
        case Some(false) => Right(Disabled)
        case None        =>
          // Unreachable: we just checked `isBoolean`, but Circe's asBoolean is Option-typed.
          Left(DecodingFailure("streamChanges: impossible boolean decode", cursor.history))
      }
    } else if (json.isObject) {
      decodeObject(cursor)
    } else {
      Left(
        DecodingFailure(
          "streamChanges must be a boolean, null, or an object with a 'type' field. " +
            "Got: " + json.noSpaces,
          cursor.history
        )
      )
    }
  }

  private def decodeObject(cursor: HCursor): Decoder.Result[StreamChangesSetting] =
    cursor.get[String]("type").flatMap {
      case "disabled" | "none" | "off" =>
        Right(Disabled)
      case "dynamodb-streams" | "dynamodb" | "ddb" =>
        Right(DynamoDBStreams)
      case "kinesis" | "kinesis-data-streams" =>
        for {
          streamArn        <- cursor.get[String]("streamArn").flatMap(validateArn(_, cursor))
          initialRaw       <- cursor.getOrElse[Option[Instant]]("initialTimestamp")(None)
          initialTimestamp <- validateInitialTimestamp(initialRaw, cursor)
          appName          <- cursor.getOrElse[Option[String]]("appName")(None)
        } yield KinesisDataStreams(streamArn, initialTimestamp, appName)
      case other =>
        Left(
          DecodingFailure(
            s"Unknown streamChanges type '$other'. Valid values: " +
              "'disabled', 'dynamodb-streams', 'kinesis' (a.k.a. 'kinesis-data-streams').",
            cursor.history
          )
        )
    }

  /** Validate that the configured `streamArn` is a well-formed Kinesis ARN in one of the three AWS
    * partitions. The regex enforces:
    *
    *   - Partition ∈ {`aws`, `aws-cn`, `aws-us-gov`}. These are the only partitions AWS publishes;
    *     a typo (e.g. `arn:aws2:kinesis:…`) fails fast instead of surfacing deep inside the SDK.
    *   - Region is lowercase alphanumeric with hyphens — matches every region AWS has ever
    *     launched.
    *   - Account id is exactly 12 digits, matching the AWS spec. A substring check (the previous
    *     behaviour) would accept `arn:aws:kinesis::stream/foo` — no account at all — and then fail
    *     with a confusing 403 from `EnableKinesisStreamingDestination`.
    *   - Stream name is non-empty and does not contain `:`, `/`, or whitespace. Stream names in AWS
    *     may contain `-`, `.`, `_`, and alphanumerics; we accept any non-empty tail that isn't
    *     obviously wrong rather than enforcing the exact charset because AWS has quietly relaxed
    *     the naming rules in the past.
    */
  // `private[config]` so the sibling [[KinesisArn]] helper can reuse it for region extraction.
  private[config] val KinesisArnPattern =
    """^arn:(aws|aws-cn|aws-us-gov):kinesis:([a-z0-9-]+):(\d{12}):stream/([^:/\s]+)$""".r

  private def validateArn(arn: String, cursor: HCursor): Decoder.Result[String] = {
    val trimmed = arn.trim
    KinesisArnPattern.findFirstMatchIn(trimmed) match {
      case Some(_) => Right(trimmed)
      case None =>
        Left(
          DecodingFailure(
            s"streamChanges.streamArn must be a full Kinesis ARN of the form " +
              "'arn:<partition>:kinesis:<region>:<12-digit-account>:stream/<stream-name>' " +
              "(partition ∈ {aws, aws-cn, aws-us-gov}). " +
              s"Got: '$arn'",
            cursor.history
          )
        )
    }
  }

  /** Upper bound on how far into the future [[KinesisDataStreams.initialTimestamp]] may be. A
    * timestamp beyond this window almost always indicates a typo (e.g. 2026 -> 2035) or a driver
    * clock skewed by NTP failure; accepting it would park the KCL consumer on `AT_TIMESTAMP` for
    * years before producing any records. Five minutes covers ordinary NTP drift without masking
    * real mistakes.
    */
  private val MaxFutureInitialTimestampSkew: JDuration = JDuration.ofMinutes(5)

  /** Validate that an [[KinesisDataStreams.initialTimestamp]] is not set to an absurd future
    * instant. Past timestamps are always accepted; the KCL / AWS retention window is validated at
    * runtime because only AWS knows the stream's retention.
    *
    * Uses [[Instant.now]] — deterministic in tests as long as the test's chosen timestamp is either
    * clearly past ("2024-01-01") or clearly far-future ("9999-01-01"). A test that uses
    * `Instant.now().plus(minutes)` is acceptable too because this window is 5 minutes.
    */
  private def validateInitialTimestamp(
    ts: Option[Instant],
    cursor: HCursor
  ): Decoder.Result[Option[Instant]] =
    ts match {
      case None => Right(None)
      case Some(t) =>
        val now = Instant.now()
        if (t.isAfter(now.plus(MaxFutureInitialTimestampSkew)))
          Left(
            DecodingFailure(
              s"streamChanges.initialTimestamp '$t' is more than " +
                s"${MaxFutureInitialTimestampSkew.toMinutes} minutes in the future (clock is " +
                s"'$now'). This is almost always a typo or a driver-clock skew; if you really " +
                "want to start consuming at a future instant, correct the driver clock first.",
              cursor.history
            )
          )
        else
          Right(Some(t))
    }

  /** Round-trip encoder. The legacy boolean form is used for [[Disabled]] / [[DynamoDBStreams]] so
    * that round-tripping an old YAML file does not change its shape; the object form is only used
    * for the new Kinesis variant which has no boolean representation.
    */
  implicit val encoder: Encoder[StreamChangesSetting] = Encoder.instance {
    case Disabled        => Json.False
    case DynamoDBStreams => Json.True
    case KinesisDataStreams(arn, initialTs, app) =>
      val fields = List(
        "type"      -> Json.fromString("kinesis"),
        "streamArn" -> Json.fromString(arn)
      ) ++ initialTs.map(ts => "initialTimestamp" -> Json.fromString(ts.toString)) ++
        app.map(a => "appName" -> Json.fromString(a))
      Json.obj(fields: _*)
  }
}

/** Parse-side helpers for Kinesis ARNs. Lives next to [[StreamChangesSetting]] because it is the
  * only caller but is a separate object so the tests can cover the helper in isolation without
  * going through the whole decoder.
  */
private[config] object KinesisArn {

  /** Extract the region field from a Kinesis ARN. The caller must have already confirmed the ARN is
    * well-formed (via [[StreamChangesSetting.KinesisArnPattern]]); calling this on an ARN that does
    * not match the pattern throws [[IllegalArgumentException]].
    */
  def regionOf(arn: String): String =
    StreamChangesSetting.KinesisArnPattern.findFirstMatchIn(arn.trim) match {
      case Some(m) => m.group(2)
      case None =>
        throw new IllegalArgumentException(
          s"Cannot extract region from malformed Kinesis ARN: '$arn'. " +
            "This is a programming error — regionOf must only be called on ARNs that have " +
            "already passed StreamChangesSetting.validateArn."
        )
    }

  /** Extract the stream-name segment (the part after `stream/`) from a Kinesis ARN. Symmetric with
    * [[regionOf]]. The caller must have already confirmed the ARN is well-formed (via
    * [[StreamChangesSetting.KinesisArnPattern]]); calling this on an ARN that does not match the
    * pattern throws [[IllegalArgumentException]].
    *
    * Used by `DynamoStreamReplication` to feed Spark's `KinesisInputDStream.Builder.streamName`.
    * Spark forwards this string to KCL 1.x `DescribeStream`, whose `StreamName` parameter is
    * validated by the Kinesis service against `[a-zA-Z0-9_.-]{1,128}` — a full ARN (which contains
    * `:` and `/`) would be rejected at runtime with an `InvalidArgumentException`. The full ARN is
    * still required elsewhere for `EnableKinesisStreamingDestination` (DynamoDB SDK v2), so we keep
    * the ARN in config and strip to the bare name only at this one call site.
    */
  def nameOf(arn: String): String =
    StreamChangesSetting.KinesisArnPattern.findFirstMatchIn(arn.trim) match {
      case Some(m) => m.group(4)
      case None =>
        throw new IllegalArgumentException(
          s"Cannot extract stream name from malformed Kinesis ARN: '$arn'. " +
            "This is a programming error — nameOf must only be called on ARNs that have " +
            "already passed StreamChangesSetting.validateArn."
        )
    }
}
