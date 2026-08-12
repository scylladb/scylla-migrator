package com.scylladb.migrator.readers

import com.scylladb.migrator.readers.DynamoDBS3Export.ManifestSummary
import io.circe.parser.{ decode => jsonDecode }

import java.time.Instant

/** Unit tests for the `ManifestSummary` additions that drive the S3-export + streamChanges chaining
  * flow (ARCH-1 / GitHub issue #250 acceptance criterion #4).
  *
  * The production code uses `summary.exportTime.orElse(summary.startTime)` as the default Kinesis
  * `AT_TIMESTAMP` when the caller supplies a `streamSource`, so the contract these tests pin is:
  *
  *   - New fields DECODE from a real DynamoDB-PITR manifest.
  *   - BOTH fields are optional so older manifests (which omit them) continue to work.
  *   - Unknown additional fields in the manifest are tolerated — AWS is free to add new fields to
  *     the export manifest format and we must not break on them.
  */
class DynamoDBS3ExportManifestTest extends munit.FunSuite {

  test("ManifestSummary decodes the exportTime field (ARCH-1)") {
    // Example payload fragment produced by a real DynamoDB PITR export. Only the fields
    // the reader cares about are included — Circe ignores unknowns by default.
    val json =
      """{
        |  "manifestFilesS3Key": "AWSDynamoDB/01715094384115/manifest-files.json",
        |  "itemCount": 1234,
        |  "outputFormat": "DYNAMODB_JSON",
        |  "exportType": "FULL_EXPORT",
        |  "exportTime": "2024-05-07T12:34:56Z",
        |  "startTime": "2024-05-07T12:34:50Z"
        |}""".stripMargin

    val parsed = jsonDecode[ManifestSummary](json).fold(throw _, identity)
    assertEquals(parsed.exportTime, Some(Instant.parse("2024-05-07T12:34:56Z")))
    assertEquals(parsed.startTime, Some(Instant.parse("2024-05-07T12:34:50Z")))
  }

  test("ManifestSummary decodes when exportTime / startTime are absent (backward-compat)") {
    // Older exports (or exports produced by a non-AWS writer such as the migrator's own
    // `DynamoDBS3Export.writeRDD`) omit these fields. Must not fail — `readRDD` returns
    // `None` in that case and logs a warning at the orchestration layer.
    val json =
      """{
        |  "manifestFilesS3Key": "AWSDynamoDB/01715094384115/manifest-files.json",
        |  "itemCount": 1234,
        |  "outputFormat": "DYNAMODB_JSON",
        |  "exportType": "FULL_EXPORT"
        |}""".stripMargin

    val parsed = jsonDecode[ManifestSummary](json).fold(throw _, identity)
    assertEquals(parsed.exportTime, None)
    assertEquals(parsed.startTime, None)
  }

  test("ManifestSummary preserves exportType = None for fresh exports without it") {
    // Very old AWS exports pre-date the `exportType` field. Keep decoding them.
    val json =
      """{
        |  "manifestFilesS3Key": "AWSDynamoDB/x/manifest-files.json",
        |  "itemCount": 0,
        |  "outputFormat": "DYNAMODB_JSON"
        |}""".stripMargin

    val parsed = jsonDecode[ManifestSummary](json).fold(throw _, identity)
    assertEquals(parsed.exportType, None)
  }

  test("ManifestSummary tolerates unknown fields added by AWS (future-proofing)") {
    // AWS extends the manifest schema from time to time — the decoder must ignore fields
    // it doesn't recognize rather than failing the migration at setup.
    val json =
      """{
        |  "manifestFilesS3Key": "AWSDynamoDB/x/manifest-files.json",
        |  "itemCount": 1,
        |  "outputFormat": "DYNAMODB_JSON",
        |  "futureAwsField": "ignore-me",
        |  "nestedExtra": { "k": "v" },
        |  "exportTime": "2024-01-01T00:00:00Z"
        |}""".stripMargin

    val parsed = jsonDecode[ManifestSummary](json).fold(throw _, identity)
    assertEquals(parsed.exportTime, Some(Instant.parse("2024-01-01T00:00:00Z")))
  }
}
