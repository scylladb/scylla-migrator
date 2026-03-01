package com.scylladb.migrator.config

import io.circe.syntax._
import io.circe.yaml

class ParquetTargetValidationTest extends munit.FunSuite {

  private def decodeTarget(yamlStr: String) =
    yaml.parser
      .parse(yamlStr)
      .flatMap(_.as[TargetSettings])

  test("valid Parquet target with defaults") {
    val result = decodeTarget(
      """type: parquet
        |path: /tmp/output
        |""".stripMargin
    )
    assertEquals(result, Right(TargetSettings.Parquet("/tmp/output", "snappy", "error")))
  }

  test("valid Parquet target with explicit compression and mode") {
    val result = decodeTarget(
      """type: parquet
        |path: /tmp/output
        |compression: gzip
        |mode: overwrite
        |""".stripMargin
    )
    assertEquals(result, Right(TargetSettings.Parquet("/tmp/output", "gzip", "overwrite")))
  }

  test("invalid compression codec returns DecodingFailure") {
    val result = decodeTarget(
      """type: parquet
        |path: /tmp/output
        |compression: deflate
        |""".stripMargin
    )
    assert(result.isLeft, s"Expected Left but got $result")
    assert(result.left.exists(_.getMessage.contains("Invalid Parquet compression codec")))
  }

  test("invalid write mode returns DecodingFailure") {
    val result = decodeTarget(
      """type: parquet
        |path: /tmp/output
        |mode: upsert
        |""".stripMargin
    )
    assert(result.isLeft, s"Expected Left but got $result")
    assert(result.left.exists(_.getMessage.contains("Invalid Parquet write mode")))
  }

  test("Parquet target encoder/decoder roundtrip") {
    val original: TargetSettings = TargetSettings.Parquet("/data/output", "zstd", "overwrite")
    val json = original.asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }

  test("mixed-case compression and mode are normalized to lowercase") {
    val result = decodeTarget(
      """type: parquet
        |path: /tmp/output
        |compression: Snappy
        |mode: Overwrite
        |""".stripMargin
    )
    assertEquals(result, Right(TargetSettings.Parquet("/tmp/output", "snappy", "overwrite")))
  }

  test("DynamoDBS3Export target encoder/decoder roundtrip") {
    val original: TargetSettings = TargetSettings.DynamoDBS3Export("/data/s3export")
    val json = original.asJson
    val decoded = json.as[TargetSettings]
    assertEquals(decoded, Right(original))
  }
}
