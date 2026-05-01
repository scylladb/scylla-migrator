package com.scylladb.migrator.config

import io.circe.yaml
import org.apache.spark.SparkConf

class SecurityHardeningTest extends munit.FunSuite {

  test("SparkSecretRedaction covers S3A and DynamoDB secret option keys by default") {
    SparkSecretRedaction.ensureKeysRedacted(
      None,
      Seq(
        "fs.s3a.access.key",
        "fs.s3a.secret.key",
        "fs.s3a.session.token",
        "dynamodb.awsAccessKeyId",
        "dynamodb.awsSecretAccessKey",
        "dynamodb.awsSessionToken",
        "fs.s3a.api.key",
        "aws.credentials.file",
        "spark.cassandra.auth.password",
        "spark.cassandra.connection.ssl.trustStore.password"
      ),
      "test options"
    )
  }

  test("SparkSecretRedaction rejects custom regex that misses sensitive option keys") {
    intercept[IllegalArgumentException] {
      SparkSecretRedaction.ensureKeysRedacted(
        Some("(?i)token"),
        Seq("fs.s3a.secret.key"),
        "test options"
      )
    }
  }

  test(
    "SparkSecretRedaction rejects local-only sensitive keys under explicit Spark default regex"
  ) {
    val error = intercept[IllegalArgumentException] {
      SparkSecretRedaction.ensureKeysRedacted(
        Some(SparkSecretRedaction.SparkDefaultRedactionRegex),
        Seq("fs.s3a.api.key", "aws.credentials.file"),
        "test options"
      )
    }

    assert(error.getMessage.contains("fs.s3a.api.key"))
    assert(error.getMessage.contains("aws.credentials.file"))
  }

  test("SparkSecretRedaction installs migrator regex into SparkConf before Spark starts") {
    val sparkConf = new SparkConf(false)

    SparkSecretRedaction.ensureKeysRedacted(
      sparkConf,
      Seq("fs.s3a.api.key", "aws.credentials.file"),
      "test options"
    )

    assertEquals(
      sparkConf.get(SparkSecretRedaction.RedactionRegexConfKey),
      SensitiveKeys.DefaultRedactionRegex
    )
  }

  test("SparkSecretRedaction keeps user-provided SparkConf regex") {
    val sparkConf = new SparkConf(false)
      .set(SparkSecretRedaction.RedactionRegexConfKey, "(?i)token")

    intercept[IllegalArgumentException] {
      SparkSecretRedaction.ensureKeysRedacted(
        sparkConf,
        Seq("fs.s3a.secret.key"),
        "test options"
      )
    }

    assertEquals(sparkConf.get(SparkSecretRedaction.RedactionRegexConfKey), "(?i)token")
  }

  test("Cassandra source host validation rejects URL metacharacters") {
    val result = parseConfig(
      """source:
        |  type: cassandra
        |  host: bad/host
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  splitCount: 16
        |  connections: 4
        |  fetchSize: 1000
        |  preserveTimestamps: false
        |  consistencyLevel: LOCAL_QUORUM
        |target:
        |  type: parquet
        |  path: /tmp/out
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin
    )

    assert(result.isLeft, s"Expected invalid config, got: $result")
    assert(result.left.exists(_.getMessage.contains("Source type 'cassandra'")))
  }

  test("Scylla target host validation rejects URL metacharacters") {
    val result = parseConfig(
      """source:
        |  type: parquet
        |  path: /tmp/in
        |target:
        |  type: scylla
        |  host: bad?host
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin
    )

    assert(result.isLeft, s"Expected invalid config, got: $result")
    assert(result.left.exists(_.getMessage.contains("Target type 'scylla'")))
  }

  test("DynamoDB endpoint validation rejects URL paths and embedded ports") {
    val withPath = parseConfig(
      """source:
        |  type: dynamodb
        |  table: Src
        |  endpoint:
        |    host: http://dynamodb/private
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin
    )
    val withPort = parseConfig(
      """source:
        |  type: dynamodb
        |  table: Src
        |  endpoint:
        |    host: http://dynamodb:8000
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin
    )

    assert(withPath.isLeft, s"Expected invalid endpoint path, got: $withPath")
    assert(withPort.isLeft, s"Expected invalid embedded endpoint port, got: $withPort")
  }

  test("DynamoDB endpoint render safely wraps bare IPv6 hosts") {
    val endpoint = DynamoDBEndpoint("2001:db8::1", 8000)

    assertEquals(endpoint.renderEndpoint, "http://[2001:db8::1]:8000")
  }

  private def parseConfig(config: String): Either[io.circe.Error, MigratorConfig] =
    yaml.parser.parse(config).flatMap(_.as[MigratorConfig])
}
