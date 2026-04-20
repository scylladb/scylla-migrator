package com.scylladb.migrator.config

/** Asserts that the example YAML configuration for the Kinesis Data Streams path parses into a
  * `MigratorConfig` without errors.
  *
  * This guards against a silent skew between the example YAML in
  * `tests/src/test/configurations/dynamodb-to-alternator-streaming-kinesis.yaml` (also embedded in
  * docs/source/stream-changes.rst) and the actual decoder. A mismatch — e.g. renaming `streamArn`
  * without updating the example — would mean the docs advertise a syntax that does not compile,
  * which the gated integration test cannot catch because it is OFF in CI.
  */
class KinesisStreamingYamlExampleTest extends munit.FunSuite {

  test("dynamodb-to-alternator-streaming-kinesis.yaml parses and produces the expected shape") {
    val configPath =
      java.nio.file.Paths
        .get(
          "src",
          "test",
          "configurations",
          "dynamodb-to-alternator-streaming-kinesis.yaml"
        )
        .toAbsolutePath
        .toString
    val config = MigratorConfig.loadFrom(configPath)

    config.target match {
      case ddb: TargetSettings.DynamoDB =>
        ddb.streamChanges match {
          case kinesis: StreamChangesSetting.KinesisDataStreams =>
            assert(
              kinesis.streamArn.startsWith("arn:aws:kinesis:"),
              s"example streamArn should be a real-looking ARN, got: ${kinesis.streamArn}"
            )
            assertEquals(kinesis.appName, Some("migrator_KinesisStreamedItemsTest"))
          case other =>
            fail(
              s"Expected streamChanges to be a Kinesis configuration in the example YAML, got $other"
            )
        }
      case other =>
        fail(s"Expected a DynamoDB target in the example YAML, got $other")
    }
  }
}
