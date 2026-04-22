package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  PutItemRequest
}

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path, Paths }
import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
import scala.jdk.CollectionConverters._
import scala.util.chaining.scalaUtilChainingOps

/** Live integration test for the Kinesis Data Streams streaming path.
  *
  * Gated behind `KINESIS_LIVE_TEST=1` because it:
  *   - reads change events from a PRE-CREATED Kinesis Data Stream in a real AWS account
  *   - mutates AWS state (enables the DynamoDB → Kinesis destination on the source table, consumes
  *     shards, writes a KCL lease table)
  *   - takes ~1-5 minutes end-to-end (``EnableKinesisStreamingDestination`` is eventually
  *     consistent and typically takes 30-60s to go ACTIVE)
  *
  * Required environment variables when running:
  *   - `KINESIS_LIVE_TEST=1` — gate (otherwise the single test in this suite is skipped via
  *     `munit.assume(...)`)
  *   - `KINESIS_STREAM_ARN=arn:aws:…` — pre-created stream; the migrator does not create it
  *   - `AWS_REGION=<region>` — required by `MigratorSuiteWithAWS`
  *   - AWS credentials (environment or `~/.aws/credentials`; handled by `MigratorSuiteWithAWS`)
  *
  * In CI (where the env var is not set), the test body short-circuits on the first line so the rest
  * of the test matrix keeps running without flakiness. `MigratorSuiteWithAWS.beforeAll` still runs
  * and attempts to read `AWS_REGION`; skipping only the test body avoids a build-stalling failure
  * when `AWS_REGION` is unset in CI.
  */
class KinesisStreamedItemsTest extends MigratorSuiteWithAWS {

  override val munitTimeout: Duration = 300.seconds

  private val GeneratedConfigFileName =
    "dynamodb-to-alternator-streaming-kinesis-live.yaml"

  private def generatedConfigPath: Path =
    Paths.get("src", "test", "configurations", GeneratedConfigFileName).toAbsolutePath

  withTable("KinesisStreamedItemsTest").test("Stream changes via Kinesis Data Streams") {
    tableName =>
      assume(
        sys.env.get("KINESIS_LIVE_TEST").contains("1"),
        "Skipping: set KINESIS_LIVE_TEST=1 and KINESIS_STREAM_ARN=<arn> to run live Kinesis tests"
      )
      val kinesisStreamArn = sys.env.getOrElse(
        "KINESIS_STREAM_ARN",
        fail(
          "KINESIS_STREAM_ARN env var is required when KINESIS_LIVE_TEST=1 " +
            "(pre-create a Kinesis Data Stream and export its full ARN)"
        )
      )

      val templatePath =
        Paths
          .get("src", "test", "configurations", "dynamodb-to-alternator-streaming-kinesis.yaml")
          .toAbsolutePath
      val template = new String(Files.readAllBytes(templatePath), StandardCharsets.UTF_8)

      // Regex-replace rather than String.replace("arn:...:stream/...example") because the
      // example ARN in the template can be tuned later without this test silently no-op'ing.
      val configText = template.replaceAll(
        """streamArn:\s*arn:aws:kinesis:[^\s]+""",
        s"streamArn: ${kinesisStreamArn}"
      )
      Files.write(generatedConfigPath, configText.getBytes(StandardCharsets.UTF_8))

      try {
        val keys1 = Map("id" -> AttributeValue.fromS("kds-12345"))
        val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
        val item1Data = keys1 ++ attrs1
        sourceDDb().putItem(
          PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build()
        )

        val sparkJob = SparkUtils.submitMigrationInBackground(GeneratedConfigFileName)

        awaitAtMost(120.seconds) {
          targetAlternator()
            .getItem(GetItemRequest.builder().tableName(tableName).key(keys1.asJava).build())
            .tap { itemResult =>
              assert(itemResult.hasItem, "First item not found in the target database")
              assertEquals(itemResult.item.asScala.toMap, item1Data)
            }
        }

        // After snapshot completion, the Kinesis receiver starts; inserting another item
        // should eventually replicate via KDS. Give it a generous window because KDS has a
        // slightly higher steady-state latency than DDB Streams (30-120s in practice).
        val keys2 = Map("id" -> AttributeValue.fromS("kds-67890"))
        val attrs2 = Map("foo" -> AttributeValue.fromS("baz"))
        val item2Data = keys2 ++ attrs2
        sourceDDb().putItem(
          PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build()
        )

        awaitAtMost(180.seconds) {
          targetAlternator()
            .getItem(GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build())
            .tap { itemResult =>
              assert(itemResult.hasItem, "Second item not found in the target database")
              // The op-type is now carried out-of-band on `StreamChange` (LOGIC-1). Target items
              // must NOT contain an `_dynamo_op_type` attribute — that would be a regression to
              // the in-band marker which could collide with user data.
              assertEquals(itemResult.item.asScala.toMap, item2Data)
              assert(
                !itemResult.item.asScala.contains("_dynamo_op_type"),
                "Target row must not contain the legacy _dynamo_op_type marker (LOGIC-1)"
              )
            }
        }

        sparkJob.stop()

        // KCL writes a lease table named after the configured appName
        // ("migrator_KinesisStreamedItemsTest"); clean it up so re-runs start with a fresh
        // shard assignment.
        deleteLeaseTableIfExists("migrator_KinesisStreamedItemsTest")
      } finally
        Files.deleteIfExists(generatedConfigPath)
  }

  /** Continuously evaluate `assertion` until it succeeds (MUnit models failures as
    * `AssertionError`), or until the deadline passes. Tests use this rather than a simple sleep
    * because KDS propagation latency varies considerably across runs and regions.
    */
  def awaitAtMost(delay: FiniteDuration)(assertion: => Unit): Unit = {
    val deadline = delay.fromNow
    var maybeFailure: Option[AssertionError] = Some(
      new AssertionError("Assertion has not been tested yet")
    )
    while (maybeFailure.isDefined && deadline.hasTimeLeft())
      try {
        assertion
        maybeFailure = None
      } catch {
        case failure: AssertionError =>
          maybeFailure = Some(failure)
          Thread.sleep(2000)
      }
    for (failure <- maybeFailure)
      fail(s"Assertion was not true after ${delay}", failure)
  }

  private def deleteLeaseTableIfExists(tableName: String): Unit =
    try deleteTableIfExists(sourceDDb(), tableName)
    catch {
      case _: Throwable => ()
    }
}
