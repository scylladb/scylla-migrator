package com.scylladb.migrator.writers

import com.scylladb.migrator.config.{ DynamoDBEndpoint, SourceSettings }

/** Tests for DynamoStreamReplication.buildCheckpointTableName determinism and edge cases. */
class BuildCheckpointTableNameTest extends munit.FunSuite {

  private def makeSettings(
    table: String,
    endpoint: Option[DynamoDBEndpoint] = None,
    region: Option[String] = None
  ): SourceSettings.DynamoDB =
    SourceSettings.DynamoDB(
      endpoint              = endpoint,
      region                = region,
      credentials           = None,
      table                 = table,
      scanSegments          = None,
      readThroughput        = None,
      throughputReadPercent = None,
      maxMapTasks           = None
    )

  test("simple table name without endpoint or region") {
    val name = DynamoStreamReplication.buildCheckpointTableName(makeSettings("MyTable"))
    assertEquals(name, "migrator_MyTable")
  }

  test("deterministic: same inputs produce same output") {
    val s = makeSettings("T", Some(DynamoDBEndpoint("localhost", 8000)), Some("us-east-1"))
    val a = DynamoStreamReplication.buildCheckpointTableName(s)
    val b = DynamoStreamReplication.buildCheckpointTableName(s)
    assertEquals(a, b)
  }

  test("different endpoints produce different names") {
    val a = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", Some(DynamoDBEndpoint("host-a", 8000)), Some("us-east-1"))
    )
    val b = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", Some(DynamoDBEndpoint("host-b", 8000)), Some("us-east-1"))
    )
    assertNotEquals(a, b)
  }

  test("different regions produce different names") {
    val a = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", None, Some("us-east-1"))
    )
    val b = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", None, Some("eu-west-1"))
    )
    assertNotEquals(a, b)
  }

  test("different table names produce different names") {
    val a = DynamoStreamReplication.buildCheckpointTableName(makeSettings("TableA"))
    val b = DynamoStreamReplication.buildCheckpointTableName(makeSettings("TableB"))
    assertNotEquals(a, b)
  }

  test("name includes hash suffix when endpoint is set") {
    val name = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", Some(DynamoDBEndpoint("localhost", 8000)))
    )
    assert(name.startsWith("migrator_T_"), s"Expected hash suffix, got: $name")
    // Hash suffix is 8 hex chars
    val suffix = name.stripPrefix("migrator_T_")
    assert(suffix.matches("[0-9a-f]{8}"), s"Expected 8 hex chars, got: $suffix")
  }

  test("name includes hash suffix when region is set") {
    val name = DynamoStreamReplication.buildCheckpointTableName(
      makeSettings("T", region = Some("us-east-1"))
    )
    assert(name.startsWith("migrator_T_"))
  }

  test("empty table name edge case") {
    val name = DynamoStreamReplication.buildCheckpointTableName(makeSettings(""))
    assertEquals(name, "migrator_")
  }
}
