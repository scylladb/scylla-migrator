package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }
import org.junit.experimental.categories.Category

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

abstract class CopyMissingRowsTest(version: CassandraVersion) extends MigratorSuite(version.port) {

  override val munitTimeout: Duration = 120.seconds

  private val sourceInsertWritetimeMicros: Long = 1000000000000L
  private val sourceInsertTtlSeconds: Int = 86400
  private val fixedTargetWriteTtlSeconds: Int = 43200
  private val fixedTargetWriteWritetimeMicros: Long = 2000000000000L

  private val basicConfigFile =
    CassandraVersion.configForSource("cassandra-to-scylla-basic.yaml", version)
  private val copyMissingConfigFile =
    CassandraVersion.configForSource("cassandra-to-scylla-copy-missing-rows.yaml", version)
  private val fixedTargetWriteConfigFile =
    CassandraVersion.configForSource(
      "cassandra-to-scylla-copy-missing-rows-fixed-target-write-settings.yaml",
      version
    )

  private def assertTargetRowMetadata(
    tableName: String,
    rowId: String,
    expectedFoo: String,
    expectedBar: Int,
    expectedTtlSeconds: Int,
    expectedWritetimeMicros: Long
  ): Unit = {
    val rows = targetScylla()
      .execute(
        s"SELECT id, foo, bar, TTL(foo) AS foo_ttl, WRITETIME(foo) AS foo_writetime " +
          s"FROM ${keyspace}.${tableName} WHERE id = '${rowId}'"
      )
      .all()
      .asScala

    assertEquals(rows.size, 1, "Expected repaired row to exist in target")

    val row = rows.head
    assertEquals(row.getString("id"), rowId)
    assertEquals(row.getString("foo"), expectedFoo)
    assertEquals(row.getInt("bar"), expectedBar)
    assertEquals(row.getLong("foo_writetime"), expectedWritetimeMicros)

    val rowTtl = row.getInt("foo_ttl")
    assert(rowTtl > 0, s"TTL should be > 0, got ${rowTtl}")
    assert(rowTtl <= expectedTtlSeconds, s"TTL should be <= ${expectedTtlSeconds}, got ${rowTtl}")
  }

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows preserves source TTL and writetime"
  ) { tableName =>
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('12345', 'bar', 42) " +
        s"USING TTL ${sourceInsertTtlSeconds} AND TIMESTAMP ${sourceInsertWritetimeMicros}"
    )

    successfullyPerformMigration(basicConfigFile)
    assertEquals(performValidation(basicConfigFile), 0, "Initial validation failed")

    // Delete the row from the target to simulate a missing row
    val deleteStatement =
      QueryBuilder
        .deleteFrom(keyspace, tableName)
        .whereColumn("id")
        .isEqualTo(literal("12345"))
        .build()
    targetScylla().execute(deleteStatement)

    // Validate with copyMissingRows enabled — detects the missing row and copies it
    assertEquals(performValidation(copyMissingConfigFile), 1, "Should detect missing row")

    // Validate again with timestamp comparison — the repaired row should now converge fully
    assertEquals(performValidation(basicConfigFile), 0, "Row should have been copied to target")

    assertTargetRowMetadata(
      tableName,
      rowId = "12345",
      expectedFoo = "bar",
      expectedBar = 42,
      expectedTtlSeconds = sourceInsertTtlSeconds,
      expectedWritetimeMicros = sourceInsertWritetimeMicros
    )
  }

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows honors fixed target write settings"
  ) { tableName =>
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('67890', 'baz', 99)"
    )

    successfullyPerformMigration(fixedTargetWriteConfigFile)
    assertEquals(performValidation(fixedTargetWriteConfigFile), 0, "Initial validation failed")

    val deleteStatement =
      QueryBuilder
        .deleteFrom(keyspace, tableName)
        .whereColumn("id")
        .isEqualTo(literal("67890"))
        .build()
    targetScylla().execute(deleteStatement)

    assertEquals(performValidation(fixedTargetWriteConfigFile), 1, "Should detect missing row")
    assertEquals(
      performValidation(fixedTargetWriteConfigFile),
      0,
      "Row should have been copied using fixed target write settings"
    )

    assertTargetRowMetadata(
      tableName,
      rowId = "67890",
      expectedFoo = "baz",
      expectedBar = 99,
      expectedTtlSeconds = fixedTargetWriteTtlSeconds,
      expectedWritetimeMicros = fixedTargetWriteWritetimeMicros
    )
  }

}

@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra2CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V2)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra3CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V3)
class Cassandra4CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V4)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V5)
