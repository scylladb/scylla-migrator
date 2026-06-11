package com.scylladb.migrator.scylla

import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
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
    expectedWritetimeMicros: Long = 0L,
    minWritetimeMicros: Long = 0L
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

    if (expectedWritetimeMicros != 0L)
      assertEquals(row.getLong("foo_writetime"), expectedWritetimeMicros)
    if (minWritetimeMicros != 0L)
      assert(
        row.getLong("foo_writetime") >= minWritetimeMicros,
        s"writetime should be >= ${minWritetimeMicros}, got ${row.getLong("foo_writetime")}"
      )

    val rowTtl = row.getInt("foo_ttl")
    assert(rowTtl > 0, s"TTL should be > 0, got ${rowTtl}")
    assert(rowTtl <= expectedTtlSeconds, s"TTL should be <= ${expectedTtlSeconds}, got ${rowTtl}")
  }

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows preserves source TTL and beats tombstones"
  ) { tableName =>
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('11111', 'keep', 7) " +
        s"USING TTL ${sourceInsertTtlSeconds} AND TIMESTAMP ${sourceInsertWritetimeMicros}"
    )
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('12345', 'bar', 42) " +
        s"USING TTL ${sourceInsertTtlSeconds} AND TIMESTAMP ${sourceInsertWritetimeMicros}"
    )

    successfullyPerformMigration(basicConfigFile)
    assertEquals(performValidation(basicConfigFile), 0, "Initial validation failed")

    // Delete the row from the target (creating a tombstone with a timestamp newer than
    // the source writetime). The repair must use coordinator time to beat the tombstone.
    targetScylla().execute(
      s"DELETE FROM ${keyspace}.${tableName} WHERE id = '12345'"
    )

    val repairStartTimeMicros = System.currentTimeMillis() * 1000L

    assertEquals(performValidation(copyMissingConfigFile), 1, "Should detect missing row")

    assertTargetRowMetadata(
      tableName,
      rowId              = "12345",
      expectedFoo        = "bar",
      expectedBar        = 42,
      expectedTtlSeconds = sourceInsertTtlSeconds,
      minWritetimeMicros = repairStartTimeMicros
    )
  }

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows honors fixed target write settings"
  ) { tableName =>
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('11111', 'keep', 7)"
    )
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('67890', 'baz', 99)"
    )

    successfullyPerformMigration(fixedTargetWriteConfigFile)
    assertEquals(performValidation(fixedTargetWriteConfigFile), 0, "Initial validation failed")

    dropAndRecreateTable(targetScylla(), keyspace, tableName, identity)
    targetScylla().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('11111', 'keep', 7) " +
        s"USING TTL ${fixedTargetWriteTtlSeconds} AND TIMESTAMP ${fixedTargetWriteWritetimeMicros}"
    )

    assertEquals(performValidation(fixedTargetWriteConfigFile), 1, "Should detect missing row")

    assertTargetRowMetadata(
      tableName,
      rowId                   = "67890",
      expectedFoo             = "baz",
      expectedBar             = 99,
      expectedTtlSeconds      = fixedTargetWriteTtlSeconds,
      expectedWritetimeMicros = fixedTargetWriteWritetimeMicros
    )

    assertEquals(
      performValidation(fixedTargetWriteConfigFile),
      0,
      "Row should have been copied using fixed target write settings"
    )
  }

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows rejects timestamp preservation for collection columns"
  ) { tableName =>
    val alterTable =
      s"ALTER TABLE ${keyspace}.${tableName} ADD tags set<text>"
    sourceCassandra().execute(alterTable)
    targetScylla().execute(alterTable)

    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar, tags) " +
        s"VALUES ('24680', 'bar', 42, {'x', 'y'})"
    )

    val err = intercept[Exception] {
      performValidation(copyMissingConfigFile)
    }

    assert(
      err.getMessage.contains(
        "TTL/Writetime preservation is unsupported for tables with collection types"
      ),
      s"Unexpected error: ${err.getMessage}"
    )
  }

}

@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra2CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V2)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra3CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V3)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra4CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V4)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5CopyMissingRowsTest extends CopyMissingRowsTest(CassandraVersion.V5)
