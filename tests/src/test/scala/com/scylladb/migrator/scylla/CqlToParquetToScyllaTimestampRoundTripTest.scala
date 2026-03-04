package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.scylladb.migrator.Integration
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import com.scylladb.migrator.TestFileUtils

import java.nio.file.{ Files, Path, Paths }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.chaining._

/** Round-trip integration test: CQL (with TTL and writetime) -> Parquet -> Scylla.
  *
  * Verifies that TTL and writetime metadata are preserved through the Parquet intermediate format
  * using the `__migrator_meta_*` column convention.
  */
@Category(Array(classOf[Integration]))
class CqlToParquetToScyllaTimestampRoundTripTest extends MigratorSuite(sourcePort = 9043) {

  override val munitTimeout: FiniteDuration = 3.minutes

  val tableName = "timestamptest"

  val insertWritetimeMicros: Long = 1000000000000L // microseconds
  val insertTtlSeconds: Int = 86400 // 1 day

  val parquetHostRoot: Path = Paths.get("docker/parquet")

  val withTimestampTable: FunFixture[String] =
    FunFixture(
      setup = { _ =>
        dropAndRecreate(sourceCassandra(), tableName)
        dropAndRecreate(targetScylla(), tableName)
        tableName
      },
      teardown = { _ =>
        val dropTableQuery = SchemaBuilder.dropTable(keyspace, tableName).build()
        targetScylla().execute(dropTableQuery)
        sourceCassandra().execute(dropTableQuery)
        TestFileUtils.deleteRecursive(parquetHostRoot.resolve("timestamps").toFile)
        ()
      }
    )

  withTimestampTable.test("CQL -> Parquet -> Scylla preserves TTL and writetime") { _ =>
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('row1', 'hello', 42) " +
        s"USING TTL ${insertTtlSeconds} AND TIMESTAMP ${insertWritetimeMicros}"
    )
    sourceCassandra().execute(
      s"INSERT INTO ${keyspace}.${tableName} (id, foo, bar) VALUES ('row2', 'world', 99) " +
        s"USING TIMESTAMP ${insertWritetimeMicros}"
    )

    successfullyPerformMigration("cassandra-to-parquet-timestamps.yaml")

    val parquetDir = parquetHostRoot.resolve("timestamps")
    assert(
      Files.exists(parquetDir) && Files.list(parquetDir).iterator().asScala.nonEmpty,
      "Parquet output directory should contain files"
    )

    successfullyPerformMigration("parquet-to-scylla-timestamps.yaml")

    targetScylla()
      .execute(
        s"SELECT id, foo, bar, TTL(foo) AS foo_ttl, WRITETIME(foo) AS foo_writetime " +
          s"FROM ${keyspace}.${tableName}"
      )
      .tap { resultSet =>
        val rows = resultSet.all().asScala.sortBy(_.getString("id"))
        assertEquals(rows.size, 2, "Expected 2 rows in target")

        val row1 = rows(0)
        assertEquals(row1.getString("id"), "row1")
        assertEquals(row1.getString("foo"), "hello")
        assertEquals(row1.getInt("bar"), 42)
        assertEquals(row1.getLong("foo_writetime"), insertWritetimeMicros)
        // TTL decreases over time, but should still be > 0 and close to our original value
        val row1Ttl = row1.getInt("foo_ttl")
        assert(row1Ttl > 0, s"TTL should be > 0, got ${row1Ttl}")
        assert(
          row1Ttl <= insertTtlSeconds,
          s"TTL should be <= ${insertTtlSeconds}, got ${row1Ttl}"
        )
        // Allow up to 5 minutes of TTL drift due to test execution time
        assert(
          row1Ttl > insertTtlSeconds - 300,
          s"TTL should be within 300s of ${insertTtlSeconds}, got ${row1Ttl}"
        )

        val row2 = rows(1)
        assertEquals(row2.getString("id"), "row2")
        assertEquals(row2.getString("foo"), "world")
        assertEquals(row2.getInt("bar"), 99)
        assertEquals(row2.getLong("foo_writetime"), insertWritetimeMicros)
      }
  }

  private def dropAndRecreate(database: CqlSession, name: String): Unit = {
    database.execute(SchemaBuilder.dropTable(keyspace, name).ifExists().build())
    database.execute(
      SchemaBuilder
        .createTable(keyspace, name)
        .withPartitionKey("id", DataTypes.TEXT)
        .withColumn("foo", DataTypes.TEXT)
        .withColumn("bar", DataTypes.INT)
        .build()
    )
  }

}
