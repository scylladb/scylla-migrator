package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.scylladb.migrator.Integration
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/** Integration test for migrating tables with counter columns.
  *
  * Verifies that:
  *   - Counter tables migrate successfully even with preserveTimestamps=true (timestamps are
  *     auto-disabled for counters)
  *   - Null counter values are handled without errors
  *   - Non-null counter values are preserved correctly
  */
@Category(Array(classOf[Integration]))
class CounterTableMigrationTest extends MigratorSuite(9043) {

  private val configFile = "cassandra-to-scylla-counters.yaml"
  private val tableName = "countertest"

  private def createCounterTable(session: com.datastax.oss.driver.api.core.CqlSession): Unit = {
    session.execute(SchemaBuilder.dropTable(keyspace, tableName).ifExists().build())
    session.execute(
      SchemaBuilder
        .createTable(keyspace, tableName)
        .withPartitionKey("id", DataTypes.TEXT)
        .withClusteringColumn("category", DataTypes.TEXT)
        .withColumn("counter_1", DataTypes.COUNTER)
        .withColumn("counter_2", DataTypes.COUNTER)
        .build()
    )
  }

  val counterTable: FunFixture[String] = FunFixture(
    setup = { _ =>
      createCounterTable(sourceCassandra())
      createCounterTable(targetScylla())
      tableName
    },
    teardown = { _ =>
      val drop = SchemaBuilder.dropTable(keyspace, tableName).build()
      targetScylla().execute(drop)
      sourceCassandra().execute(drop)
      ()
    }
  )

  counterTable.test("Counter table migration with mixed null and non-null values") { _ =>
    // Insert counter values using UPDATE (the only way to modify counters)
    // Row 1: both counters set
    sourceCassandra().execute(
      s"UPDATE $keyspace.$tableName SET counter_1 = counter_1 + 10, counter_2 = counter_2 + 20 WHERE id = 'row1' AND category = 'a'"
    )

    // Row 2: only counter_1 set (counter_2 remains null)
    sourceCassandra().execute(
      s"UPDATE $keyspace.$tableName SET counter_1 = counter_1 + 5 WHERE id = 'row2' AND category = 'b'"
    )

    successfullyPerformMigration(configFile)

    val rows = targetScylla()
      .execute(
        QueryBuilder.selectFrom(keyspace, tableName).all().build()
      )
      .all()
      .asScala
      .sortBy(_.getString("id"))

    // Row 1: both counters migrated
    assertEquals(rows.size, 2)
    val row1 = rows(0)
    assertEquals(row1.getString("id"), "row1")
    assertEquals(row1.getLong("counter_1"), 10L)
    assertEquals(row1.getLong("counter_2"), 20L)

    // Row 2: counter_1 migrated, counter_2 should be 0 (null counter replaced with 0)
    val row2 = rows(1)
    assertEquals(row2.getString("id"), "row2")
    assertEquals(row2.getLong("counter_1"), 5L)
    assertEquals(row2.getLong("counter_2"), 0L)
  }
}
