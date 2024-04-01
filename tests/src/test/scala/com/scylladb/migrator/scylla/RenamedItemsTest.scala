package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term

import scala.jdk.CollectionConverters._
import scala.util.chaining._

class RenamedItemsTest extends MigratorSuite {

  withTable("RenamedItems", renames = Map("bar" -> "quux")).test("Read from source and write to target") { tableName =>
    val insertStatement =
      QueryBuilder
        .insertInto(keyspace, tableName)
        .values(Map[String, Term](
          "id" -> literal("12345"),
          "foo" -> literal("bar"),
          "bar" -> literal(42)
        ).asJava)
        .build()

    // Insert some items
    sourceCassandra.execute(insertStatement)

    // Perform the migration
    submitSparkJob("cassandra-to-scylla-renames.yaml")

    // Check that the item has been migrated to the target table
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()
    targetScylla.execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, 1)
      val row = rows.head
      assertEquals(row.getString("id"), "12345")
      assertEquals(row.getString("foo"), "bar")
      assertEquals(row.getInt("quux"), 42)
    }
  }

}
