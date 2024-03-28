package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder

class BasicMigrationTest extends MigratorSuite {

  withTable("BasicTest").test("Read from source and write to target") { tableName =>
    val insertStatement =
      QueryBuilder
        .insertInto(keyspace, tableName)
        .json("""{ "id": "12345", "foo": "bar" }""")
        .build()

    // Insert some items
    sourceCassandra.execute(insertStatement)

    // Perform the migration
    submitSparkJob("cassandra-to-scylla-basic.yaml")

    // Check that the schema has been replicated to the target table
    val selectQuery =
      QueryBuilder.selectFrom(keyspace, tableName).all().build()
    val sourceColumns =
      sourceCassandra.execute(selectQuery).getColumnDefinitions
    val targetColumns =
      targetScylla.execute(selectQuery).getColumnDefinitions
    assertEquals(targetColumns, sourceColumns)

    // Check that the items have been migrated to the target table
    // TODO
  }

}
