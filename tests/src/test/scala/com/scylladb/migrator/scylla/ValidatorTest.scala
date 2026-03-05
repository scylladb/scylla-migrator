package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

class ValidatorTest extends MigratorSuite(sourcePort = 9043) {

  override val munitTimeout: Duration = 120.seconds

  withTable("BasicTest").test("Validate migration") { tableName =>
    val configFile = "cassandra-to-scylla-basic.yaml"

    // Insert some items
    val insertStatement =
      QueryBuilder
        .insertInto(keyspace, tableName)
        .values(
          Map[String, Term](
            "id"  -> literal("12345"),
            "foo" -> literal("bar")
          ).asJava
        )
        .build()
    sourceCassandra().execute(insertStatement)

    // Perform the migration
    successfullyPerformMigration(configFile)

    // Perform the validation
    assertEquals(performValidation(configFile), 0, "Validation failed")

    // Change the value of an item
    val updateStatement =
      QueryBuilder
        .update(keyspace, tableName)
        .setColumn("foo", literal("baz"))
        .whereColumn("id")
        .isEqualTo(literal("12345"))
        .build()
    targetScylla().execute(updateStatement)

    // Check that the validation failed because of the introduced change
    assertEquals(performValidation(configFile), 1)

  }

}
