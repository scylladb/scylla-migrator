package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }
import org.junit.experimental.categories.Category

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

abstract class CopyMissingRowsTest(version: CassandraVersion) extends MigratorSuite(version.port) {

  override val munitTimeout: Duration = 120.seconds

  private val basicConfigFile =
    CassandraVersion.configForSource("cassandra-to-scylla-basic.yaml", version)
  private val copyMissingConfigFile =
    CassandraVersion.configForSource("cassandra-to-scylla-copy-missing-rows.yaml", version)

  withTable("BasicTest").test(
    s"Cassandra ${version.label}: copyMissingRows copies missing rows to target"
  ) { tableName =>
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

    // Validate again without timestamp comparison — the copied row has fresh timestamps
    assertEquals(
      performValidation(copyMissingConfigFile),
      0,
      "Row should have been copied to target"
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
