package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.SparkUtils.{ performValidation, successfullyPerformMigration }

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

abstract class ValidatorTest(version: CassandraVersion) extends MigratorSuite(version.port) {

  override val munitTimeout: Duration = 120.seconds

  private val configFile =
    CassandraVersion.configForSource("cassandra-to-scylla-basic.yaml", version)

  withTable("BasicTest").test(s"Cassandra ${version.label}: validate migration") { tableName =>
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

    successfullyPerformMigration(configFile)

    assertEquals(performValidation(configFile), 0, "Validation failed")

    val updateStatement =
      QueryBuilder
        .update(keyspace, tableName)
        .setColumn("foo", literal("baz"))
        .whereColumn("id")
        .isEqualTo(literal("12345"))
        .build()
    targetScylla().execute(updateStatement)

    assertEquals(performValidation(configFile), 1)
  }

}

class Cassandra2ValidatorTest extends ValidatorTest(CassandraVersion.V2)
class Cassandra3ValidatorTest extends ValidatorTest(CassandraVersion.V3)
class Cassandra4ValidatorTest extends ValidatorTest(CassandraVersion.V4)
class Cassandra5ValidatorTest extends ValidatorTest(CassandraVersion.V5)
