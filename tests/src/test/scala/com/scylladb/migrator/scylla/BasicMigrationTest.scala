package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import scala.jdk.CollectionConverters._
import scala.util.chaining._

abstract class BasicMigrationTest(version: CassandraVersion) extends MigratorSuite(version.port) {

  private val configFile =
    CassandraVersion.configForSource("cassandra-to-scylla-basic.yaml", version)

  withTable("BasicTest").test(s"Cassandra ${version.label}: basic migration") { tableName =>
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

    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()
    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, 1)
      val row = rows.head
      assertEquals(row.getString("id"), "12345")
      assertEquals(row.getString("foo"), "bar")
    }
  }

}

@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra2BasicMigrationTest extends BasicMigrationTest(CassandraVersion.V2)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra3BasicMigrationTest extends BasicMigrationTest(CassandraVersion.V3)
class Cassandra4BasicMigrationTest extends BasicMigrationTest(CassandraVersion.V4)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5BasicMigrationTest extends BasicMigrationTest(CassandraVersion.V5)
