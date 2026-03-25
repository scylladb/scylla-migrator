package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import scala.jdk.CollectionConverters._
import scala.util.chaining._

abstract class RenamedItemsTest(version: CassandraVersion) extends MigratorSuite(version.port) {

  private val configFile =
    CassandraVersion.configForSource("cassandra-to-scylla-renames.yaml", version)

  withTable("RenamedItems", renames = Map("bar" -> "quux"))
    .test(s"Cassandra ${version.label}: rename items along the migration") { tableName =>
      val insertStatement =
        QueryBuilder
          .insertInto(keyspace, tableName)
          .values(
            Map[String, Term](
              "id"  -> literal("12345"),
              "foo" -> literal("bar"),
              "bar" -> literal(42)
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
        assertEquals(row.getInt("quux"), 42)
      }
    }

}

@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra2RenamedItemsTest extends RenamedItemsTest(CassandraVersion.V2)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra3RenamedItemsTest extends RenamedItemsTest(CassandraVersion.V3)
class Cassandra4RenamedItemsTest extends RenamedItemsTest(CassandraVersion.V4)
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5RenamedItemsTest extends RenamedItemsTest(CassandraVersion.V5)
