package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.scylla.ParquetToScyllaBasicMigrationTest.BasicTestSchema
import org.apache.parquet.hadoop.ParquetFileWriter

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetToScyllaBasicMigrationTest extends munit.FunSuite {

  test("Basic migration from Parquet to ScyllaDB") {
    val keyspace = "test"
    val tableName = "BasicTest"

    val targetScylla: CqlSession = CqlSession
      .builder()
      .addContactPoint(new InetSocketAddress("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .withAuthCredentials("cassandra", "cassandra")
      .build()

    val keyspaceStatement =
      SchemaBuilder
        .createKeyspace(keyspace)
        .ifNotExists()
        .withReplicationOptions(Map[String, AnyRef](
          "class"              -> "SimpleStrategy",
          "replication_factor" -> new Integer(1)).asJava)
        .build()
    targetScylla.execute(keyspaceStatement)

    // Create the Parquet data source
    ParquetWriter.writeAndClose(
      "docker/parquet/basic.parquet",
      List(BasicTestSchema(id = "12345", foo = "bar")),
      ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE)
    )

    // Create the target table in the target database
    dropAndRecreateTable(targetScylla, keyspace, tableName, identity)

    // Perform the migration
    successfullyPerformMigration("parquet-to-scylla-basic.yaml")

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
    }

    // Clean the target table
    val dropTableQuery = SchemaBuilder.dropTable(keyspace, tableName).build()
    targetScylla.execute(dropTableQuery)
    // Close the database driver
    targetScylla.close()
  }

}

object ParquetToScyllaBasicMigrationTest {

  // parquet4s automatically derives the Parquet schema from this class definition.
  // It must be consistent with the definition of the table from `dropAndRecreateTable`.
  case class BasicTestSchema(id: String, foo: String)

}
