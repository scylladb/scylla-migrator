package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import java.nio.file.Path
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetToScyllaBasicMigrationTest extends ParquetMigratorSuite {

  FunFixture.map2(withTable("BasicTest"), withParquetDir("basic")).test("Basic migration from Parquet to ScyllaDB") { case (tableName, parquetDir) =>
    writeParquetTestFile(
      parquetDir.resolve("basic.parquet"),
      List(TestRecord(id = "12345", foo = "bar", bar = 0))
    )

    successfullyPerformMigration("parquet-to-scylla-basic.yaml")

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