package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import java.nio.file.Files
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetParallelModeTest extends ParquetMigratorSuite {

  private val configFileName: String = "parquet-to-scylla-parallel.yaml"

  FunFixture
    .map2(withTable("paralleltest"), withParquetDir("parallel"))
    .test("Parallel mode migration with multiple Parquet files") {
      case (tableName, parquetRoot) =>
        val parquetDir = parquetRoot.resolve("parallel")
        Files.createDirectories(parquetDir)

        // Create multiple parquet files to test parallel processing
        val parquetBatches = List(
          parquetDir.resolve("batch-1.parquet") -> List(
            TestRecord("1", "alpha", 10),
            TestRecord("2", "beta", 20)
          ),
          parquetDir.resolve("batch-2.parquet") -> List(
            TestRecord("3", "gamma", 30),
            TestRecord("4", "delta", 40)
          ),
          parquetDir.resolve("batch-3.parquet") -> List(
            TestRecord("5", "epsilon", 50)
          )
        )

        parquetBatches.foreach { case (path, rows) =>
          writeParquetTestFile(path, rows)
        }

        // Run migration with parallel mode
        successfullyPerformMigration(configFileName)

        // Verify all data was migrated
        val selectAllStatement = QueryBuilder
          .selectFrom(keyspace, tableName)
          .all()
          .build()

        val expectedRows = parquetBatches.flatMap(_._2).map(row => row.id -> row).toMap

        targetScylla().execute(selectAllStatement).tap { resultSet =>
          val rows = resultSet.all().asScala
          assertEquals(rows.size, expectedRows.size)
          rows.foreach { row =>
            val id = row.getString("id")
            val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
            assertEquals(migrated, expectedRows(id))
          }
        }
    }
}
