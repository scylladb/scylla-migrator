package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.MigratorConfig
import java.nio.file.Files
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetParallelModeTest extends ParquetMigratorSuite {

  private val configFileName: String = "parquet-to-scylla-parallel.yaml"
  private val savepointsConfigFileName: String = "parquet-to-scylla-parallel-savepoints.yaml"

  override val munitTimeout: FiniteDuration = 2.minutes

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

  withTableAndSavepoints("paralleltest2", "parallel-savepoints", "parallel-savepoints-test").test(
    "Parallel mode tracks all files in savepoints correctly"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("parallel-savepoints")
    Files.createDirectories(parquetDir)

    // Create multiple parquet files to test parallel processing with savepoints
    val parquetBatches = List(
      parquetDir.resolve("file-1.parquet") -> List(
        TestRecord("1", "alpha", 10),
        TestRecord("2", "beta", 20)
      ),
      parquetDir.resolve("file-2.parquet") -> List(
        TestRecord("3", "gamma", 30),
        TestRecord("4", "delta", 40)
      ),
      parquetDir.resolve("file-3.parquet") -> List(
        TestRecord("5", "epsilon", 50),
        TestRecord("6", "zeta", 60)
      ),
      parquetDir.resolve("file-4.parquet") -> List(
        TestRecord("7", "eta", 70)
      )
    )

    parquetBatches.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)

    // Run migration with parallel mode and savepoints
    successfullyPerformMigration(savepointsConfigFileName)

    // Verify all data was migrated correctly
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val expectedRows = parquetBatches.flatMap(_._2).map(row => row.id -> row).toMap

    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, expectedRows.size, "All rows should be migrated")
      rows.foreach { row =>
        val id = row.getString("id")
        val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
        assertEquals(migrated, expectedRows(id))
      }
    }

    // Verify savepoint was created
    val savepointFile = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created in parallel mode"))

    // Verify savepoint contains all processed files
    val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
    val skipFiles = savepointConfig.skipParquetFiles
      .getOrElse(fail("skipParquetFiles were not written in parallel mode"))

    assertEquals(
      skipFiles,
      expectedProcessedFiles,
      "Savepoint should contain all processed files in parallel mode"
    )

    assertEquals(skipFiles.size, 4, "Should have tracked 4 parquet files")

    // Verify idempotency: run migration again, should skip all files
    val rowCountBefore = targetScylla().execute(selectAllStatement).all().size()

    successfullyPerformMigration(savepointsConfigFileName)

    val rowCountAfter = targetScylla().execute(selectAllStatement).all().size()

    assertEquals(
      rowCountBefore,
      rowCountAfter,
      "Second run should not insert duplicate data (idempotency check)"
    )
  }
}
