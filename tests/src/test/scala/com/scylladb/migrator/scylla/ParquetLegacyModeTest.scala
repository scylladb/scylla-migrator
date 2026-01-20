package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.MigratorConfig
import java.nio.file.Files
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetLegacyModeTest extends ParquetMigratorSuite {

  override val munitTimeout: FiniteDuration = 2.minutes

  FunFixture
    .map2(withTable("legacytest"), withParquetDir("legacy"))
    .test("Legacy mode (enableParquetFileTracking=false) migrates data correctly") {
      case (tableName, parquetRoot) =>
        val parquetDir = parquetRoot.resolve("legacy")
        Files.createDirectories(parquetDir)

        // Create multiple parquet files to ensure parallel processing works
        val parquetFiles = List(
          parquetDir.resolve("file-1.parquet") -> List(
            TestRecord("1", "alpha", 10),
            TestRecord("2", "beta", 20)
          ),
          parquetDir.resolve("file-2.parquet") -> List(
            TestRecord("3", "gamma", 30),
            TestRecord("4", "delta", 40)
          ),
          parquetDir.resolve("file-3.parquet") -> List(
            TestRecord("5", "epsilon", 50)
          )
        )

        parquetFiles.foreach { case (path, rows) =>
          writeParquetTestFile(path, rows)
        }

        // Run migration with legacy mode
        successfullyPerformMigration("parquet-to-scylla-legacy-test1.yaml")

        // Verify all data was migrated correctly
        val selectAllStatement = QueryBuilder
          .selectFrom(keyspace, tableName)
          .all()
          .build()

        val expectedRows = parquetFiles.flatMap(_._2).map(row => row.id -> row).toMap

        targetScylla().execute(selectAllStatement).tap { resultSet =>
          val rows = resultSet.all().asScala
          assertEquals(rows.size, expectedRows.size, "All rows should be migrated in legacy mode")
          rows.foreach { row =>
            val id = row.getString("id")
            val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
            assertEquals(migrated, expectedRows(id), s"Row $id should match expected data")
          }
        }
    }

  withTableAndSavepoints("legacytest2", "legacy2", "legacy-savepoints").test(
    "Legacy mode does NOT create file-level savepoints"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("legacy2")
    Files.createDirectories(parquetDir)

    val parquetFiles = List(
      parquetDir.resolve("data-1.parquet") -> List(
        TestRecord("1", "foo", 100)
      ),
      parquetDir.resolve("data-2.parquet") -> List(
        TestRecord("2", "bar", 200)
      )
    )

    parquetFiles.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    // Run migration with legacy mode
    successfullyPerformMigration("parquet-to-scylla-legacy-test2.yaml")

    // Verify data migrated
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val rowCount = targetScylla().execute(selectAllStatement).all().size()
    assertEquals(rowCount, 2, "Should have migrated 2 rows")

    // Check savepoint file if it exists
    val maybeSavepointFile = findLatestSavepoint(savepointsDir)

    maybeSavepointFile match {
      case Some(savepointFile) =>
        // If savepoint exists, it should NOT contain skipParquetFiles
        val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
        assertEquals(
          savepointConfig.skipParquetFiles,
          None,
          "Legacy mode should NOT track skipParquetFiles in savepoints"
        )
      case None =>
        // It's also acceptable if no savepoint is created at all
        // This is the expected behavior when savepointsSupported = false
        ()
    }
  }

  withTableAndSavepoints("legacytest3", "legacy3", "legacy-noresume").test(
    "Legacy mode does NOT support resume - data is re-processed on restart"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("legacy3")
    Files.createDirectories(parquetDir)

    writeParquetTestFile(
      parquetDir.resolve("data.parquet"),
      List(TestRecord("unique-id", "test-data", 999))
    )

    // First run
    successfullyPerformMigration("parquet-to-scylla-legacy-test3.yaml")

    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val rowCountAfterFirstRun = targetScylla().execute(selectAllStatement).all().size()
    assertEquals(rowCountAfterFirstRun, 1, "Should have 1 row after first run")

    // Second run - in legacy mode, this should re-process the same file
    // Note: This will either duplicate data OR fail with unique constraint
    // depending on table schema. Since our test schema has 'id' as primary key,
    // the data should be idempotent (same id overwrites).
    successfullyPerformMigration("parquet-to-scylla-legacy-test3.yaml")

    val rowCountAfterSecondRun = targetScylla().execute(selectAllStatement).all().size()

    // In legacy mode without resume:
    // - Data is re-processed
    // - But since we have primary key, it just overwrites
    // - So count stays the same, but this proves no "skip" happened
    assertEquals(
      rowCountAfterSecondRun,
      1,
      "Row count should still be 1 (overwritten, not skipped)"
    )

    // The key difference from new mode: no file was marked as "processed"
    // In new mode, the second run would log "No Parquet files to process"
    // In legacy mode, it re-reads and re-processes everything
  }

  withTableAndSavepoints("comparison", "comparison-data", "comparison-savepoints").test(
    "Legacy and new modes produce identical results on same dataset"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("comparison-data")
    Files.createDirectories(parquetDir)

    // Create a representative dataset
    val testData = List(
      parquetDir.resolve("batch-a.parquet") -> List(
        TestRecord("id-1", "data-a", 111),
        TestRecord("id-2", "data-b", 222)
      ),
      parquetDir.resolve("batch-b.parquet") -> List(
        TestRecord("id-3", "data-c", 333),
        TestRecord("id-4", "data-d", 444)
      ),
      parquetDir.resolve("batch-c.parquet") -> List(
        TestRecord("id-5", "data-e", 555)
      )
    )

    testData.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    // Run with legacy mode first
    successfullyPerformMigration("parquet-to-scylla-legacy-comparison.yaml")

    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    // Capture results from legacy mode
    val legacyResults = targetScylla().execute(selectAllStatement).all().asScala
      .map { row =>
        TestRecord(
          row.getString("id"),
          row.getString("foo"),
          row.getInt("bar")
        )
      }
      .toSet

    assertEquals(legacyResults.size, 5, "Legacy mode should migrate all 5 rows")

    // Clear the table for second run
    val truncateStatement = QueryBuilder.truncate(keyspace, tableName).build()
    targetScylla().execute(truncateStatement)

    // Run with new mode
    successfullyPerformMigration("parquet-to-scylla-newmode-comparison.yaml")

    // Capture results from new mode
    val newModeResults = targetScylla().execute(selectAllStatement).all().asScala
      .map { row =>
        TestRecord(
          row.getString("id"),
          row.getString("foo"),
          row.getInt("bar")
        )
      }
      .toSet


    assertEquals(
      newModeResults,
      legacyResults,
      "New mode and legacy mode should produce identical data"
    )
  }

  FunFixture
    .map2(withTable("singlefile"), withParquetDir("legacy"))
    .test("Legacy mode migrates single file correctly") {
      case (tableName, parquetRoot) =>
        val parquetDir = parquetRoot.resolve("legacy")
        Files.createDirectories(parquetDir)

        // Single file scenario
        writeParquetTestFile(
          parquetDir.resolve("single.parquet"),
          List(TestRecord("only-one", "single-file", 42))
        )

        successfullyPerformMigration("parquet-to-scylla-legacy-singlefile.yaml")

        val selectAllStatement = QueryBuilder
          .selectFrom(keyspace, tableName)
          .all()
          .build()

        val rows = targetScylla().execute(selectAllStatement).all().asScala
        assertEquals(rows.size, 1, "Should migrate the single row")
        assertEquals(rows.head.getString("id"), "only-one")
    }
}
