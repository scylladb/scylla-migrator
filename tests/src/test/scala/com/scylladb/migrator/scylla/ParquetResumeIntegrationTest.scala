package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.MigratorConfig

import java.nio.file.Files
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetResumeIntegrationTest extends ParquetMigratorSuite {

  override val munitTimeout: Duration = 120.seconds

  private val resumeConfig: String = "parquet-to-scylla-resume.yaml"
  private val resumeAllProcessedConfig: String = "parquet-to-scylla-resume2.yaml"

  withTableAndSavepoints("resumetest", "resume", "parquet-resume-test").test(
    "Resume migration after interruption skips already processed files"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("resume")
    Files.createDirectories(parquetDir)

    // Phase 1: Create initial batch of files and migrate them
    val firstBatch = List(
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

    firstBatch.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    // Run first migration
    successfullyPerformMigration(resumeConfig)

    // Verify first batch was migrated
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val firstBatchRows = firstBatch.flatMap(_._2).map(row => row.id -> row).toMap

    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, firstBatchRows.size, "First batch should be fully migrated")
      rows.foreach { row =>
        val id = row.getString("id")
        val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
        assertEquals(migrated, firstBatchRows(id))
      }
    }

    // Load savepoint and verify it contains all first batch files
    val savepointAfterFirstRun = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created after first run"))

    val configAfterFirstRun = MigratorConfig.loadFrom(savepointAfterFirstRun.toString)
    val skipFilesAfterFirstRun = configAfterFirstRun.skipParquetFiles
      .getOrElse(fail("skipParquetFiles were not written"))

    val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)
    assertEquals(
      skipFilesAfterFirstRun,
      expectedProcessedFiles,
      "Savepoint should contain all processed files from first batch"
    )

    // Phase 2: Create second batch of files (simulating new data)
    val secondBatch = List(
      parquetDir.resolve("batch-4.parquet") -> List(
        TestRecord("6", "zeta", 60),
        TestRecord("7", "eta", 70)
      ),
      parquetDir.resolve("batch-5.parquet") -> List(
        TestRecord("8", "theta", 80)
      )
    )

    secondBatch.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    // Phase 3: Run migration again (should skip first batch, process only second batch)
    successfullyPerformMigration(resumeConfig)

    // Verify all data is present (first + second batch)
    val allExpectedRows = (firstBatch ++ secondBatch)
      .flatMap(_._2)
      .map(row => row.id -> row)
      .toMap

    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, allExpectedRows.size, "All data from both batches should be present")
      rows.foreach { row =>
        val id = row.getString("id")
        val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
        assertEquals(migrated, allExpectedRows(id))
      }
    }

    // Verify final savepoint includes all files
    val savepointAfterSecondRun = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created after second run"))

    val configAfterSecondRun = MigratorConfig.loadFrom(savepointAfterSecondRun.toString)
    val skipFilesAfterSecondRun = configAfterSecondRun.skipParquetFiles
      .getOrElse(fail("skipParquetFiles were not written"))

    val allProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)
    assertEquals(
      skipFilesAfterSecondRun,
      allProcessedFiles,
      "Final savepoint should contain all files from both batches"
    )

    // Verify count matches expectation
    assertEquals(allProcessedFiles.size, 5, "Should have 5 total parquet files")
  }

  withTableAndSavepoints("resumetest2", "resume2", "parquet-resume-test2").test(
    "Resume when all files already processed performs no work"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("resume2")
    Files.createDirectories(parquetDir)

    val parquetBatches = List(
      parquetDir.resolve("file-1.parquet") -> List(
        TestRecord("1", "test1", 100)
      ),
      parquetDir.resolve("file-2.parquet") -> List(
        TestRecord("2", "test2", 200)
      )
    )

    parquetBatches.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    // First run: migrate all files
    successfullyPerformMigration(resumeAllProcessedConfig)

    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val initialRowCount = targetScylla().execute(selectAllStatement).all().size()
    assertEquals(initialRowCount, 2, "Initial migration should have 2 rows")

    // Second run: should detect all files already processed and do nothing
    successfullyPerformMigration(resumeAllProcessedConfig)

    // Verify data unchanged (no duplicates)
    val finalRowCount = targetScylla().execute(selectAllStatement).all().size()
    assertEquals(finalRowCount, 2, "Should still have exactly 2 rows (no duplicates)")

    // Verify savepoint still tracks all files
    val finalSavepoint = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint should exist"))

    val finalConfig = MigratorConfig.loadFrom(finalSavepoint.toString)
    val finalSkipFiles = finalConfig.skipParquetFiles.getOrElse(Set.empty)

    assertEquals(finalSkipFiles.size, 2, "Should still skip 2 files")
  }
}
