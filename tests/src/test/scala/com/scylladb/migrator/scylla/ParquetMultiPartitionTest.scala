package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.config.MigratorConfig

import java.nio.file.Files
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.sys.process.Process
import scala.util.chaining._

class ParquetMultiPartitionTest extends ParquetMigratorSuite {

  private val configFileName: String = "parquet-to-scylla-multipartition.yaml"
  private val configFileName2: String = "parquet-to-scylla-multipartition2.yaml"

  override val munitTimeout: FiniteDuration = 2.minutes

  /**
   * Run migration with custom Spark configuration to force file splitting.
   * This sets spark.sql.files.maxPartitionBytes to a very small value (64KB)
   * to ensure even small files get split into multiple partitions.
   */
  private def performMigrationWithSmallPartitions(
      configFile: String = configFileName
  ): Unit = {
    Process(
      Seq(
        "docker",
        "compose",
        "-f",
        "../docker-compose-tests.yml",
        "exec",
        "spark-master",
        "/spark/bin/spark-submit",
        "--class",
        "com.scylladb.migrator.Migrator",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.driver.host=spark-master",
        "--conf",
        s"spark.scylla.config=/app/configurations/${configFile}",
        "--conf",
        "spark.sql.files.maxPartitionBytes=65536", // 64KB - forces multiple partitions
        "--conf",
        "spark.sql.files.openCostInBytes=4096", // 4KB - small open cost
        "--executor-cores", "2",
        "--executor-memory", "4G",
        "/jars/scylla-migrator-assembly.jar"
      )
    ).run()
      .exitValue()
      .ensuring(statusCode => statusCode == 0, "Spark job with small partitions failed")
    ()
  }

  withTableAndSavepoints("multipartitiontest", "multipartition", "parquet-multipartition-test").test(
    "Large parquet file split into multiple partitions is tracked correctly"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("multipartition")
    Files.createDirectories(parquetDir)

    // Create a SINGLE large parquet file with many rows
    // Each row has relatively large string to ensure file size > 64KB
    val largeFilePath = parquetDir.resolve("large-file.parquet")
    val largeData = (1 to 500).map { i =>
      // Each record is ~200 bytes, total ~100KB file
      TestRecord(
        id = f"id-$i%05d",
        foo = s"data-$i-" + ("x" * 150), // Large string to increase file size
        bar = i * 10
      )
    }.toList

    writeParquetTestFile(largeFilePath, largeData)

    val fileSizeBytes = Files.size(largeFilePath)
    // With 500 rows and ~200 bytes each, file should be ~100KB
    assert(fileSizeBytes > 65536, s"File size $fileSizeBytes should be > 64KB to force splitting")

    val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)
    assertEquals(expectedProcessedFiles.size, 1, "Should have exactly 1 parquet file")

    // Run migration with small partition size to force splitting
    performMigrationWithSmallPartitions()

    // Verify all data was migrated correctly
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val expectedRows = largeData.map(row => row.id -> row).toMap

    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, expectedRows.size, "All 500 rows should be migrated")
      rows.foreach { row =>
        val id = row.getString("id")
        val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
        assertEquals(migrated, expectedRows(id))
      }
    }

    // Verify savepoint was created and contains the file
    val savepointFile = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created"))

    val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
    val skipFiles = savepointConfig.skipParquetFiles
      .getOrElse(fail("skipParquetFiles were not written"))

    assertEquals(
      skipFiles,
      expectedProcessedFiles,
      "Savepoint should contain the large file after all its partitions completed"
    )

    // Verify idempotency: running again should skip the file
    val rowCountBefore = targetScylla().execute(selectAllStatement).all().size()

    performMigrationWithSmallPartitions()

    val rowCountAfter = targetScylla().execute(selectAllStatement).all().size()

    assertEquals(
      rowCountBefore,
      rowCountAfter,
      "Second run should not duplicate data (file should be skipped)"
    )
  }

  withTableAndSavepoints("multipartitiontest2", "multipartition2", "parquet-multipartition-test2").test(
    "Mix of single-partition and multi-partition files tracked correctly"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("multipartition2")
    Files.createDirectories(parquetDir)

    // Create one LARGE file (will be split) and two SMALL files (won't be split)
    val files = List(
      parquetDir.resolve("small-1.parquet") -> List(
        TestRecord("small-1", "data", 100)
      ),
      parquetDir.resolve("large.parquet") -> (1 to 500).map { i =>
        TestRecord(
          id = f"large-$i%05d",
          foo = s"data-$i-" + ("x" * 150),
          bar = i * 10
        )
      }.toList,
      parquetDir.resolve("small-2.parquet") -> List(
        TestRecord("small-2", "data", 200)
      )
    )

    files.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    val largeFileSize = Files.size(files(1)._1)
    assert(largeFileSize > 65536, s"Large file should be > 64KB, got $largeFileSize")

    val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)
    assertEquals(expectedProcessedFiles.size, 3, "Should have 3 parquet files total")

    // Run migration
    performMigrationWithSmallPartitions(configFileName2)

    // Verify all data migrated
    val selectAllStatement = QueryBuilder
      .selectFrom(keyspace, tableName)
      .all()
      .build()

    val expectedRows = files.flatMap(_._2).map(row => row.id -> row).toMap

    targetScylla().execute(selectAllStatement).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, expectedRows.size, "All rows from all files should be migrated")
      rows.foreach { row =>
        val id = row.getString("id")
        val migrated = TestRecord(id, row.getString("foo"), row.getInt("bar"))
        assertEquals(migrated, expectedRows(id))
      }
    }

    // Verify savepoint contains all 3 files
    val savepointFile = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created"))

    val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
    val skipFiles = savepointConfig.skipParquetFiles
      .getOrElse(fail("skipParquetFiles were not written"))

    assertEquals(
      skipFiles.size,
      3,
      "Savepoint should contain all 3 files (both single and multi-partition)"
    )

    assertEquals(
      skipFiles,
      expectedProcessedFiles,
      "All processed files should be in savepoint"
    )
  }
}
