package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.MigratorConfig

import java.nio.file.Files
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetSavepointsIntegrationTest extends ParquetMigratorSuite {

  private val configFileName: String = "parquet-to-scylla-savepoints.yaml"

  withTableAndSavepoints("savepointstest", "savepoints", "parquet-savepoints-test").test(
    "Parquet savepoints include all processed files"
  ) { case (tableName, (parquetRoot, savepointsDir)) =>

    val parquetDir = parquetRoot.resolve("savepoints")
    Files.createDirectories(parquetDir)

    val parquetBatches = List(
      parquetDir.resolve("batch-1.parquet") -> List(
        TestRecord("1", "alpha", 10),
        TestRecord("2", "beta", 20)
      ),
      parquetDir.resolve("batch-2.parquet") -> List(
        TestRecord("3", "gamma", 30)
      )
    )

    parquetBatches.foreach { case (path, rows) =>
      writeParquetTestFile(path, rows)
    }

    val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)

    successfullyPerformMigration(configFileName)

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

    val savepointFile = findLatestSavepoint(savepointsDir)
      .getOrElse(fail("Savepoint file was not created"))

    val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
    val skipFiles =
      savepointConfig.skipParquetFiles.getOrElse(fail("skipParquetFiles were not written"))

    assertEquals(skipFiles, expectedProcessedFiles)
  }

}
