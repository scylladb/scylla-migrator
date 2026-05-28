package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils.{
  successfullyPerformMigration,
  successfullyPerformMigrationWithAutoResume
}
import com.scylladb.migrator.{
  SavepointCoordinates,
  SavepointRecord,
  SavepointStore,
  SavepointsResume,
  SparkUtils
}
import com.scylladb.migrator.config.{ MigratorConfig, TargetSettings }

import java.nio.file.Files
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetSavepointsIntegrationTest extends ParquetMigratorSuite {

  private val configFileName: String = "parquet-to-scylla-savepoints.yaml"
  private val targetConfigFileName: String = "parquet-to-scylla-target-savepoints.yaml"
  private val targetResumeConfigFileName: String =
    "parquet-to-scylla-target-savepoints-resume.yaml"
  private val targetSavepointsTable: String = "scylla_migrator_savepoints_target_test"
  private val targetSavepointsJobId: String = "parquet-target-savepoints-test"

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

  FunFixture
    .map2(
      withTable("targetsavepointstest"),
      withParquetDir("target-savepoints")
    )
    .test("Parquet target savepoints are written to Scylla and used for resume") {
      case (tableName, parquetRoot) =>
        targetScylla().execute(s"DROP TABLE IF EXISTS ${keyspace}.${targetSavepointsTable}")

        try {
          val parquetDir = parquetRoot.resolve("target-savepoints")
          Files.createDirectories(parquetDir)

          writeParquetTestFile(
            parquetDir.resolve("batch.parquet"),
            List(
              TestRecord("1", "alpha", 10),
              TestRecord("2", "beta", 20)
            )
          )

          val expectedProcessedFiles = listDataFiles(parquetDir).map(toContainerParquetUri)

          successfullyPerformMigrationWithAutoResume(targetConfigFileName)

          val rows = targetScylla()
            .execute(
              s"SELECT epoch_millis, sequence, schema_version, reason, migrator_version, config_sha256, config_yaml FROM ${keyspace}.${targetSavepointsTable} WHERE job_id = '${targetSavepointsJobId}'"
            )
            .all()
            .asScala

          assert(rows.nonEmpty, "target-backed savepoint row was not inserted")
          assert(
            rows.forall(_.getInt("schema_version") == 1),
            "all target-backed savepoints should use schema_version 1"
          )
          assert(
            rows.exists(_.getString("reason") == "completed"),
            "target-backed savepoints should include the completed terminal snapshot"
          )
          assert(
            rows.forall(row =>
              Option(row.getString("migrator_version")).exists(_.nonEmpty) &&
                Option(row.getString("config_sha256")).exists(_.matches("[0-9a-f]{64}"))
            ),
            "target-backed savepoints should include migrator version and config SHA-256 metadata"
          )

          val savedConfigs =
            rows.map(row => MigratorConfig.loadFromString(row.getString("config_yaml")))
          assert(
            savedConfigs.forall(
              _.target
                .asInstanceOf[TargetSettings.Scylla]
                .credentials
                .exists(_.password == "<redacted>")
            )
          )
          assert(
            savedConfigs.exists(_.skipParquetFiles.contains(expectedProcessedFiles)),
            s"no target savepoint contained expected processed files: ${expectedProcessedFiles}"
          )

          // The query must skip a corrupt newest row and lazily fall back to valid progress.
          val corruptInsert = targetScylla().prepare(
            s"INSERT INTO ${keyspace}.${targetSavepointsTable} (job_id, epoch_millis, sequence, config_sha256, config_yaml) VALUES (?, ?, ?, ?, ?)"
          )
          targetScylla().execute(
            corruptInsert.bind(
              targetSavepointsJobId,
              java.lang.Long.valueOf(rows.map(_.getLong("epoch_millis")).max + 1L),
              java.lang.Long.valueOf(1L),
              rows.head.getString("config_sha256"),
              "invalid: ["
            )
          )

          targetScylla().execute(s"TRUNCATE ${keyspace}.${tableName}")
          val resumed = successfullyPerformMigrationWithAutoResume(targetResumeConfigFileName)
          assertEquals(resumed.skipParquetFiles, Some(expectedProcessedFiles))
          val credentials = resumed.target
            .asInstanceOf[TargetSettings.Scylla]
            .credentials
          assertEquals(credentials.map(_.password), Some("dummy"))

          val rowCountAfterResume = targetScylla()
            .execute(s"SELECT id FROM ${keyspace}.${tableName}")
            .all()
            .size()

          assertEquals(
            rowCountAfterResume,
            0,
            "autoResume should load DB progress and skip the already processed Parquet file"
          )
          val fresh =
            successfullyPerformMigrationWithAutoResume(targetConfigFileName, autoResume = false)
          assertEquals(fresh.skipParquetFiles, None)
          assertEquals(
            targetScylla().execute(s"SELECT id FROM ${keyspace}.${tableName}").all().size(),
            2
          )
        } finally
          targetScylla().execute(s"DROP TABLE IF EXISTS ${keyspace}.${targetSavepointsTable}")
    }

  test("target savepoints inherit the exact mixed-case migration keyspace") {
    val mixedKeyspace = "TargetSavepointsCase"
    targetScylla().execute(
      s"CREATE KEYSPACE \"${mixedKeyspace}\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    try {
      val loaded = SparkUtils.remapForLocalExecution(
        MigratorConfig.loadFrom(s"src/test/configurations/${targetConfigFileName}")
      )
      val config = loaded.copy(target =
        loaded.target.asInstanceOf[TargetSettings.Scylla].copy(keyspace = mixedKeyspace)
      )
      SparkUtils.withSparkSession() { spark =>
        val store = SavepointStore.target(config, Some(spark.sparkContext))
        store.prepare()
        store.save(
          SavepointRecord(
            SavepointCoordinates(1000L, 1L),
            "test",
            SavepointStore.renderSavepoint(
              config.copy(skipParquetFiles = Some(Set("done.parquet")))
            )
          )
        )
        assertEquals(store.seedState(), Some(SavepointCoordinates(1000L, 1L)))
        val resumed = SavepointsResume.resume(config, None, None, Some(spark.sparkContext))
        assertEquals(resumed.skipParquetFiles, Some(Set("done.parquet")))
        assertEquals(resumed.target, config.target)
      }
    } finally targetScylla().execute(s"DROP KEYSPACE \"${mixedKeyspace}\"")
  }

}
