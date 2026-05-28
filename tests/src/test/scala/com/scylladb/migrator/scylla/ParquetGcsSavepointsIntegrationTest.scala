package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.scylladb.migrator.SparkUtils
import com.scylladb.migrator.config.MigratorConfig
import org.apache.hadoop.fs.{ Path => HadoopPath }

import java.nio.file.Files
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.chaining._

class ParquetGcsSavepointsIntegrationTest extends ParquetMigratorSuite {

  override val munitTimeout: FiniteDuration = 2.minutes

  private val configFileName: String = "parquet-to-scylla-gcs-savepoints.yaml"
  private val savepointsUri: String = "gs://migrator-savepoints/parquet-gcs-savepoints"

  FunFixture
    .map2(withTable("gcssavepointstest"), withParquetDir("gcs-savepoints"))
    .test(
      "Parquet savepoints can be written to a local GCS-compatible bucket"
    ) { case (tableName, parquetRoot) =>
      val hadoopConf = SparkUtils.configureLocalGcs()
      val savepointsPath = new HadoopPath(savepointsUri)
      val savepointsFs = savepointsPath.getFileSystem(hadoopConf)

      if (savepointsFs.exists(savepointsPath)) savepointsFs.delete(savepointsPath, true)

      try {
        val parquetDir = parquetRoot.resolve("gcs-savepoints")
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

        SparkUtils.successfullyPerformMigration(configFileName)

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

        val savepointFiles = savepointsFs
          .listStatus(savepointsPath)
          .map(_.getPath)
          .filter(path => path.getName.startsWith("savepoint_") && path.getName.endsWith(".yaml"))

        val latestSavepoint = savepointFiles
          .sortBy(_.getName)
          .lastOption
          .getOrElse(fail("Savepoint file was not created"))

        val savepointConfig = MigratorConfig.loadFrom(latestSavepoint.toString, hadoopConf)
        val skipFiles =
          savepointConfig.skipParquetFiles.getOrElse(fail("skipParquetFiles were not written"))

        assertEquals(skipFiles, expectedProcessedFiles)
      } finally
        if (savepointsFs.exists(savepointsPath)) savepointsFs.delete(savepointsPath, true)
    }
}
