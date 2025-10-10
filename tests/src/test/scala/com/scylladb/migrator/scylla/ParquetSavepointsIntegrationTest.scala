import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.{ QueryBuilder, SchemaBuilder }
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.MigratorConfig
import com.scylladb.migrator.scylla.ParquetSavepointsIntegrationTest.SavepointsTestSchema
import org.apache.parquet.hadoop.ParquetFileWriter

import java.net.InetSocketAddress
import java.nio.file.{ Files, Path, Paths }
import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.Using

class ParquetSavepointsIntegrationTest extends munit.FunSuite {

  private val parquetHostRoot: Path = Paths.get("docker/parquet")
  private val parquetTestDirectory: Path = parquetHostRoot.resolve("savepoints")
  private val savepointsHostRoot: Path = Paths.get("docker/spark-master")
  private val savepointsTestDirectory: Path = savepointsHostRoot.resolve("parquet-savepoints-test")
  private val configFileName: String = "parquet-to-scylla-savepoints.yaml"

  test("Parquet savepoints include all processed files") {
    val keyspace = "test"
    val tableName = "savepointstest"

    ensureEmptyDirectory(parquetTestDirectory)
    ensureEmptyDirectory(savepointsTestDirectory)

    val targetScylla: CqlSession = CqlSession
      .builder()
      .addContactPoint(new InetSocketAddress("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .withAuthCredentials("dummy", "dummy")
      .build()

    try {
      val keyspaceStatement =
        SchemaBuilder
          .createKeyspace(keyspace)
          .ifNotExists()
          .withReplicationOptions(Map[String, AnyRef](
            "class"              -> "SimpleStrategy",
            "replication_factor" -> new Integer(1)).asJava)
          .build()

      targetScylla.execute(keyspaceStatement)

      dropAndRecreateTable(targetScylla, keyspace, tableName, identity)

      val parquetBatches = List(
        parquetTestDirectory.resolve("batch-1.parquet") -> List(
          SavepointsTestSchema("1", "alpha", 10),
          SavepointsTestSchema("2", "beta", 20)
        ),
        parquetTestDirectory.resolve("batch-2.parquet") -> List(
          SavepointsTestSchema("3", "gamma", 30)
        )
      )

      parquetBatches.foreach { case (path, rows) =>
        ParquetWriter.writeAndClose(
          path.toString,
          rows,
          ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE)
        )
      }

      val expectedProcessedFiles = listDataFiles(parquetTestDirectory).map(toContainerParquetUri)

      successfullyPerformMigration(configFileName)

      val selectAllStatement = QueryBuilder
        .selectFrom(keyspace, tableName)
        .all()
        .build()

      val expectedRows = parquetBatches.flatMap(_._2).map(row => row.id -> row).toMap

      targetScylla.execute(selectAllStatement).tap { resultSet =>
        val rows = resultSet.all().asScala
        assertEquals(rows.size, expectedRows.size)
        rows.foreach { row =>
          val id = row.getString("id")
          val migrated = SavepointsTestSchema(id, row.getString("foo"), row.getInt("bar"))
          assertEquals(migrated, expectedRows(id))
        }
      }

      val savepointFile = findLatestSavepoint(savepointsTestDirectory)
        .getOrElse(fail("Savepoint file was not created"))

      val savepointConfig = MigratorConfig.loadFrom(savepointFile.toString)
      val skipFiles = savepointConfig.skipParquetFiles.getOrElse(fail("skipParquetFiles were not written"))

      assertEquals(skipFiles, expectedProcessedFiles)
    } finally {
      val dropTableQuery = SchemaBuilder.dropTable(keyspace, tableName).build()
      targetScylla.execute(dropTableQuery)
      targetScylla.close()

      ensureEmptyDirectory(parquetTestDirectory)
      ensureEmptyDirectory(savepointsTestDirectory)
    }
  }

  private def ensureEmptyDirectory(directory: Path): Unit = {
    if (Files.exists(directory)) {
      Using.resource(Files.list(directory)) { stream =>
        stream.iterator().asScala.foreach(deleteRecursively)
      }
    }
    Files.createDirectories(directory)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      Using.resource(Files.list(path)) { stream =>
        stream.iterator().asScala.foreach(deleteRecursively)
      }
    }
    Files.deleteIfExists(path)
  }

  private def listDataFiles(root: Path): Set[Path] =
    Using.resource(Files.walk(root)) { stream =>
      stream.iterator().asScala
        .filter(path => Files.isRegularFile(path))
        .filter(_.getFileName.toString.startsWith("part-"))
        .toSet
    }

  private def toContainerParquetUri(path: Path): String = {
    require(path.startsWith(parquetHostRoot), s"Unexpected parquet file location: $path")
    val relative = parquetHostRoot.relativize(path)
    Paths.get("/app/parquet").resolve(relative).toUri.toString
  }

  private def findLatestSavepoint(directory: Path): Option[Path] =
    if (!Files.exists(directory)) None
    else
      Using.resource(Files.list(directory)) { stream =>
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path))
          .filter(_.getFileName.toString.startsWith("savepoint_"))
          .toSeq
      }.sortBy(path => Files.getLastModifiedTime(path).toMillis)
        .lastOption
}

object ParquetSavepointsIntegrationTest {
  case class SavepointsTestSchema(id: String, foo: String, bar: Int)
}