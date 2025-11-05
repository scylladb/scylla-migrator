package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
import org.apache.parquet.hadoop.ParquetFileWriter

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Using

abstract class ParquetMigratorSuite extends MigratorSuite(sourcePort = 0) {

  // parquet4s automatically derives the Parquet schema from this case class definition.
  // This definition must remain consistent with the table schema created by dropAndRecreateTable,
  // which has columns: id TEXT, foo TEXT, bar INT.
  case class TestRecord(id: String, foo: String, bar: Int)

  protected val parquetHostRoot: Path = Paths.get("docker/parquet")
  protected val savepointsHostRoot: Path = Paths.get("docker/spark-master")

  override def munitFixtures: Seq[Fixture[_]] = Seq(targetScylla)

  // Override withTable to only create table in target (no source Cassandra for Parquet tests)
  override def withTable(name: String, renames: Map[String, String] = Map.empty): FunFixture[String] =
    FunFixture(
      setup = { _ =>
        try {
          // Only create table in target ScyllaDB, not in source
          dropAndRecreateTable(
            targetScylla(),
            keyspace,
            name,
            columnName = originalName => renames.getOrElse(originalName, originalName))
        } catch {
          case any: Throwable =>
            fail(s"Something did not work as expected", any)
        }
        name
      },
      teardown = { _ =>
        val dropTableQuery = SchemaBuilder.dropTable(keyspace, name).build()
        targetScylla().execute(dropTableQuery)
        ()
      }
    )

  def withParquetDir(parquetDir: String): FunFixture[Path] =
    FunFixture(
      setup = { _ =>
        ensureEmptyDirectory(parquetHostRoot)
        parquetHostRoot
      },
      teardown = { directory =>
        ensureEmptyDirectory(directory)
        ()
      }
    )

  def withSavepointsDir(savepointsDir: String): FunFixture[Path] =
    FunFixture(
      setup = { _ =>
        val directory = savepointsHostRoot.resolve(savepointsDir)
        ensureEmptyDirectory(directory)
        directory
      },
      teardown = { directory =>
        ensureEmptyDirectory(directory)
        ()
      }
    )

  def withParquetAndSavepoints(parquetDir: String, savepointsDir: String): FunFixture[(Path, Path)] =
    FunFixture.map2(withParquetDir(parquetDir), withSavepointsDir(savepointsDir))

  def withTableAndSavepoints(tableName: String, parquetDir: String, savepointsDir: String): FunFixture[(String, (Path, Path))] =
    FunFixture.map2(withTable(tableName), withParquetAndSavepoints(parquetDir, savepointsDir))

  def writeParquetTestFile(path: Path, data: List[TestRecord]): Unit = {
    ParquetWriter.writeAndClose(
      path.toString,
      data,
      ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE)
    )
  }

  def toContainerParquetUri(path: Path): String = {
    require(path.startsWith(parquetHostRoot), s"Unexpected parquet file location: $path")
    val relative = parquetHostRoot.relativize(path)
    Paths.get("/app/parquet").resolve(relative).toUri.toString
  }

  def listDataFiles(root: Path): Set[Path] =
    Using.resource(Files.walk(root)) { stream =>
      stream.iterator().asScala
        .filter(path => Files.isRegularFile(path))
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .toSet
    }

  def findLatestSavepoint(directory: Path): Option[Path] =
    if (!Files.exists(directory)) None
    else
      Using.resource(Files.list(directory)) { stream =>
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path))
          .filter(_.getFileName.toString.startsWith("savepoint_"))
          .toSeq
      }.sortBy(path => Files.getLastModifiedTime(path).toMillis)
        .lastOption

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

}
