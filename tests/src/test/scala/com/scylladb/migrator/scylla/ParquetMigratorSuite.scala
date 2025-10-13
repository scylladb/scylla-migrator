package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
import org.apache.parquet.hadoop.ParquetFileWriter

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Using

abstract class ParquetMigratorSuite extends MigratorSuite(sourcePort = 0) {

  case class TestRecord(id: String, foo: String, bar: Int)

  protected val parquetHostRoot: Path = Paths.get("docker/parquet")
  protected val savepointsHostRoot: Path = Paths.get("docker/spark-master")

  override def munitFixtures: Seq[Fixture[_]] = Seq(targetScylla)

  def withParquetDir(parquetDir: String): FunFixture[Path] =
    FunFixture(
      setup = { _ =>
        val directory = parquetHostRoot.resolve(parquetDir)
        ensureEmptyDirectory(directory)
        directory
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
        .filter(_.getFileName.toString.startsWith("part-"))
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