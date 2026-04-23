package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable
import com.scylladb.migrator.TestFileUtils
import org.apache.parquet.hadoop.ParquetFileWriter

import java.nio.file.{ Files, Path, Paths }
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
  override def withTable(
    name: String,
    renames: Map[String, String] = Map.empty
  ): FunFixture[String] =
    FunFixture(
      setup = { _ =>
        try
          // Only create table in target ScyllaDB, not in source
          dropAndRecreateTable(
            targetScylla(),
            keyspace,
            name,
            columnName = originalName => renames.getOrElse(originalName, originalName)
          )
        catch {
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

  def withParquetAndSavepoints(
    parquetDir: String,
    savepointsDir: String
  ): FunFixture[(Path, Path)] =
    FunFixture.map2(withParquetDir(parquetDir), withSavepointsDir(savepointsDir))

  def withTableAndSavepoints(
    tableName: String,
    parquetDir: String,
    savepointsDir: String
  ): FunFixture[(String, (Path, Path))] =
    FunFixture.map2(withTable(tableName), withParquetAndSavepoints(parquetDir, savepointsDir))

  def writeParquetTestFile(path: Path, data: List[TestRecord]): Unit =
    ParquetWriter.writeAndClose(
      path.toString,
      data,
      ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE)
    )

  /** Convert a host-side parquet path to the URI that Spark will use when reading/tracking it. */
  def toContainerParquetUri(path: Path): String =
    path.toAbsolutePath.toUri.toString

  def listDataFiles(root: Path): Set[Path] =
    Using.resource(Files.walk(root)) { stream =>
      stream
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path))
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .toSet
    }

  // Matches:
  //   new format: savepoint_<epochMillis>_<counter>.yaml
  //   legacy:     savepoint_<epochSeconds>.yaml
  private val savepointNamePattern =
    """^savepoint_(\d+)(?:_(\d+))?\.yaml$""".r

  /** Sort key for a savepoint file. Uses the filename timestamp + counter for files produced by
    * the current `SavepointsManager`, and falls back to the last-modified time for legacy files
    * written by older versions. The filename-based key is preferred because it is independent of
    * filesystem mtime granularity and immune to wall-clock adjustments.
    *
    * Defensive: an attacker who can write into the savepoints directory could drop a file whose
    * numeric component overflows `Long`. `parseLong` on such a string throws
    * `NumberFormatException`, which would crash the whole sort (denial-of-service on resume) if
    * left uncaught. Treat overflow as an unknown filename and fall through to mtime so a hostile
    * name cannot hijack or break the selection.
    */
  private def savepointSortKey(path: Path): (Long, Long) = {
    val name = path.getFileName.toString
    name match {
      case savepointNamePattern(head, tailOrNull) =>
        try
          if (tailOrNull == null) {
            // Legacy: `savepoint_<epochSeconds>.yaml`. Scale up to millis and treat counter as -1
            // so a legacy file always sorts before a new-format file with the same wall-clock
            // time.
            (java.lang.Math.multiplyExact(java.lang.Long.parseLong(head), 1000L), -1L)
          } else {
            (java.lang.Long.parseLong(head), java.lang.Long.parseLong(tailOrNull))
          }
        catch {
          case _: NumberFormatException | _: ArithmeticException =>
            (Files.getLastModifiedTime(path).toMillis, -1L)
        }
      case _ =>
        // Unknown format: fall back to mtime with a neutral counter.
        (Files.getLastModifiedTime(path).toMillis, -1L)
    }
  }

  def findLatestSavepoint(directory: Path): Option[Path] =
    if (!Files.exists(directory)) None
    else {
      val candidates =
        Using.resource(Files.list(directory)) { stream =>
          stream
            .iterator()
            .asScala
            .filter(path => Files.isRegularFile(path))
            .filter { path =>
              val name = path.getFileName.toString
              // Exclude temp files produced by the atomic-rename write path.
              name.startsWith("savepoint_") && name.endsWith(".yaml")
            }
            .toSeq
        }
      // Materialize the sort key once per file so the legacy-fallback branch does not issue an
      // `Files.getLastModifiedTime` syscall per comparison.
      candidates
        .map(p => savepointSortKey(p) -> p)
        .sortBy(_._1)
        .lastOption
        .map(_._2)
    }

  private def ensureEmptyDirectory(directory: Path): Unit = {
    if (Files.exists(directory)) {
      val children = directory.toFile.listFiles()
      if (children != null) children.foreach(TestFileUtils.deleteRecursive)
    }
    Files.createDirectories(directory)
  }

}
