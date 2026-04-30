package com.scylladb.migrator.config

import java.nio.file.{ Files, Path, Paths }
import scala.jdk.CollectionConverters._
import scala.util.Try

class MigratorConfigFixtureParsingTest extends munit.FunSuite {

  private def listYamlFiles(dir: Path): Seq[Path] = {
    val stream = Files.list(dir)
    try
      stream
        .iterator()
        .asScala
        .toSeq
        .filter(_.toString.endsWith(".yaml"))
        .sortBy(_.getFileName.toString)
    finally
      stream.close()
  }

  private def parseFailure(file: Path): Option[String] =
    Try(MigratorConfig.loadFrom(file.toString)).toEither.left.toOption.map { error =>
      val path = file.toString
      s"${path}: ${error.getMessage}"
    }

  test("parses all test fixture configurations") {
    val configDir = Seq(
      Paths.get("src", "test", "configurations"),
      Paths.get("tests", "src", "test", "configurations")
    ).find(path => Files.exists(path))
      .getOrElse(
        fail("Could not find test configuration directory")
      )
    val configurationFiles = listYamlFiles(configDir)
    val failures = configurationFiles.flatMap(parseFailure)
    assert(
      failures.isEmpty,
      s"Expected all test configurations to parse, but failed on: ${failures.mkString(", ")}"
    )
  }

  private def resolveExistingPath(candidates: Seq[Path], description: String): Path =
    candidates.find(path => Files.exists(path)).getOrElse(fail(s"Expected $description"))

  test("parses curated shipped example configurations") {
    val exampleFiles = Seq(
      resolveExistingPath(
        Seq(
          Paths.get("ansible", "files", "config.dynamodb.yml"),
          Paths.get("..", "ansible", "files", "config.dynamodb.yml")
        ),
        "ansible/files/config.dynamodb.yml"
      ),
      resolveExistingPath(
        Seq(
          Paths.get(
            "docs",
            "source",
            "tutorials",
            "dynamodb-to-scylladb-alternator",
            "spark-data",
            "config.yaml"
          ),
          Paths.get(
            "..",
            "docs",
            "source",
            "tutorials",
            "dynamodb-to-scylladb-alternator",
            "spark-data",
            "config.yaml"
          )
        ),
        "docs/source/tutorials/dynamodb-to-scylladb-alternator/spark-data/config.yaml"
      )
    )

    val failures = exampleFiles.sortBy(_.toString).flatMap(parseFailure)

    assert(
      failures.isEmpty,
      s"Expected shipped example configs to parse, but failed on: ${failures.mkString(", ")}"
    )
  }
}
