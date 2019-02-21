package com.scylladb.migrator

import cats.implicits._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import io.circe._, io.circe.syntax._, io.circe.generic.auto._
import io.circe.yaml._, io.circe.yaml.syntax._

case class MigratorConfig(source: SourceSettings,
                          target: TargetSettings,
                          preserveTimestamps: Boolean,
                          renames: List[Rename],
                          savepoints: Savepoints,
                          skipTokenRanges: Set[(Long, Long)]) {
  def render: String = this.asJson.asYaml.spaces2
}

object MigratorConfig {
  def loadFrom(path: String): MigratorConfig = {
    val configData = scala.io.Source.fromFile(path).mkString

    parser
      .parse(configData)
      .leftWiden[Error]
      .flatMap(_.as[MigratorConfig])
      .valueOr(throw _)
  }
}

case class Credentials(username: String, password: String)

case class SourceSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          splitCount: Option[Int],
                          connections: Option[Int],
                          fetchSize: Int)

case class TargetSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          connections: Option[Int])

case class Rename(from: String, to: String)

case class Savepoints(intervalSeconds: Int, path: String)
