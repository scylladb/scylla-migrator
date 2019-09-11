package com.scylladb.migrator

import cats.implicits._
import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import io.circe._, io.circe.syntax._, io.circe.generic.auto._
import io.circe.yaml._, io.circe.yaml.syntax._

case class DynamoDBMigratorConfig(source: DynamoDBSourceSettings, target: DynamoDBTargetSettings) {
  def render: String = this.asJson.asYaml.spaces2
}

object DynamoDBMigratorConfig {
  def loadFrom(path: String): DynamoDBMigratorConfig = {
    val configData = scala.io.Source.fromFile(path).mkString

    parser
      .parse(configData)
      .leftWiden[Error]
      .flatMap(_.as[DynamoDBMigratorConfig])
      .valueOr(throw _)
  }
}

case class AWSCredentials(accessKey: String, secretKey: String)

case class DynamoDBSourceSettings(hostURL: Option[String],
                                  region: String,
                                  port: Option[Int],
                                  credentials: Option[AWSCredentials],
                                  table: String,
                                  scan_segments: Option[Int],
                                  read_throughput: Option[Int],
                                  throughput_read_percent: Option[Float],
                                  max_map_tasks: Option[Int])

case class DynamoDBTargetSettings(hostURL: String,
                                  region: Option[String],
                                  port: Int,
                                  credentials: Option[AWSCredentials],
                                  table: Option[String],
                                  scan_segments: Option[Int],
                                  write_throughput: Option[Int],
                                  throughput_write_percent: Option[Float],
                                  max_map_tasks: Option[Int])
