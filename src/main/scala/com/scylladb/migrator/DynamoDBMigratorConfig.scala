package com.scylladb.migrator

import cats.implicits._
import com.scylladb.migrator.config.AWSCredentials
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.yaml._
import io.circe.yaml.syntax._

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
