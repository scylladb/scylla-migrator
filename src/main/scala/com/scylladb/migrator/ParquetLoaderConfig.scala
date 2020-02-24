package com.scylladb.migrator

import cats.implicits._
import io.circe.generic.semiauto._
import io.circe._, io.circe.syntax._
import io.circe.yaml._, io.circe.yaml.syntax._

case class ParquetSourceSettings(path: String, credentials: Option[AWSCredentials])
object ParquetSourceSettings {
  implicit val decoder: Decoder[ParquetSourceSettings] = deriveDecoder
  implicit val encoder: Encoder[ParquetSourceSettings] = deriveEncoder
}
case class ParquetLoaderConfig(source: ParquetSourceSettings,
                               target: TargetSettings,
                               renames: List[Rename])

object ParquetLoaderConfig {
  implicit val decoder: Decoder[ParquetLoaderConfig] = deriveDecoder
  implicit val encoder: Encoder[ParquetLoaderConfig] = deriveEncoder

  def loadFrom(path: String): ParquetLoaderConfig = {
    val configData = scala.io.Source.fromFile(path)

    try parser
      .parse(configData.mkString)
      .leftWiden[Error]
      .flatMap(_.as[ParquetLoaderConfig])
      .valueOr(throw _)
    finally configData.close
  }
}
