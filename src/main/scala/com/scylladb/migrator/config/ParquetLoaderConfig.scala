package com.scylladb.migrator.config

import cats.implicits._
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.yaml.parser
import io.circe.{ Decoder, Encoder, Error }

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
