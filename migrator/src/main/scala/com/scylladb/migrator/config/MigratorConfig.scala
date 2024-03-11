package com.scylladb.migrator.config

import cats.implicits._
import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken, Token }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._
import io.circe.yaml.parser
import io.circe.yaml.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Error, Json }

case class MigratorConfig(source: SourceSettings,
                          target: TargetSettings,
                          renames: List[Rename],
                          savepoints: Savepoints,
                          skipTokenRanges: Set[(Token[_], Token[_])],
                          validation: Validation) {
  def render: String = this.asJson.asYaml.spaces2
}
object MigratorConfig {
  implicit val tokenEncoder: Encoder[Token[_]] = Encoder.instance {
    case LongToken(value)   => Json.obj("type" := "long", "value"   := value)
    case BigIntToken(value) => Json.obj("type" := "bigint", "value" := value)
  }

  implicit val tokenDecoder: Decoder[Token[_]] = Decoder.instance { cursor =>
    for {
      tpe <- cursor.get[String]("type").right
      result <- tpe match {
                 case "long"    => cursor.get[Long]("value").right.map(LongToken(_))
                 case "bigint"  => cursor.get[BigInt]("value").right.map(BigIntToken(_))
                 case otherwise => Left(DecodingFailure(s"Unknown token type '$otherwise'", Nil))
               }
    } yield result
  }

  implicit val migratorConfigDecoder: Decoder[MigratorConfig] = deriveDecoder[MigratorConfig]
  implicit val migratorConfigEncoder: Encoder[MigratorConfig] = deriveEncoder[MigratorConfig]

  def loadFrom(path: String): MigratorConfig = {
    val configData = scala.io.Source.fromFile(path).mkString

    parser
      .parse(configData)
      .leftWiden[Error]
      .flatMap(_.as[MigratorConfig])
      .valueOr(throw _)
  }
}
