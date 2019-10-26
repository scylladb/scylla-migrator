package com.scylladb.migrator

import cats.implicits._
import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken, Token }
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.yaml._
import io.circe.yaml.syntax._

case class MigratorConfig(source: SourceSettings,
                          target: TargetSettings,
                          preserveTimestamps: Boolean,
                          renames: List[Rename],
                          savepoints: Savepoints,
                          skipTokenRanges: Set[(Token[_], Token[_])]) {
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

case class Credentials(username: String, password: String)
object Credentials {
  implicit val encoder: Encoder[Credentials] = deriveEncoder
  implicit val decoder: Decoder[Credentials] = deriveDecoder
}

case class SourceSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          splitCount: Option[Int],
                          connections: Option[Int],
                          fetchSize: Int)
object SourceSettings {
  implicit val encoder: Encoder[SourceSettings] = deriveEncoder
  implicit val decoder: Decoder[SourceSettings] = deriveDecoder
}

case class TargetSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          connections: Option[Int])
object TargetSettings {
  implicit val encoder: Encoder[TargetSettings] = deriveEncoder
  implicit val decoder: Decoder[TargetSettings] = deriveDecoder
}

case class Rename(from: String, to: String)
object Rename {
  implicit val encoder: Encoder[Rename] = deriveEncoder
  implicit val decoder: Decoder[Rename] = deriveDecoder
}

case class Savepoints(intervalSeconds: Int, path: String)
object Savepoints {
  implicit val encoder: Encoder[Savepoints] = deriveEncoder
  implicit val decoder: Decoder[Savepoints] = deriveDecoder
}
