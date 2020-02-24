package com.scylladb.migrator

import cats.implicits._
import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken, Token }
import io.circe.generic.semiauto._
import io.circe._, io.circe.syntax._
import io.circe.yaml._, io.circe.yaml.syntax._

case class MigratorConfig(source: SourceSettings,
                          target: TargetSettings,
                          preserveTimestamps: Boolean,
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

case class Credentials(username: String, password: String)
object Credentials {
  implicit val encoder: Encoder[Credentials] = deriveEncoder[Credentials]
  implicit val decoder: Decoder[Credentials] = deriveDecoder[Credentials]
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
  implicit val encoder: Encoder[SourceSettings] = deriveEncoder[SourceSettings]
  implicit val decoder: Decoder[SourceSettings] = deriveDecoder[SourceSettings]
}

case class TargetSettings(host: String,
                          port: Int,
                          credentials: Option[Credentials],
                          keyspace: String,
                          table: String,
                          connections: Option[Int])
object TargetSettings {
  implicit val encoder: Encoder[TargetSettings] = deriveEncoder[TargetSettings]
  implicit val decoder: Decoder[TargetSettings] = deriveDecoder[TargetSettings]
}

case class Rename(from: String, to: String)
object Rename {
  implicit val encoder: Encoder[Rename] = deriveEncoder[Rename]
  implicit val decoder: Decoder[Rename] = deriveDecoder[Rename]
}

case class Savepoints(intervalSeconds: Int, path: String)
object Savepoints {
  implicit val encoder: Encoder[Savepoints] = deriveEncoder[Savepoints]
  implicit val decoder: Decoder[Savepoints] = deriveDecoder[Savepoints]
}

case class Validation(compareTimestamps: Boolean,
                      ttlToleranceMillis: Long,
                      writetimeToleranceMillis: Long,
                      failuresToFetch: Int,
                      floatingPointTolerance: Double)
object Validation {
  implicit val encoder: Encoder[Validation] = deriveEncoder[Validation]
  implicit val decoder: Decoder[Validation] = deriveDecoder[Validation]
}
