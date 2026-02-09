package com.scylladb.migrator.config

import io.circe._
import io.circe.generic.semiauto._
import io.circe.yaml.parser

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import scala.io.Source
import scala.util.{Try, Using}

/**
 * Main migrator configuration that supports MariaDB source.
 * 
 * This configuration class is designed to be compatible with the existing
 * scylla-migrator config format while adding MariaDB support.
 */
case class MigratorConfig(
  source: MigratorConfig.SourceConfig,
  target: TargetSettings.Scylla,
  renames: Option[Renames] = None,
  savepoints: Option[SavepointConfig] = None,
  skipTokenRanges: Option[List[TokenRange]] = None
)

object MigratorConfig {
  
  /**
   * Source configuration - discriminated union
   */
  sealed trait SourceConfig
  
  object SourceConfig {
    case class MariaDBSource(settings: SourceSettings.MariaDB) extends SourceConfig
    case class CassandraSource(settings: CassandraSourceSettings) extends SourceConfig
    // Add other source types as needed
    
    implicit val mariadbDecoder: Decoder[MariaDBSource] = 
      SourceSettings.MariaDB.decoder.map(MariaDBSource)
    
    // Discriminated decoder based on 'type' field
    implicit val decoder: Decoder[SourceConfig] = Decoder.instance { cursor =>
      cursor.downField("type").as[String].flatMap {
        case "mariadb" =>
          cursor.as[SourceSettings.MariaDB].map(MariaDBSource)
        case "cassandra" =>
          cursor.as[CassandraSourceSettings].map(CassandraSource)
        case other =>
          Left(DecodingFailure(s"Unknown source type: $other", cursor.history))
      }
    }
  }
  
  implicit val decoder: Decoder[MigratorConfig] = deriveDecoder[MigratorConfig]
  
  /**
   * Load configuration from a YAML file
   */
  def loadFrom(path: String): Either[String, MigratorConfig] = {
    Try {
      val file = new File(path)
      Using.resource(Source.fromFile(file, StandardCharsets.UTF_8.name())) { source =>
        source.mkString
      }
    }.toEither.left.map(_.getMessage).flatMap { content =>
      parser.parse(content)
        .left.map(_.getMessage)
        .flatMap(_.as[MigratorConfig].left.map(_.getMessage))
    }
  }
  
  /**
   * Load configuration from Spark config
   */
  def loadFromSpark(sparkConf: org.apache.spark.SparkConf): Either[String, MigratorConfig] = {
    sparkConf.getOption("spark.scylla.config") match {
      case Some(path) => loadFrom(path)
      case None => Left("spark.scylla.config not set")
    }
  }
}

/**
 * Savepoint configuration for resumable migrations
 */
case class SavepointConfig(
  path: String,
  intervalSeconds: Int = 300
)

object SavepointConfig {
  implicit val decoder: Decoder[SavepointConfig] = deriveDecoder[SavepointConfig]
  implicit val encoder: Encoder[SavepointConfig] = deriveEncoder[SavepointConfig]
}

/**
 * Token range for skip configuration
 */
case class TokenRange(
  start: Long,
  end: Long
)

object TokenRange {
  implicit val decoder: Decoder[TokenRange] = deriveDecoder[TokenRange]
  implicit val encoder: Encoder[TokenRange] = deriveEncoder[TokenRange]
}

/**
 * Placeholder for Cassandra source settings (existing in scylla-migrator)
 */
case class CassandraSourceSettings(
  host: String,
  port: Int = 9042,
  localDC: Option[String] = None,
  credentials: Option[TargetSettings.Credentials] = None,
  sslOptions: Option[TargetSettings.SSLOptions] = None,
  keyspace: String,
  table: String,
  splitCount: Int = 256,
  connections: Int = 8,
  fetchSize: Int = 1000,
  preserveTimestamps: Boolean = true,
  where: Option[String] = None
)

object CassandraSourceSettings {
  implicit val decoder: Decoder[CassandraSourceSettings] = deriveDecoder[CassandraSourceSettings]
  implicit val encoder: Encoder[CassandraSourceSettings] = deriveEncoder[CassandraSourceSettings]
}
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
                          renames: Option[List[Rename]],
                          savepoints: Savepoints,
                          skipTokenRanges: Option[Set[(Token[_], Token[_])]],
                          skipSegments: Option[Set[Int]],
                          skipParquetFiles: Option[Set[String]],
                          validation: Option[Validation]) {
  def render: String = this.asJson.asYaml.spaces2

  def getRenamesOrNil: List[Rename] = renames.getOrElse(Nil)

  /** The list of renames modelled as a Map from the old column name to the new column name */
  lazy val renamesMap: Map[String, String] =
    getRenamesOrNil.map(rename => rename.from -> rename.to).toMap.withDefault(identity)

  def getSkipTokenRangesOrEmptySet: Set[(Token[_], Token[_])] = skipTokenRanges.getOrElse(Set.empty)

  def getSkipParquetFilesOrEmptySet: Set[String] = skipParquetFiles.getOrElse(Set.empty)

}
object MigratorConfig {
  implicit val tokenEncoder: Encoder[Token[_]] = Encoder.instance {
    case LongToken(value)   => Json.obj("type" := "long", "value"   := value)
    case BigIntToken(value) => Json.obj("type" := "bigint", "value" := value)
  }

  implicit val tokenDecoder: Decoder[Token[_]] = Decoder.instance { cursor =>
    for {
      tpe <- cursor.get[String]("type")
      result <- tpe match {
                 case "long"    => cursor.get[Long]("value").map(LongToken(_))
                 case "bigint"  => cursor.get[BigInt]("value").map(BigIntToken(_))
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
