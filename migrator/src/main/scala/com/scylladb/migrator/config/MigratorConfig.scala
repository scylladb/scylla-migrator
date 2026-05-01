package com.scylladb.migrator.config

import cats.implicits._
import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken, Token }
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.syntax._
import io.circe.yaml.parser
import io.circe.yaml.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Error, Json, JsonObject }
import scala.util.Using

case class MigratorConfig(
  source: SourceSettings,
  target: TargetSettings,
  renames: Option[List[Rename]],
  savepoints: Savepoints = Savepoints.Default,
  skipTokenRanges: Option[Set[(Token[_], Token[_])]],
  skipSegments: Option[Set[Int]],
  skipParquetFiles: Option[Set[String]],
  validation: Option[Validation]
) {
  def render: String = this.asJson.asYaml.spaces2
  def renderRedacted: String = MigratorConfig.redactSecrets(this.asJson).asYaml.spaces2
  override def toString: String = renderRedacted

  def getRenamesOrNil: List[Rename] = renames.getOrElse(Nil)

  /** The list of renames modelled as a Map from the old column name to the new column name */
  lazy val renamesMap: Map[String, String] =
    getRenamesOrNil.map(rename => rename.from -> rename.to).toMap.withDefault(identity)

  def getSkipTokenRangesOrEmptySet: Set[(Token[_], Token[_])] = skipTokenRanges.getOrElse(Set.empty)

  def getSkipParquetFilesOrEmptySet: Set[String] = skipParquetFiles.getOrElse(Set.empty)

}
object MigratorConfig {
  private val RedactedValue = "<redacted>"
  implicit val config: Configuration = Configuration.default.withDefaults

  implicit val tokenEncoder: Encoder[Token[_]] = Encoder.instance {
    case LongToken(value)   => Json.obj("type" := "long", "value" := value)
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

  implicit val migratorConfigDecoder: Decoder[MigratorConfig] =
    Decoder.instance { cursor =>
      deriveConfiguredDecoder[MigratorConfig].apply(cursor).flatMap { decoded =>
        val savepointsProvided = cursor.downField("savepoints").success.isDefined
        val savepointsRequired = decoded.source match {
          case _: SourceSettings.MySQL => false
          case _                       => true
        }

        if (!savepointsProvided && savepointsRequired)
          Left(
            DecodingFailure(
              "Missing required field: savepoints. This field is optional only for MySQL migrations.",
              cursor.history
            )
          )
        else {
          (decoded.source, decoded.target) match {
            case (_: SourceSettings.Alternator, t: TargetSettings.DynamoDBLike)
                if t.streamChanges.isEnabled =>
              Left(
                DecodingFailure(
                  "'streamChanges' streaming replication is not supported when the source is an Alternator table. " +
                    "Scylla Alternator does not support DynamoDB Streams or AWS-side Kinesis publishing from the source.",
                  cursor.history
                )
              )
            case (_: SourceSettings.Alternator, _: TargetSettings.DynamoDBS3Export) =>
              Left(
                DecodingFailure(
                  "A source of type 'alternator' is not supported with target type 'dynamodb-s3-export'. " +
                    "DynamoDB S3 export output is only supported when the source is AWS DynamoDB.",
                  cursor.history
                )
              )
            case (s: SourceSettings.DynamoDBS3Export, t: TargetSettings.DynamoDBLike) =>
              t.streamChanges match {
                case StreamChangesSetting.DynamoDBStreams =>
                  Left(
                    DecodingFailure(
                      "'streamChanges' with DynamoDB Streams is not supported when the source is a DynamoDB S3 export. " +
                        "Use Kinesis Data Streams (`type: kinesis` and `streamArn`) together with `source.streamSource` " +
                        "for the live DynamoDB table instead. See docs/source/stream-changes.rst.",
                      cursor.history
                    )
                  )
                case _: StreamChangesSetting.KinesisDataStreams if s.streamSource.isEmpty =>
                  Left(
                    DecodingFailure(
                      "When the source is dynamodb-s3-export and target streamChanges uses Kinesis, " +
                        "`source.streamSource` must be set to the live DynamoDB table whose changes should be replayed. " +
                        "See docs/source/stream-changes.rst.",
                      cursor.history
                    )
                  )
                case _ =>
                  Right(decoded)
              }
            case _ => Right(decoded)
          }
        }
      }
    }
  implicit val migratorConfigEncoder: Encoder[MigratorConfig] =
    Encoder.instance { migratorConfig =>
      val savepointsField = migratorConfig.source match {
        case _: SourceSettings.MySQL => Nil
        case _                       => List("savepoints" -> migratorConfig.savepoints.asJson)
      }

      Json.obj(
        (
          List(
            "source"  -> migratorConfig.source.asJson,
            "target"  -> migratorConfig.target.asJson,
            "renames" -> migratorConfig.renames.asJson
          ) ++ savepointsField ++ List(
            "skipTokenRanges"  -> migratorConfig.skipTokenRanges.asJson,
            "skipSegments"     -> migratorConfig.skipSegments.asJson,
            "skipParquetFiles" -> migratorConfig.skipParquetFiles.asJson,
            "validation"       -> migratorConfig.validation.asJson
          )
        ): _*
      )
    }

  private def shouldRedactValue(key: String, value: Json): Boolean =
    value.isString && (SensitiveKeys
      .isSensitiveKey(key) || key == "where")

  private[config] def redactSecrets(json: Json): Json =
    json.arrayOrObject(
      json,
      arr => Json.fromValues(arr.map(redactSecrets)),
      obj =>
        Json.fromJsonObject(
          obj.toIterable.foldLeft(JsonObject.empty) { case (acc, (key, value)) =>
            val updatedValue =
              if (shouldRedactValue(key, value))
                Json.fromString(RedactedValue)
              else
                redactSecrets(value)
            acc.add(key, updatedValue)
          }
        )
    )

  def loadFrom(path: String): MigratorConfig = {
    val configData = Using.resource(scala.io.Source.fromFile(path))(_.mkString)

    parser
      .parse(configData)
      .leftWiden[Error]
      .flatMap(_.as[MigratorConfig])
      .valueOr(throw _)
  }
}
