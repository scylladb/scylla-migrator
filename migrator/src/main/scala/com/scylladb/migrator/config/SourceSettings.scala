package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class DynamoDBEndpoint(host: String, port: Int) {
  def renderEndpoint = s"${host}:${port}"
}

object DynamoDBEndpoint {
  implicit val encoder: Encoder[DynamoDBEndpoint] = deriveEncoder[DynamoDBEndpoint]
  implicit val decoder: Decoder[DynamoDBEndpoint] = deriveDecoder[DynamoDBEndpoint]
}

sealed trait SourceSettings
object SourceSettings {
  case class Cassandra(host: String,
                       port: Int,
                       localDC: Option[String],
                       credentials: Option[Credentials],
                       sslOptions: Option[SSLOptions],
                       keyspace: String,
                       table: String,
                       splitCount: Option[Int],
                       connections: Option[Int],
                       fetchSize: Int,
                       preserveTimestamps: Boolean,
                       where: Option[String],
                       consistencyLevel: String)
      extends SourceSettings
  case class DynamoDB(endpoint: Option[DynamoDBEndpoint],
                      region: Option[String],
                      credentials: Option[AWSCredentials],
                      table: String,
                      scanSegments: Option[Int],
                      readThroughput: Option[Int],
                      throughputReadPercent: Option[Float],
                      maxMapTasks: Option[Int])
      extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }
  case class Parquet(path: String, credentials: Option[AWSCredentials]) extends SourceSettings

  case class DynamoDBS3Export(bucket: String,
                              manifestKey: String,
                              tableDescription: DynamoDBS3Export.TableDescription,
                              endpoint: Option[DynamoDBEndpoint],
                              region: Option[String],
                              credentials: Option[AWSCredentials],
                              usePathStyleAccess: Option[Boolean])
      extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  object DynamoDBS3Export {

    /** Model the required fields of the “TableCreationParameters” object from the AWS API.
      * @see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableCreationParameters.html
      */
    case class TableDescription(
      attributeDefinitions: Seq[AttributeDefinition],
      keySchema: Seq[KeySchema]
    )

    object TableDescription {
      implicit val decoder: Decoder[TableDescription] = deriveDecoder[TableDescription]
      implicit val encoder: Encoder[TableDescription] = deriveEncoder[TableDescription]
    }

    case class AttributeDefinition(name: String, `type`: AttributeType)

    object AttributeDefinition {
      implicit val decoder: Decoder[AttributeDefinition] = deriveDecoder[AttributeDefinition]
      implicit val encoder: Encoder[AttributeDefinition] = deriveEncoder[AttributeDefinition]
    }

    case class KeySchema(name: String, `type`: KeyType)

    object KeySchema {
      implicit val decoder: Decoder[KeySchema] = deriveDecoder[KeySchema]
      implicit val encoder: Encoder[KeySchema] = deriveEncoder[KeySchema]
    }

    sealed trait AttributeType
    object AttributeType {
      case object S extends AttributeType
      case object N extends AttributeType
      case object B extends AttributeType
      implicit val decoder: Decoder[AttributeType] =
        Decoder.decodeString.emap {
          case "S" => Right(S)
          case "N" => Right(N)
          case "B" => Right(B)
          case t   => Left(s"Unknown attribute type ${t}")
        }
      implicit val encoder: Encoder[AttributeType] = Encoder.instance {
        case S => Json.fromString("S")
        case N => Json.fromString("N")
        case B => Json.fromString("B")
      }
    }
    sealed trait KeyType
    object KeyType {
      case object Hash extends KeyType
      case object Range extends KeyType
      implicit val decoder: Decoder[KeyType] =
        Decoder.decodeString.emap {
          case "HASH"  => Right(Hash)
          case "RANGE" => Right(Range)
          case t       => Left(s"Unknown key type ${t}")
        }
      implicit val encoder: Encoder[KeyType] = Encoder.instance {
        case Hash  => Json.fromString("HASH")
        case Range => Json.fromString("RANGE")
      }
    }
  }

  implicit val decoder: Decoder[SourceSettings] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "cassandra" | "scylla" =>
        deriveDecoder[Cassandra].apply(cursor)
      case "parquet" =>
        deriveDecoder[Parquet].apply(cursor)
      case "dynamo" | "dynamodb" =>
        deriveDecoder[DynamoDB].apply(cursor)
      case "dynamodb-s3-export" =>
        deriveDecoder[DynamoDBS3Export].apply(cursor)
      case otherwise =>
        Left(DecodingFailure(s"Unknown source type: ${otherwise}", cursor.history))
    }
  }

  implicit val encoder: Encoder[SourceSettings] = Encoder.instance {
    case s: Cassandra =>
      deriveEncoder[Cassandra]
        .encodeObject(s)
        .add("type", Json.fromString("cassandra"))
        .asJson
    case s: DynamoDB =>
      deriveEncoder[DynamoDB]
        .encodeObject(s)
        .add("type", Json.fromString("dynamodb"))
        .asJson
    case s: Parquet =>
      deriveEncoder[Parquet]
        .encodeObject(s)
        .add("type", Json.fromString("parquet"))
        .asJson
    case s: DynamoDBS3Export =>
      deriveEncoder[DynamoDBS3Export]
        .encodeObject(s)
        .add("type", Json.fromString("dynamodb-s3-export"))
        .asJson
  }
}
