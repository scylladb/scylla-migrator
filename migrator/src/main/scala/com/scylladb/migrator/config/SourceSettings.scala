package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import software.amazon.awssdk.services.dynamodb.model.BillingMode

case class DynamoDBEndpoint(host: String, port: Int) {
  def renderEndpoint = s"${host}:${port}"
}

object DynamoDBEndpoint {
  implicit val encoder: Encoder[DynamoDBEndpoint] = deriveEncoder[DynamoDBEndpoint]
  implicit val decoder: Decoder[DynamoDBEndpoint] = deriveDecoder[DynamoDBEndpoint]
}

sealed trait SourceSettings
object SourceSettings {
  case class Cassandra(
    host: String,
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
    consistencyLevel: String
  ) extends SourceSettings

  /** @param streamingPollIntervalSeconds
    *   Interval between stream poll cycles (default: 5, min: 1).
    * @param streamingMaxConsecutiveErrors
    *   Max consecutive poll failures before stopping (default: 50).
    * @param streamingPollingPoolSize
    *   Thread pool size for parallel shard polling (default: max(4, CPUs)).
    * @param streamingLeaseDurationMs
    *   Duration of shard leases in milliseconds (default: 60000). Must be positive.
    * @param streamingMaxRecordsPerPoll
    *   Max records per GetRecords call per shard. Must be positive if set.
    * @param streamingMaxRecordsPerSecond
    *   Global rate limit for records processed per second. Must be positive if set.
    * @param streamingEnableCloudWatchMetrics
    *   Enable publishing metrics to CloudWatch (default: false).
    * @param streamApiCallTimeoutSeconds
    *   Overall timeout for DynamoDB Streams API calls (default: 30). Must be positive.
    * @param streamApiCallAttemptTimeoutSeconds
    *   Per-attempt timeout for Streams API calls (default: 10). Must be positive.
    * @param streamingPollFutureTimeoutSeconds
    *   Timeout for waiting on parallel poll futures (default: 60). Must be positive.
    */
  case class DynamoDB(
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    scanSegments: Option[Int],
    readThroughput: Option[Int],
    throughputReadPercent: Option[Float],
    maxMapTasks: Option[Int],
    removeConsumedCapacity: Option[Boolean] = None,
    streamingPollIntervalSeconds: Option[Int] = None,
    streamingMaxConsecutiveErrors: Option[Int] = None,
    streamingPollingPoolSize: Option[Int] = None,
    streamingLeaseDurationMs: Option[Long] = None,
    streamingMaxRecordsPerPoll: Option[Int] = None,
    streamingMaxRecordsPerSecond: Option[Int] = None,
    streamingEnableCloudWatchMetrics: Option[Boolean] = None,
    streamApiCallTimeoutSeconds: Option[Int] = None,
    streamApiCallAttemptTimeoutSeconds: Option[Int] = None,
    streamingPollFutureTimeoutSeconds: Option[Int] = None
  ) extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }
  case class Parquet(
    path: String,
    credentials: Option[AWSCredentials],
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String]
  ) extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  case class DynamoDBS3Export(
    bucket: String,
    manifestKey: String,
    tableDescription: DynamoDBS3Export.TableDescription,
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String],
    credentials: Option[AWSCredentials],
    usePathStyleAccess: Option[Boolean]
  ) extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  object DynamoDBS3Export {

    /** Model the required fields of the “TableCreationParameters” object from the AWS API.
      * @see
      *   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableCreationParameters.html
      */
    case class TableDescription(
      attributeDefinitions: Seq[AttributeDefinition],
      keySchema: Seq[KeySchema],
      billingMode: Option[BillingMode] = None
    )

    object TableDescription {
      import com.scylladb.migrator.config.BillingModeCodec._
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
