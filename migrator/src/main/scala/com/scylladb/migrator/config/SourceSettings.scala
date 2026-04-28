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
    consistencyLevel: String,
    cloud: Option[CloudConfig] = None
  ) extends SourceSettings

  /** When `cloud` is set, the connector will reach the cluster through the Astra SNI proxy
    * described by the secure-connect bundle. In that mode the `host` and `port` fields are
    * meaningless (the bundle carries the contact points), so we make them optional in YAML and fall
    * back to inert sentinels in the case class. The decoder rejects any combination that would
    * silently ignore user input.
    */
  object Cassandra {
    private val SentinelHost: String = ""
    private val SentinelPort: Int = 0

    implicit val decoder: Decoder[Cassandra] = Decoder.instance { c =>
      val hasHost = c.get[Option[String]]("host").exists(_.isDefined)
      val hasPort = c.get[Option[Int]]("port").exists(_.isDefined)
      for {
        cloud <- c.get[Option[CloudConfig]]("cloud")
        _ <- (cloud.isDefined, hasHost, hasPort) match {
               case (true, true, _) =>
                 Left(
                   DecodingFailure(
                     "Cassandra source: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (true, _, true) =>
                 Left(
                   DecodingFailure(
                     "Cassandra source: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (false, true, true) => Right(())
               case (false, _, _) =>
                 Left(
                   DecodingFailure(
                     "Cassandra source: 'host' and 'port' are required unless 'cloud' is set.",
                     c.history
                   )
                 )
               case (true, false, false) => Right(())
             }
        host               <- c.getOrElse[String]("host")(SentinelHost)
        port               <- c.getOrElse[Int]("port")(SentinelPort)
        localDC            <- c.get[Option[String]]("localDC")
        credentials        <- c.get[Option[Credentials]]("credentials")
        sslOptions         <- c.get[Option[SSLOptions]]("sslOptions")
        keyspace           <- c.get[String]("keyspace")
        table              <- c.get[String]("table")
        splitCount         <- c.get[Option[Int]]("splitCount")
        connections        <- c.get[Option[Int]]("connections")
        fetchSize          <- c.get[Int]("fetchSize")
        preserveTimestamps <- c.get[Boolean]("preserveTimestamps")
        where              <- c.get[Option[String]]("where")
        consistencyLevel   <- c.get[String]("consistencyLevel")
        _ <- if (cloud.isDefined && localDC.isDefined)
               Left(
                 DecodingFailure(
                   "Cassandra source: 'localDC' must not be set when using 'cloud' (the bundle " +
                     "carries the local DC).",
                   c.history
                 )
               )
             else Right(())
        _ <-
          if (cloud.isDefined && sslOptions.isDefined)
            Left(
              DecodingFailure(
                "Cassandra source: 'sslOptions' must not be set when using 'cloud' (the bundle " +
                  "carries the TLS material).",
                c.history
              )
            )
          else Right(())
      } yield Cassandra(
        host,
        port,
        localDC,
        credentials,
        sslOptions,
        keyspace,
        table,
        splitCount,
        connections,
        fetchSize,
        preserveTimestamps,
        where,
        consistencyLevel,
        cloud
      )
    }

    implicit val encoder: Encoder.AsObject[Cassandra] = Encoder.AsObject.instance { c =>
      val common = io.circe.JsonObject(
        "localDC"            -> c.localDC.asJson,
        "credentials"        -> c.credentials.asJson,
        "sslOptions"         -> c.sslOptions.asJson,
        "keyspace"           -> c.keyspace.asJson,
        "table"              -> c.table.asJson,
        "splitCount"         -> c.splitCount.asJson,
        "connections"        -> c.connections.asJson,
        "fetchSize"          -> c.fetchSize.asJson,
        "preserveTimestamps" -> c.preserveTimestamps.asJson,
        "where"              -> c.where.asJson,
        "consistencyLevel"   -> c.consistencyLevel.asJson
      )
      c.cloud match {
        case Some(cloud) =>
          common.add("cloud", cloud.asJson)
        case None =>
          common.add("host", c.host.asJson).add("port", c.port.asJson)
      }
    }
  }
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
    alternator: Option[AlternatorSettings] = None
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
      billingMode: Option[BillingMode] = None,
      provisionedThroughput: Option[ProvisionedThroughputConfig] = None
    )

    object TableDescription {
      import com.scylladb.migrator.config.BillingModeCodec._
      implicit val decoder: Decoder[TableDescription] = deriveDecoder[TableDescription]
      implicit val encoder: Encoder[TableDescription] = deriveEncoder[TableDescription]
    }

    case class ProvisionedThroughputConfig(
      readCapacityUnits: Long,
      writeCapacityUnits: Long
    )

    object ProvisionedThroughputConfig {
      implicit val decoder: Decoder[ProvisionedThroughputConfig] =
        deriveDecoder[ProvisionedThroughputConfig]
      implicit val encoder: Encoder[ProvisionedThroughputConfig] =
        deriveEncoder[ProvisionedThroughputConfig]
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
        Cassandra.decoder.apply(cursor)
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
      Cassandra.encoder
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
