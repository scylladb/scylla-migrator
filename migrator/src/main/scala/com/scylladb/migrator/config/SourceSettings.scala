package com.scylladb.migrator.config

import io.circe._
import io.circe.generic.semiauto._

/**
 * Source settings discriminated union - extended with MariaDB support.
 * 
 * This file extends the existing SourceSettings to add MariaDB as a source type.
 * In the actual scylla-migrator codebase, this would be merged with the existing
 * SourceSettings.scala file.
 */
object SourceSettings {
  
  /**
   * MariaDB source configuration.
   * Used for migrating from MariaDB/MySQL to ScyllaDB.
   */
  case class MariaDB(
    host: String,
    port: Int = 3306,
    credentials: MariaDBCredentials,
    database: String,
    table: String,
    sslOptions: Option[MariaDBSslOptions] = None,
    connectionPoolSize: Int = 4,
    connectionTimeoutMs: Int = 30000,
    readTimeoutMs: Int = 300000,
    useBackupStage: Boolean = true,
    streamBinlog: Boolean = true,
    serverId: Option[String] = None,
    splitCount: Int = 256,
    fetchSize: Int = 1000
  )
  
  object MariaDB {
    implicit val decoder: Decoder[MariaDB] = deriveDecoder[MariaDB]
    implicit val encoder: Encoder[MariaDB] = deriveEncoder[MariaDB]
    
    /**
     * Convert to MariaDBSourceSettings for use with NativeBridge
     */
    def toSourceSettings(m: MariaDB): MariaDBSourceSettings = 
      MariaDBSourceSettings(
        host = m.host,
        port = m.port,
        credentials = m.credentials,
        database = m.database,
        table = m.table,
        sslOptions = m.sslOptions,
        connectionPoolSize = m.connectionPoolSize,
        connectionTimeoutMs = m.connectionTimeoutMs,
        readTimeoutMs = m.readTimeoutMs,
        useBackupStage = m.useBackupStage,
        streamBinlog = m.streamBinlog,
        serverId = m.serverId,
        splitCount = m.splitCount,
        fetchSize = m.fetchSize
      )
  }
  
  /**
   * Extended source type discriminator that includes MariaDB.
   * 
   * In the actual implementation, this would be added to the existing
   * SourceSettings sealed trait in the scylla-migrator codebase.
   */
  sealed trait SourceType
  object SourceType {
    case object Cassandra extends SourceType
    case object Parquet extends SourceType  
    case object DynamoDB extends SourceType
    case object MariaDB extends SourceType
    
    implicit val decoder: Decoder[SourceType] = Decoder.decodeString.emap {
      case "cassandra" => Right(Cassandra)
      case "parquet" => Right(Parquet)
      case "dynamodb" => Right(DynamoDB)
      case "mariadb" => Right(MariaDB)
      case other => Left(s"Unknown source type: $other")
    }
    
    implicit val encoder: Encoder[SourceType] = Encoder.encodeString.contramap {
      case Cassandra => "cassandra"
      case Parquet => "parquet"
      case DynamoDB => "dynamodb"
      case MariaDB => "mariadb"
    }
  }
}

/**
 * Target settings extension with explicit ScyllaDB case class.
 * In the actual codebase, this exists in TargetSettings.scala
 */
object TargetSettings {
  
  case class Scylla(
    host: String,
    port: Int = 9042,
    localDC: Option[String] = None,
    credentials: Option[Credentials] = None,
    sslOptions: Option[SSLOptions] = None,
    keyspace: String,
    table: String,
    connections: Int = 16,
    consistencyLevel: String = "LOCAL_QUORUM",
    stripTrailingZerosForDecimals: Boolean = false,
    writeTTLInS: Option[Int] = None,
    writeWritetimestampInuS: Option[Long] = None
  )
  
  case class Credentials(
    username: String,
    password: String
  )
  
  case class SSLOptions(
    clientAuthEnabled: Boolean = false,
    enabled: Boolean = false,
    trustStorePassword: Option[String] = None,
    trustStorePath: Option[String] = None,
    trustStoreType: String = "JKS",
    keyStorePassword: Option[String] = None,
    keyStorePath: Option[String] = None,
    keyStoreType: String = "JKS",
    enabledAlgorithms: List[String] = List.empty,
    protocol: String = "TLS"
  )
  
  object Scylla {
    implicit val credentialsDecoder: Decoder[Credentials] = deriveDecoder[Credentials]
    implicit val credentialsEncoder: Encoder[Credentials] = deriveEncoder[Credentials]
    implicit val sslDecoder: Decoder[SSLOptions] = deriveDecoder[SSLOptions]
    implicit val sslEncoder: Encoder[SSLOptions] = deriveEncoder[SSLOptions]
    implicit val decoder: Decoder[Scylla] = deriveDecoder[Scylla]
    implicit val encoder: Encoder[Scylla] = deriveEncoder[Scylla]
  }
}

/**
 * Column rename configuration
 */
case class ColumnRename(
  from: String,
  to: String
)

object ColumnRename {
  implicit val decoder: Decoder[ColumnRename] = deriveDecoder[ColumnRename]
  implicit val encoder: Encoder[ColumnRename] = deriveEncoder[ColumnRename]
}

/**
 * Renames configuration
 */
case class Renames(
  keyspace: Option[String] = None,
  table: Option[String] = None,
  columns: List[ColumnRename] = List.empty
)

object Renames {
  implicit val decoder: Decoder[Renames] = deriveDecoder[Renames]
  implicit val encoder: Encoder[Renames] = deriveEncoder[Renames]
}
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
                      maxMapTasks: Option[Int],
                      removeConsumedCapacity: Option[Boolean] = None)
      extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }
  case class Parquet(path: String,
                     credentials: Option[AWSCredentials],
                     endpoint: Option[DynamoDBEndpoint],
                     region: Option[String])
      extends SourceSettings {
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

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
