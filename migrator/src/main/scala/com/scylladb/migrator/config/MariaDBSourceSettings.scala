package com.scylladb.migrator.config

import io.circe._
import io.circe.generic.semiauto._

/**
 * Configuration for MariaDB source connection
 */
case class MariaDBSourceSettings(
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

object MariaDBSourceSettings {
  implicit val decoder: Decoder[MariaDBSourceSettings] = deriveDecoder[MariaDBSourceSettings]
  implicit val encoder: Encoder[MariaDBSourceSettings] = deriveEncoder[MariaDBSourceSettings]
}

/**
 * Credentials for MariaDB authentication
 */
case class MariaDBCredentials(
  user: String,
  password: String
)

object MariaDBCredentials {
  implicit val decoder: Decoder[MariaDBCredentials] = deriveDecoder[MariaDBCredentials]
  implicit val encoder: Encoder[MariaDBCredentials] = deriveEncoder[MariaDBCredentials]
}

/**
 * SSL options for MariaDB connection
 */
case class MariaDBSslOptions(
  enabled: Boolean = false,
  trustStorePath: Option[String] = None,
  trustStorePassword: Option[String] = None,
  keyStorePath: Option[String] = None,
  keyStorePassword: Option[String] = None,
  caCertPath: Option[String] = None,
  clientCertPath: Option[String] = None,
  clientKeyPath: Option[String] = None,
  verifyServerCert: Boolean = true
)

object MariaDBSslOptions {
  implicit val decoder: Decoder[MariaDBSslOptions] = deriveDecoder[MariaDBSslOptions]
  implicit val encoder: Encoder[MariaDBSslOptions] = deriveEncoder[MariaDBSslOptions]
}

/**
 * Represents a GTID position for binlog tracking
 */
case class GTIDPosition(
  gtidBinlogPos: String,
  binlogFile: String,
  binlogPosition: Long
)

object GTIDPosition {
  implicit val decoder: Decoder[GTIDPosition] = deriveDecoder[GTIDPosition]
  implicit val encoder: Encoder[GTIDPosition] = deriveEncoder[GTIDPosition]
}

/**
 * Represents a primary key range for parallel scanning
 */
case class PKRange(
  minValue: String,
  maxValue: String,
  isFirstRange: Boolean,
  isLastRange: Boolean,
  rangeId: Int
)

object PKRange {
  implicit val decoder: Decoder[PKRange] = deriveDecoder[PKRange]
  implicit val encoder: Encoder[PKRange] = deriveEncoder[PKRange]
}

/**
 * Column definition from MariaDB schema
 */
case class MariaDBColumn(
  name: String,
  dataType: String,
  maxLength: Int,
  precision: Int,
  scale: Int,
  isNullable: Boolean,
  isPrimaryKey: Boolean,
  isAutoIncrement: Boolean,
  ordinalPosition: Int,
  charset: Option[String] = None,
  collation: Option[String] = None
)

object MariaDBColumn {
  implicit val decoder: Decoder[MariaDBColumn] = deriveDecoder[MariaDBColumn]
  implicit val encoder: Encoder[MariaDBColumn] = deriveEncoder[MariaDBColumn]
}

/**
 * Table schema from MariaDB
 */
case class MariaDBTableSchema(
  databaseName: String,
  tableName: String,
  engine: String,
  columns: List[MariaDBColumn],
  primaryKeyColumns: List[String],
  createTableStmt: String
)

object MariaDBTableSchema {
  implicit val decoder: Decoder[MariaDBTableSchema] = deriveDecoder[MariaDBTableSchema]
  implicit val encoder: Encoder[MariaDBTableSchema] = deriveEncoder[MariaDBTableSchema]
}

/**
 * Binlog event types
 */
sealed trait BinlogEventType
object BinlogEventType {
  case object Insert extends BinlogEventType
  case object Update extends BinlogEventType
  case object Delete extends BinlogEventType
  case object Unknown extends BinlogEventType
  
  implicit val decoder: Decoder[BinlogEventType] = Decoder.decodeInt.map {
    case 0 => Insert
    case 1 => Update
    case 2 => Delete
    case _ => Unknown
  }
  
  implicit val encoder: Encoder[BinlogEventType] = Encoder.encodeInt.contramap {
    case Insert => 0
    case Update => 1
    case Delete => 2
    case Unknown => 3
  }
}

/**
 * Represents a binlog event (change data capture)
 */
case class BinlogEvent(
  eventType: BinlogEventType,
  tableName: String,
  timestampUs: Long,
  columnValues: Map[String, String],
  oldColumnValues: Option[Map[String, String]] = None,
  groupCommitId: Long,
  isLastInGroup: Boolean
)

object BinlogEvent {
  implicit val decoder: Decoder[BinlogEvent] = deriveDecoder[BinlogEvent]
  implicit val encoder: Encoder[BinlogEvent] = deriveEncoder[BinlogEvent]
}
