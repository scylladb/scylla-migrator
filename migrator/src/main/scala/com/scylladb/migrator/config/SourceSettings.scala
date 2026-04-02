package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{ deriveConfiguredDecoder => deriveExtrasDecoder }
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

  case class MySQL(
    host: String,
    port: Int,
    database: String,
    table: String,
    credentials: Credentials,
    primaryKey: Option[List[String]],
    partitionColumn: Option[String],
    numPartitions: Option[Int],
    lowerBound: Option[Long],
    upperBound: Option[Long],
    fetchSize: Int = MySQL.DefaultFetchSize,
    where: Option[String],
    connectionProperties: Option[Map[String, String]]
  ) extends SourceSettings

  object MySQL {
    val DefaultFetchSize: Int = 1000
    private implicit val circeConfig: Configuration = Configuration.default.withDefaults
    implicit val mysqlDecoder: Decoder[MySQL] = deriveExtrasDecoder[MySQL]

    /** Shared validation logic used by both the config decoder and the reader. Returns a list of
      * validation error messages (empty if valid).
      */
    def validate(mysql: MySQL): List[String] = {
      val errors = List.newBuilder[String]
      if (mysql.port < 1 || mysql.port > 65535)
        errors += s"port must be between 1 and 65535, got: ${mysql.port}"
      if (mysql.fetchSize <= 0)
        errors += s"fetchSize must be > 0, got: ${mysql.fetchSize}"
      if (mysql.fetchSize > com.scylladb.migrator.readers.MySQL.MaxFetchSize)
        errors += s"fetchSize must be <= ${com.scylladb.migrator.readers.MySQL.MaxFetchSize}, got: ${mysql.fetchSize}"
      mysql.numPartitions.foreach { n =>
        if (n <= 0) errors += s"numPartitions must be > 0, got: $n"
      }
      (mysql.lowerBound, mysql.upperBound) match {
        case (Some(lb), Some(ub)) if lb >= ub =>
          errors += s"lowerBound ($lb) must be less than upperBound ($ub)"
        case _ => // ok
      }
      errors.result()
    }
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
        deriveDecoder[Cassandra].apply(cursor)
      case "parquet" =>
        deriveDecoder[Parquet].apply(cursor)
      case "dynamo" | "dynamodb" =>
        deriveDecoder[DynamoDB].apply(cursor)
      case "dynamodb-s3-export" =>
        deriveDecoder[DynamoDBS3Export].apply(cursor)
      case "mysql" =>
        MySQL.mysqlDecoder.apply(cursor).flatMap { mysql =>
          def checkRemainingValidations(): Either[DecodingFailure, MySQL] =
            if (mysql.database.trim.isEmpty)
              Left(
                DecodingFailure(
                  "database must not be empty",
                  cursor.history
                )
              )
            else if (!mysql.database.matches("[a-zA-Z0-9_$\\-]+"))
              Left(
                DecodingFailure(
                  s"Invalid database name '${mysql.database}'. " +
                    "Must contain only alphanumeric characters, underscores, dollar signs, or hyphens. " +
                    "URL-significant characters (/, ?, #, &) are not allowed.",
                  cursor.history
                )
              )
            else if (mysql.table.trim.isEmpty)
              Left(
                DecodingFailure(
                  "table must not be empty",
                  cursor.history
                )
              )
            else if (!mysql.table.matches("[a-zA-Z0-9_$\\-]+"))
              Left(
                DecodingFailure(
                  s"table '${mysql.table}' contains invalid characters. " +
                    "Must match [a-zA-Z0-9_$$\\-]+ (alphanumeric, underscore, dollar sign, or hyphen).",
                  cursor.history
                )
              )
            else if (mysql.credentials.username.trim.isEmpty)
              Left(
                DecodingFailure(
                  "username must not be empty",
                  cursor.history
                )
              )
            else if (mysql.credentials.password.isEmpty)
              Left(
                DecodingFailure(
                  "password must not be empty",
                  cursor.history
                )
              )
            else if (mysql.credentials.password == "<redacted>")
              Left(
                DecodingFailure(
                  "password is '<redacted>'. This appears to be a savepoint file with redacted credentials. " +
                    "Use the original configuration file instead.",
                  cursor.history
                )
              )
            else if (mysql.where.exists(w => w.trim.isEmpty))
              Left(
                DecodingFailure(
                  "WHERE clause must not be empty or blank when specified",
                  cursor.history
                )
              )
            else if (mysql.where.exists(_.exists(c => c.isControl)))
              Left(
                DecodingFailure(
                  "WHERE clause contains control characters (newlines, null bytes, etc.) which are not allowed",
                  cursor.history
                )
              )
            else {
              val dangerousKeys = mysql.connectionProperties
                .getOrElse(Map.empty)
                .keys
                .filter(k =>
                  com.scylladb.migrator.readers.MySQL.DangerousJdbcKeys
                    .contains(k.toLowerCase)
                )
                .toList
              if (dangerousKeys.nonEmpty)
                Left(
                  DecodingFailure(
                    s"connectionProperties contains blocked security-sensitive keys: ${dangerousKeys.mkString(", ")}. " +
                      "These properties are blocked for security reasons.",
                    cursor.history
                  )
                )
              else {
                val validationErrors = MySQL.validate(mysql)
                if (validationErrors.nonEmpty)
                  Left(
                    DecodingFailure(
                      validationErrors.mkString("; "),
                      cursor.history
                    )
                  )
                else if (
                  mysql.partitionColumn.exists { c =>
                    // Allow either unquoted identifiers [a-zA-Z_][a-zA-Z0-9_]* or
                    // backtick-quoted identifiers where backticks can be escaped by doubling them.
                    // The regex [^`\n\r]|``)+` matches one or more occurrences of:
                    // - any character except backtick, newline, or carriage return, OR
                    // - two consecutive backticks (escaped backtick)
                    // Note: `+` (not `*`) ensures empty backtick-quoted identifiers are rejected.
                    !c.matches("[a-zA-Z_][a-zA-Z0-9_]*") &&
                    !c.matches("`(([^`\\n\\r]|``)+)`")
                  }
                )
                  Left(
                    DecodingFailure(
                      s"partitionColumn '${mysql.partitionColumn.get}' contains invalid characters. " +
                        "Must be a valid SQL identifier matching [a-zA-Z_][a-zA-Z0-9_]* or a " +
                        "backtick-quoted identifier like `my column` (doubled backticks `` are allowed for escaping). " +
                        "This restriction exists as a defense against SQL injection in the JDBC partition column path.",
                      cursor.history
                    )
                  )
                else
                  (mysql.partitionColumn, mysql.numPartitions) match {
                    case (Some(_), None) =>
                      Left(
                        DecodingFailure(
                          "partitionColumn is set but numPartitions is missing. Both must be set together.",
                          cursor.history
                        )
                      )
                    case (None, Some(_)) =>
                      Left(
                        DecodingFailure(
                          "numPartitions is set but partitionColumn is missing. Both must be set together.",
                          cursor.history
                        )
                      )
                    case (Some(_), Some(_)) =>
                      (mysql.lowerBound, mysql.upperBound) match {
                        case (Some(_), Some(_)) =>
                          Right(mysql)
                        case _ =>
                          Left(
                            DecodingFailure(
                              "Both lowerBound and upperBound must be set when using partitioned reads.",
                              cursor.history
                            )
                          )
                      }
                    case _ => Right(mysql)
                  }
              }
            }

          if (mysql.host.trim.isEmpty)
            Left(
              DecodingFailure(
                "host must not be empty",
                cursor.history
              )
            )
          else if (mysql.host.startsWith("[") && mysql.host.endsWith("]")) {
            val inner = mysql.host.slice(1, mysql.host.length - 1)
            // If it's wrapped in brackets but doesn't contain a colon, it's an IPv4 in brackets
            if (!inner.contains(':'))
              Left(
                DecodingFailure(
                  s"IPv4 addresses must not be wrapped in brackets. Use '$inner' instead of '${mysql.host}'",
                  cursor.history
                )
              )
            else if (!HostValidation.isValidIPv6Host(mysql.host))
              Left(
                DecodingFailure(
                  s"Invalid host '${mysql.host}': must be a hostname, IPv4, or IPv6 address. " +
                    "URL metacharacters (/, ?, #, &, @) are not allowed.",
                  cursor.history
                )
              )
            else
              checkRemainingValidations()
          } else if (
            !HostValidation
              .isValidHostname(mysql.host) && !HostValidation.isValidIPv6Host(mysql.host)
          )
            Left(
              DecodingFailure(
                s"Invalid host '${mysql.host}': must be a hostname, IPv4, or IPv6 address. " +
                  "URL metacharacters (/, ?, #, &, @) are not allowed.",
                cursor.history
              )
            )
          else
            checkRemainingValidations()
        }
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
    case s: MySQL =>
      val base = deriveEncoder[MySQL].encodeObject(s)
      val redacted = s.connectionProperties.map { props =>
        val sensitivePattern = "(?i).*(password|secret|token|credential).*".r
        props.map { case (k, v) =>
          k -> (if (sensitivePattern.findFirstIn(k).isDefined) "<redacted>" else v)
        }
      }
      val updated = redacted match {
        case Some(r) => base.add("connectionProperties", r.asJson)
        case None    => base
      }
      updated
        .add("type", Json.fromString("mysql"))
        .asJson
  }
}
