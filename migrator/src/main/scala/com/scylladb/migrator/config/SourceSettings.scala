package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{ deriveConfiguredDecoder => deriveExtrasDecoder }
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  getZoneId,
  stringToDate,
  stringToTimestamp
}
import org.apache.spark.unsafe.types.UTF8String
import software.amazon.awssdk.services.dynamodb.model.BillingMode

import java.util.Locale
import scala.util.Try

/** Endpoint for DynamoDB-protocol connections.
  *
  * The semantics of `host` differ by context:
  *   - '''DynamoDB (AWS/custom endpoints):''' either a bare hostname, e.g.
  *     `dynamodb.us-east-1.amazonaws.com`, or a full URL such as `http://localhost`
  *   - '''Alternator (Scylla):''' must include protocol prefix, e.g. `http://10.0.0.1`
  *
  * Bare DynamoDB hosts are normalized to `http://` when passed to APIs that require an absolute
  * URI. The protocol requirement for Alternator endpoints is validated at config parse time.
  */
case class DynamoDBEndpoint(host: String, port: Int) {
  def renderEndpoint: String = {
    val trimmedHost = host.stripSuffix("/")
    val lowerHost = trimmedHost.toLowerCase(java.util.Locale.ROOT)
    val endpointHost =
      if (lowerHost.startsWith("http://") || lowerHost.startsWith("https://"))
        trimmedHost
      else
        s"http://${trimmedHost}"
    s"${endpointHost}:${port}"
  }
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

  /** Common trait for DynamoDB-protocol sources (both AWS DynamoDB and Scylla Alternator). */
  sealed trait DynamoDBLike extends SourceSettings {
    def endpoint: Option[DynamoDBEndpoint]
    def region: Option[String]
    def credentials: Option[AWSCredentials]
    def table: String
    def scanSegments: Option[Int]
    def readThroughput: Option[Int]
    def throughputReadPercent: Option[Float]
    def maxMapTasks: Option[Int]

    /** Whether to strip `ConsumedCapacity` from DynamoDB responses.
      *
      *   - '''DynamoDB (AWS):''' defaults to `false` -- AWS uses consumed capacity for throttling
      *     and billing feedback.
      *   - '''Alternator (Scylla):''' defaults to `true` -- Scylla Alternator does not support
      *     `ConsumedCapacity`, so leaving it enabled would produce unnecessary overhead or errors.
      */
    def removeConsumedCapacity: Boolean
    def alternatorSettings: Option[AlternatorSettings]
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  case class DynamoDB(
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    scanSegments: Option[Int],
    readThroughput: Option[Int],
    throughputReadPercent: Option[Float],
    maxMapTasks: Option[Int]
  ) extends DynamoDBLike {
    val removeConsumedCapacity: Boolean = false
    val alternatorSettings: Option[AlternatorSettings] = None
  }

  case class Alternator(
    alternatorEndpoint: DynamoDBEndpoint,
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    scanSegments: Option[Int],
    readThroughput: Option[Int],
    throughputReadPercent: Option[Float],
    maxMapTasks: Option[Int],
    removeConsumedCapacity: Boolean = true,
    alternatorConfig: AlternatorSettings = AlternatorSettings()
  ) extends DynamoDBLike {
    val endpoint: Option[DynamoDBEndpoint] = Some(alternatorEndpoint)
    val alternatorSettings: Option[AlternatorSettings] = Some(alternatorConfig)
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
    lowerBound: Option[MySQL.PartitionBound],
    upperBound: Option[MySQL.PartitionBound],
    zeroDateTimeBehavior: MySQL.ZeroDateTimeBehavior = MySQL.ZeroDateTimeBehavior.Exception,
    fetchSize: Int = MySQL.DefaultFetchSize,
    where: Option[String],
    connectionProperties: Option[Map[String, String]]
  ) extends SourceSettings

  object MySQL {
    val DefaultFetchSize: Int = 1000
    private val UnquotedPartitionColumnPattern = "[a-zA-Z_][a-zA-Z0-9_]*".r
    private val QuotedPartitionColumnPattern = "`(([^`\\n\\r]|``)+)`".r
    private val NumericPartitionBoundPattern = "[-+]?\\d+".r
    private val ConfigTemporalValidationZoneId = getZoneId("UTC")
    private implicit val circeConfig: Configuration = Configuration.default.withDefaults
    implicit val mysqlDecoder: Decoder[MySQL] = deriveExtrasDecoder[MySQL]

    case class PartitionBound(value: String) extends AnyVal {
      def isBlank: Boolean = value.trim.isEmpty
      def isNumericLiteral: Boolean = NumericPartitionBoundPattern.matches(value)
    }

    object PartitionBound {
      implicit val decoder: Decoder[PartitionBound] =
        Decoder.decodeString
          .map(PartitionBound(_))
          .or(Decoder.decodeLong.map(value => PartitionBound(value.toString)))

      implicit val encoder: Encoder[PartitionBound] = Encoder.instance { bound =>
        if (bound.isNumericLiteral)
          Try(bound.value.toLong).toOption
            .map(Json.fromLong)
            .getOrElse(Json.fromString(bound.value))
        else
          Json.fromString(bound.value)
      }
    }

    sealed abstract class ZeroDateTimeBehavior(val jdbcValue: String)

    object ZeroDateTimeBehavior {
      case object Exception extends ZeroDateTimeBehavior("EXCEPTION")
      case object ConvertToNull extends ZeroDateTimeBehavior("CONVERT_TO_NULL")
      case object Round extends ZeroDateTimeBehavior("ROUND")

      val values: List[ZeroDateTimeBehavior] = List(Exception, ConvertToNull, Round)
      private val byJdbcValue: Map[String, ZeroDateTimeBehavior] =
        values.map(value => value.jdbcValue -> value).toMap

      implicit val decoder: Decoder[ZeroDateTimeBehavior] = Decoder.decodeString.emap { raw =>
        byJdbcValue
          .get(raw.trim.toUpperCase(Locale.ROOT))
          .toRight(
            s"zeroDateTimeBehavior must be one of ${values.map(_.jdbcValue).mkString(", ")}, got: '$raw'"
          )
      }

      implicit val encoder: Encoder[ZeroDateTimeBehavior] =
        Encoder.encodeString.contramap(_.jdbcValue)
    }

    private def parseNumericPartitionBound(
      boundName: String,
      bound: PartitionBound
    ): Either[String, Long] =
      if (!bound.isNumericLiteral)
        Left(s"$boundName ('${bound.value}') must be a valid integer literal")
      else
        Try(bound.value.toLong).toEither.leftMap(_ =>
          s"$boundName ('${bound.value}') must fit in a signed 64-bit integer"
        )

    private def parseDatePartitionBound(bound: PartitionBound): Option[Long] =
      stringToDate(UTF8String.fromString(bound.value)).map(_.toLong)

    private def parseTimestampPartitionBound(bound: PartitionBound): Option[Long] =
      stringToTimestamp(UTF8String.fromString(bound.value), ConfigTemporalValidationZoneId)

    private def validateBoundOrdering(
      lowerBound: PartitionBound,
      upperBound: PartitionBound
    ): Option[String] =
      if (lowerBound.isNumericLiteral && upperBound.isNumericLiteral)
        (
          parseNumericPartitionBound("lowerBound", lowerBound),
          parseNumericPartitionBound("upperBound", upperBound)
        ) match {
          case (Right(parsedLower), Right(parsedUpper)) if parsedLower >= parsedUpper =>
            Some(
              s"lowerBound (${lowerBound.value}) must be less than upperBound (${upperBound.value})"
            )
          case (Left(error), _) =>
            Some(error)
          case (_, Left(error)) =>
            Some(error)
          case _ =>
            None
        }
      else
        (
          parseTimestampPartitionBound(lowerBound),
          parseTimestampPartitionBound(upperBound)
        ) match {
          case (Some(parsedLower), Some(parsedUpper)) if parsedLower >= parsedUpper =>
            Some(
              s"lowerBound (${lowerBound.value}) must be less than upperBound (${upperBound.value})"
            )
          case (Some(_), Some(_)) =>
            None
          case _ =>
            (parseDatePartitionBound(lowerBound), parseDatePartitionBound(upperBound)) match {
              case (Some(parsedLower), Some(parsedUpper)) if parsedLower >= parsedUpper =>
                Some(
                  s"lowerBound (${lowerBound.value}) must be less than upperBound (${upperBound.value})"
                )
              case _ =>
                None
            }
        }

    private def isValidPartitionColumn(column: String): Boolean =
      UnquotedPartitionColumnPattern.matches(column) || QuotedPartitionColumnPattern.matches(column)

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
      mysql.primaryKey.foreach { pk =>
        if (pk.isEmpty)
          errors += "primaryKey must contain at least one column name when specified"
        if (pk.exists(_.trim.isEmpty))
          errors += "primaryKey must not contain empty or blank column names"
        val duplicatePK = pk
          .groupBy(_.toLowerCase(Locale.ROOT))
          .values
          .filter(_.size > 1)
          .map(_.mkString("/"))
          .toList
        if (duplicatePK.nonEmpty)
          errors += s"primaryKey contains duplicate column names: ${duplicatePK.mkString(", ")}"
      }
      mysql.lowerBound.foreach { bound =>
        if (bound.isBlank) errors += "lowerBound must not be empty or blank when specified"
      }
      mysql.upperBound.foreach { bound =>
        if (bound.isBlank) errors += "upperBound must not be empty or blank when specified"
      }
      (mysql.lowerBound, mysql.upperBound) match {
        case (Some(lb), Some(ub)) =>
          validateBoundOrdering(lb, ub).foreach(errors += _)
        case _ => // ok
      }
      mysql.partitionColumn.foreach { column =>
        if (!isValidPartitionColumn(column))
          errors +=
            s"partitionColumn '$column' contains invalid characters. " +
              "Must be a valid SQL identifier matching [a-zA-Z_][a-zA-Z0-9_]* or a " +
              "backtick-quoted identifier like `my column` (doubled backticks `` are allowed for escaping). " +
              "This restriction exists as a defense against SQL injection in the JDBC partition column path."
      }
      (mysql.partitionColumn, mysql.numPartitions) match {
        case (Some(_), None) =>
          errors += "partitionColumn is set but numPartitions is missing. Both must be set together."
        case (None, Some(_)) =>
          errors += "numPartitions is set but partitionColumn is missing. Both must be set together."
        case _ => // ok
      }
      (mysql.lowerBound, mysql.upperBound) match {
        case (Some(_), _) | (_, Some(_))
            if mysql.partitionColumn.isEmpty || mysql.numPartitions.isEmpty =>
          errors += "lowerBound and upperBound can only be set when partitionColumn and numPartitions are both set."
        case _ => // ok
      }
      (mysql.partitionColumn, mysql.numPartitions) match {
        case (Some(_), Some(_)) =>
          (mysql.lowerBound, mysql.upperBound) match {
            case (Some(_), Some(_)) => // ok
            case _ =>
              errors += "Both lowerBound and upperBound must be set when using partitioned reads."
          }
        case _ => // ok
      }
      errors ++= com.scylladb.migrator.readers.MySQL
        .validateConnectionPropertyValues(mysql.connectionProperties)
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

  private def validateDynamoDBLikeSource(s: DynamoDBLike): List[String] = {
    val errors = List.newBuilder[String]
    if (s.table.trim.isEmpty)
      errors += "'table' must not be empty."
    if (s.scanSegments.exists(_ <= 0))
      errors += "'scanSegments' must be a positive integer."
    if (s.readThroughput.exists(_ <= 0))
      errors += "'readThroughput' must be a positive integer."
    if (s.throughputReadPercent.exists(v => v < 0.1f || v > 1.5f))
      errors += "'throughputReadPercent' must be between 0.1 and 1.5."
    if (s.maxMapTasks.exists(_ <= 0))
      errors += "'maxMapTasks' must be a positive integer."
    errors.result()
  }

  implicit val decoder: Decoder[SourceSettings] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "cassandra" | "scylla" =>
        deriveDecoder[Cassandra].apply(cursor)
      case "parquet" =>
        deriveDecoder[Parquet].apply(cursor)
      case "dynamo" | "dynamodb" =>
        AlternatorSettings.guardDynamoDBType(cursor, "Source").flatMap { _ =>
          deriveDecoder[DynamoDB].apply(cursor).flatMap { d =>
            val allErrors = validateDynamoDBLikeSource(d)
            if (allErrors.nonEmpty)
              Left(
                DecodingFailure(
                  s"Source type 'dynamodb': ${allErrors.mkString("; ")}",
                  cursor.history
                )
              )
            else Right(d)
          }
        }
      case "alternator" =>
        for {
          _ <- Either.cond(
                 cursor.downField("alternator").focus.isEmpty,
                 (),
                 DecodingFailure(
                   "Source type 'alternator' does not use a nested 'alternator' block; " +
                     "place Alternator settings at the top level.",
                   cursor.history
                 )
               )
          altSettings           <- AlternatorSettings.decoder(cursor)
          maybeEndpoint         <- cursor.get[Option[DynamoDBEndpoint]]("endpoint")
          region                <- cursor.get[Option[String]]("region")
          credentials           <- cursor.get[Option[AWSCredentials]]("credentials")
          table                 <- cursor.get[String]("table")
          scanSegments          <- cursor.get[Option[Int]]("scanSegments")
          readThroughput        <- cursor.get[Option[Int]]("readThroughput")
          throughputReadPercent <- cursor.get[Option[Float]]("throughputReadPercent")
          maxMapTasks           <- cursor.get[Option[Int]]("maxMapTasks")
          // Default to true for Alternator (Scylla doesn't support ConsumedCapacity).
          rcc <- cursor.getOrElse[Boolean]("removeConsumedCapacity")(true)
          result <- {
            val errors = List.newBuilder[String]
            errors ++= AlternatorSettings.validateDecoding(
              maybeEndpoint,
              credentials,
              altSettings
            )
            errors ++= validateDynamoDBLikeSource(
              // Temporarily build with a dummy endpoint for shared validation.
              // The endpoint-required check is already in validateDecoding above.
              Alternator(
                maybeEndpoint.getOrElse(DynamoDBEndpoint("http://placeholder", 0)),
                region,
                credentials,
                table,
                scanSegments,
                readThroughput,
                throughputReadPercent,
                maxMapTasks,
                rcc,
                altSettings
              )
            )
            val allErrors = errors.result()
            if (allErrors.nonEmpty)
              Left(
                DecodingFailure(
                  s"Source type 'alternator': ${allErrors.mkString("; ")}",
                  cursor.history
                )
              )
            else
              maybeEndpoint match {
                case Some(ep) =>
                  Right(
                    Alternator(
                      ep,
                      region,
                      credentials,
                      table,
                      scanSegments,
                      readThroughput,
                      throughputReadPercent,
                      maxMapTasks,
                      rcc,
                      altSettings
                    )
                  )
                case None =>
                  // Should not reach here: validateDecoding already rejects missing endpoint.
                  Left(
                    DecodingFailure(
                      "Source type 'alternator' requires an 'endpoint' to be set.",
                      cursor.history
                    )
                  )
              }
          }
        } yield result
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
                    .contains(k.toLowerCase(Locale.ROOT))
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
                else
                  Right(mysql)
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
        .filter { case (_, v) => !v.isNull }
        .add("type", Json.fromString("cassandra"))
        .asJson
    case s: DynamoDB =>
      deriveEncoder[DynamoDB]
        .encodeObject(s)
        .filter { case (_, v) => !v.isNull }
        .add("type", Json.fromString("dynamodb"))
        .asJson
    case s: Alternator =>
      val baseObj = deriveEncoder[Alternator]
        .encodeObject(s)
        .remove("alternatorConfig")
        .remove("alternatorEndpoint")
        .add("endpoint", Encoder[DynamoDBEndpoint].apply(s.alternatorEndpoint))
      val altObj = AlternatorSettings.asObjectEncoder.encodeObject(s.alternatorConfig)
      altObj.toList
        .foldLeft(baseObj) { case (acc, (k, v)) => acc.add(k, v) }
        .filter { case (_, v) => !v.isNull }
        .add("type", Json.fromString("alternator"))
        .asJson
    case s: Parquet =>
      deriveEncoder[Parquet]
        .encodeObject(s)
        .filter { case (_, v) => !v.isNull }
        .add("type", Json.fromString("parquet"))
        .asJson
    case s: DynamoDBS3Export =>
      deriveEncoder[DynamoDBS3Export]
        .encodeObject(s)
        .filter { case (_, v) => !v.isNull }
        .add("type", Json.fromString("dynamodb-s3-export"))
        .asJson
    case s: MySQL =>
      deriveEncoder[MySQL]
        .encodeObject(s)
        .add("type", Json.fromString("mysql"))
        .asJson
  }
}
