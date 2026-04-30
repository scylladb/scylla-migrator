package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import software.amazon.awssdk.services.dynamodb.model.BillingMode

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
        Cassandra.decoder.apply(cursor)
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
      case otherwise =>
        Left(DecodingFailure(s"Unknown source type: ${otherwise}", cursor.history))
    }
  }

  implicit val encoder: Encoder[SourceSettings] = Encoder.instance {
    case s: Cassandra =>
      Cassandra.encoder
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
  }
}
