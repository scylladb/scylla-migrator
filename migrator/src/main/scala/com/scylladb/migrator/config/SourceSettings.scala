package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.syntax._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json, JsonObject }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import software.amazon.awssdk.services.dynamodb.model.BillingMode

import scala.collection.immutable.ListMap

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

  /** @param schemaSampleSize
    *   Number of records to sample for schema discovery (default: 1000). Values above 10K will
    *   trigger a warning; schema discovery uses a synchronized callback so very large values add
    *   lock contention.
    * @param preserveTTL
    *   When true, adds an 'aero_ttl' column (IntegerType) with the record's remaining TTL in
    *   seconds. Records with no expiration will have TTL = -1. (default: false)
    * @param preserveGeneration
    *   When true, adds an 'aero_generation' column (IntegerType) with the record's generation
    *   counter. (default: false)
    * @param totalTimeoutMs
    *   Total timeout in milliseconds for Aerospike operations. When not specified, defaults to
    *   `socketTimeoutMs * 3` if socketTimeoutMs is set, otherwise 30000ms.
    * @param maxPollRetries
    *   Maximum consecutive poll timeouts before failing a partition (default: 5). With the default
    *   pollTimeoutSeconds of 120, this means a partition can wait up to 10 minutes before failing.
    * @param schemaDiscoveryStrategy
    *   Controls how many partitions are scanned during schema discovery (default: "progressive").
    *   "progressive" scans 8 -> 64 -> all partitions (stops after finding records). "single" scans
    *   only 8 partitions (fastest startup, may miss bins in sparse data). "full" scans all
    *   partitions (most thorough, may be slow on large clusters).
    * @param maxConnsPerNode
    *   Maximum number of connections to each Aerospike node (default: Aerospike client default).
    * @param connPoolsPerNode
    *   Number of connection pools per Aerospike node (default: Aerospike client default).
    */
  case class Aerospike(
    hosts: Seq[String],
    port: Option[Int],
    namespace: String,
    set: String,
    bins: Option[Seq[String]],
    splitCount: Option[Int],
    schemaSampleSize: Option[Int],
    queueSize: Option[Int],
    credentials: Option[Credentials],
    connectTimeoutMs: Option[Int],
    socketTimeoutMs: Option[Int],
    tlsName: Option[String],
    schema: Option[ListMap[String, String]],
    pollTimeoutSeconds: Option[Int],
    preserveTTL: Option[Boolean],
    preserveGeneration: Option[Boolean],
    totalTimeoutMs: Option[Int],
    maxScanRetries: Option[Int],
    schemaDiscoveryStrategy: Option[SchemaDiscoveryStrategy],
    maxPollRetries: Option[Int],
    maxConnsPerNode: Option[Int],
    connPoolsPerNode: Option[Int]
  ) extends SourceSettings {
    def ttlEnabled: Boolean = preserveTTL.getOrElse(false)
    def generationEnabled: Boolean = preserveGeneration.getOrElse(false)
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

  // ListMap preserves insertion order from YAML, ensuring schema column order is deterministic.
  // The standard Map may reorder keys for maps with 5+ entries.
  private implicit val listMapStringDecoder: Decoder[ListMap[String, String]] =
    Decoder.decodeJsonObject.emap { obj =>
      val builder = ListMap.newBuilder[String, String]
      val errors = List.newBuilder[String]
      obj.toList.foreach { case (k, v) =>
        v.asString match {
          case Some(s) => builder += k -> s
          case None    => errors += s"Expected string value for key '$k', got: ${v.noSpaces}"
        }
      }
      val errs = errors.result()
      if (errs.nonEmpty) Left(errs.mkString("; "))
      else Right(builder.result())
    }

  private implicit val listMapStringEncoder: Encoder[ListMap[String, String]] =
    Encoder.instance { m =>
      Json.fromJsonObject(JsonObject.fromIterable(m.map { case (k, v) => k -> Json.fromString(v) }))
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
      case "aerospike" =>
        deriveDecoder[Aerospike].apply(cursor).flatMap { a =>
          val errors = List.newBuilder[String]
          a.port.foreach(p => if (p <= 0 || p > 65535) errors += s"Invalid port: $p")
          a.splitCount.foreach(s => if (s <= 0) errors += s"splitCount must be positive, got $s")
          a.schemaSampleSize
            .foreach(s => if (s <= 0) errors += s"schemaSampleSize must be positive, got $s")
          a.queueSize.foreach(q => if (q <= 0) errors += s"queueSize must be positive, got $q")
          a.pollTimeoutSeconds
            .foreach(p => if (p <= 0) errors += s"pollTimeoutSeconds must be positive, got $p")
          a.maxPollRetries
            .foreach(r => if (r <= 0) errors += s"maxPollRetries must be positive, got $r")
          a.maxConnsPerNode
            .foreach(n => if (n <= 0) errors += s"maxConnsPerNode must be positive, got $n")
          a.connPoolsPerNode
            .foreach(n => if (n <= 0) errors += s"connPoolsPerNode must be positive, got $n")
          val errs = errors.result()
          if (errs.nonEmpty) Left(DecodingFailure(errs.mkString("; "), cursor.history))
          else Right(a)
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
    case s: Aerospike =>
      deriveEncoder[Aerospike]
        .encodeObject(s)
        .add("type", Json.fromString("aerospike"))
        .asJson
  }
}
