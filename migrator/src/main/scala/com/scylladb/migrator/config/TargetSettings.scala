package com.scylladb.migrator.config

import cats.implicits._
import com.scylladb.migrator.AwsUtils
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import com.scylladb.migrator.config.BillingModeCodec._

sealed trait TargetSettings
object TargetSettings {
  case class Scylla(
    host: String,
    port: Int,
    localDC: Option[String],
    credentials: Option[Credentials],
    sslOptions: Option[SSLOptions],
    keyspace: String,
    table: String,
    connections: Option[Int],
    stripTrailingZerosForDecimals: Boolean,
    writeTTLInS: Option[Int],
    writeWritetimestampInuS: Option[Long],
    consistencyLevel: String,
    dropNullPrimaryKeys: Option[Boolean] = None,
    cloud: Option[CloudConfig] = None
  ) extends TargetSettings

  /** Mirror of `SourceSettings.Cassandra` cloud handling. See the doc-comment on [[CloudConfig]]
    * for why `host`/`port`/`localDC`/`sslOptions` are rejected when `cloud` is set.
    */
  object Scylla {
    private val SentinelHost: String = ""
    private val SentinelPort: Int = 0

    implicit val decoder: Decoder[Scylla] = Decoder.instance { c =>
      val hasHost = c.get[Option[String]]("host").exists(_.isDefined)
      val hasPort = c.get[Option[Int]]("port").exists(_.isDefined)
      for {
        cloud <- c.get[Option[CloudConfig]]("cloud")
        _ <- (cloud.isDefined, hasHost, hasPort) match {
               case (true, true, _) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (true, _, true) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'cloud' is mutually exclusive with 'host'/'port'. " +
                       "Remove 'host' and 'port' when using a secure-connect bundle.",
                     c.history
                   )
                 )
               case (false, true, true) => Right(())
               case (false, _, _) =>
                 Left(
                   DecodingFailure(
                     "Scylla target: 'host' and 'port' are required unless 'cloud' is set.",
                     c.history
                   )
                 )
               case (true, false, false) => Right(())
             }
        host                          <- c.getOrElse[String]("host")(SentinelHost)
        port                          <- c.getOrElse[Int]("port")(SentinelPort)
        localDC                       <- c.get[Option[String]]("localDC")
        credentials                   <- c.get[Option[Credentials]]("credentials")
        sslOptions                    <- c.get[Option[SSLOptions]]("sslOptions")
        keyspace                      <- c.get[String]("keyspace")
        table                         <- c.get[String]("table")
        connections                   <- c.get[Option[Int]]("connections")
        stripTrailingZerosForDecimals <- c.get[Boolean]("stripTrailingZerosForDecimals")
        writeTTLInS                   <- c.get[Option[Int]]("writeTTLInS")
        writeWritetimestampInuS       <- c.get[Option[Long]]("writeWritetimestampInuS")
        consistencyLevel              <- c.get[String]("consistencyLevel")
        dropNullPrimaryKeys           <- c.get[Option[Boolean]]("dropNullPrimaryKeys")
        _ <- if (cloud.isDefined && localDC.isDefined)
               Left(
                 DecodingFailure(
                   "Scylla target: 'localDC' must not be set when using 'cloud' (the bundle " +
                     "carries the local DC).",
                   c.history
                 )
               )
             else Right(())
        _ <- if (cloud.isDefined && sslOptions.isDefined)
               Left(
                 DecodingFailure(
                   "Scylla target: 'sslOptions' must not be set when using 'cloud' (the bundle " +
                     "carries the TLS material).",
                   c.history
                 )
               )
             else Right(())
      } yield Scylla(
        host,
        port,
        localDC,
        credentials,
        sslOptions,
        keyspace,
        table,
        connections,
        stripTrailingZerosForDecimals,
        writeTTLInS,
        writeWritetimestampInuS,
        consistencyLevel,
        dropNullPrimaryKeys,
        cloud
      )
    }

    implicit val encoder: Encoder.AsObject[Scylla] = Encoder.AsObject.instance { s =>
      val common = io.circe.JsonObject(
        "localDC"                       -> s.localDC.asJson,
        "credentials"                   -> s.credentials.asJson,
        "sslOptions"                    -> s.sslOptions.asJson,
        "keyspace"                      -> s.keyspace.asJson,
        "table"                         -> s.table.asJson,
        "connections"                   -> s.connections.asJson,
        "stripTrailingZerosForDecimals" -> s.stripTrailingZerosForDecimals.asJson,
        "writeTTLInS"                   -> s.writeTTLInS.asJson,
        "writeWritetimestampInuS"       -> s.writeWritetimestampInuS.asJson,
        "consistencyLevel"              -> s.consistencyLevel.asJson,
        "dropNullPrimaryKeys"           -> s.dropNullPrimaryKeys.asJson
      )
      s.cloud match {
        case Some(cloud) => common.add("cloud", cloud.asJson)
        case None =>
          common.add("host", s.host.asJson).add("port", s.port.asJson)
      }
    }
  }

  /** Common trait for DynamoDB-protocol targets (both AWS DynamoDB and Scylla Alternator). */
  sealed trait DynamoDBLike extends TargetSettings {
    def endpoint: Option[DynamoDBEndpoint]
    def region: Option[String]
    def credentials: Option[AWSCredentials]
    def table: String
    def writeThroughput: Option[Int]
    def throughputWritePercent: Option[Float]
    def streamChanges: Boolean
    def skipInitialSnapshotTransfer: Option[Boolean]

    /** Whether to strip `ConsumedCapacity` from DynamoDB responses.
      *
      *   - '''DynamoDB (AWS):''' defaults to `false` -- AWS uses consumed capacity for throttling
      *     and billing feedback.
      *   - '''Alternator (Scylla):''' defaults to `true` -- Scylla Alternator does not support
      *     `ConsumedCapacity`, so leaving it enabled would produce unnecessary overhead or errors.
      */
    def removeConsumedCapacity: Boolean
    def billingMode: Option[BillingMode]
    def alternatorSettings: Option[AlternatorSettings]
    lazy val finalCredentials: Option[com.scylladb.migrator.AWSCredentials] =
      AwsUtils.computeFinalCredentials(credentials, endpoint, region)
  }

  case class DynamoDB(
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    writeThroughput: Option[Int],
    throughputWritePercent: Option[Float],
    streamChanges: Boolean,
    skipInitialSnapshotTransfer: Option[Boolean],
    billingMode: Option[BillingMode] = None
  ) extends DynamoDBLike {
    val removeConsumedCapacity: Boolean = false
    val alternatorSettings: Option[AlternatorSettings] = None
  }

  case class Alternator(
    alternatorEndpoint: DynamoDBEndpoint,
    region: Option[String],
    credentials: Option[AWSCredentials],
    table: String,
    writeThroughput: Option[Int],
    throughputWritePercent: Option[Float],
    streamChanges: Boolean,
    skipInitialSnapshotTransfer: Option[Boolean],
    removeConsumedCapacity: Boolean = true,
    billingMode: Option[BillingMode] = None,
    alternatorConfig: AlternatorSettings = AlternatorSettings()
  ) extends DynamoDBLike {
    val endpoint: Option[DynamoDBEndpoint] = Some(alternatorEndpoint)
    val alternatorSettings: Option[AlternatorSettings] = Some(alternatorConfig)
  }

  case class Parquet(
    path: String,
    compression: String = "snappy",
    mode: String = "error"
  ) extends TargetSettings
  object Parquet {
    val validCompressionCodecs: Set[String] =
      Set("none", "uncompressed", "snappy", "gzip", "lzo", "brotli", "lz4", "zstd")
    val validModes: Set[String] =
      Set("error", "overwrite", "append", "ignore")
  }

  case class DynamoDBS3Export(
    path: String
  ) extends TargetSettings

  private def validateDynamoDBLikeTarget(t: DynamoDBLike): List[String] = {
    val errors = List.newBuilder[String]
    if (t.table.trim.isEmpty)
      errors += "'table' must not be empty."
    if (t.writeThroughput.exists(_ <= 0))
      errors += "'writeThroughput' must be a positive integer."
    if (t.throughputWritePercent.exists(v => v < 0.1f || v > 1.5f))
      errors += "'throughputWritePercent' must be between 0.1 and 1.5."
    errors.result()
  }

  private def validateScyllaTarget(t: Scylla): List[String] =
    HostValidation.validateHostOrIp("Scylla target", t.host) ++
      HostValidation.validatePort("Scylla target", t.port)

  implicit val decoder: Decoder[TargetSettings] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "scylla" | "cassandra" =>
          Scylla.decoder.apply(cursor).flatMap { s =>
            if (s.cloud.isDefined) Right(s)
            else {
              val allErrors = validateScyllaTarget(s)
              if (allErrors.nonEmpty)
                Left(
                  DecodingFailure(
                    s"Target type 'scylla': ${allErrors.mkString("; ")}",
                    cursor.history
                  )
                )
              else Right(s)
            }
          }
        case "dynamodb" | "dynamo" =>
          AlternatorSettings.guardDynamoDBType(cursor, "Target").flatMap { _ =>
            deriveDecoder[DynamoDB].apply(cursor).flatMap { d =>
              val allErrors = validateDynamoDBLikeTarget(d)
              if (allErrors.nonEmpty)
                Left(
                  DecodingFailure(
                    s"Target type 'dynamodb': ${allErrors.mkString("; ")}",
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
                     "Target type 'alternator' does not use a nested 'alternator' block; " +
                       "place Alternator settings at the top level.",
                     cursor.history
                   )
                 )
            altSettings            <- AlternatorSettings.decoder(cursor)
            maybeEndpoint          <- cursor.get[Option[DynamoDBEndpoint]]("endpoint")
            region                 <- cursor.get[Option[String]]("region")
            credentials            <- cursor.get[Option[AWSCredentials]]("credentials")
            table                  <- cursor.get[String]("table")
            writeThroughput        <- cursor.get[Option[Int]]("writeThroughput")
            throughputWritePercent <- cursor.get[Option[Float]]("throughputWritePercent")
            streamChanges          <- cursor.get[Boolean]("streamChanges")
            skipInitialSnapshotTransfer <-
              cursor.get[Option[Boolean]]("skipInitialSnapshotTransfer")
            // Default to true for Alternator (Scylla doesn't support ConsumedCapacity).
            rcc         <- cursor.getOrElse[Boolean]("removeConsumedCapacity")(true)
            billingMode <- cursor.get[Option[BillingMode]]("billingMode")
            result <- {
              val errors = List.newBuilder[String]
              errors ++= AlternatorSettings.validateDecoding(
                maybeEndpoint,
                credentials,
                altSettings
              )
              errors ++= validateDynamoDBLikeTarget(
                // Temporarily build with a dummy endpoint for shared validation.
                // The endpoint-required check is already in validateDecoding above.
                Alternator(
                  maybeEndpoint.getOrElse(DynamoDBEndpoint("http://placeholder", 0)),
                  region,
                  credentials,
                  table,
                  writeThroughput,
                  throughputWritePercent,
                  streamChanges,
                  skipInitialSnapshotTransfer,
                  rcc,
                  billingMode,
                  altSettings
                )
              )
              val allErrors = errors.result()
              if (allErrors.nonEmpty)
                Left(
                  DecodingFailure(
                    s"Target type 'alternator': ${allErrors.mkString("; ")}",
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
                        writeThroughput,
                        throughputWritePercent,
                        streamChanges,
                        skipInitialSnapshotTransfer,
                        rcc,
                        billingMode,
                        altSettings
                      )
                    )
                  case None =>
                    // Should not reach here: validateDecoding already rejects missing endpoint.
                    Left(
                      DecodingFailure(
                        "Target type 'alternator' requires an 'endpoint' to be set.",
                        cursor.history
                      )
                    )
                }
            }
          } yield result
        case "parquet" =>
          for {
            path        <- cursor.get[String]("path")
            compression <- cursor.getOrElse[String]("compression")("snappy")
            mode        <- cursor.getOrElse[String]("mode")("error")
            _ <- Either.cond(
                   path.trim.nonEmpty,
                   (),
                   DecodingFailure("Parquet target 'path' must not be empty", cursor.history)
                 )
            _ <- Either.cond(
                   Parquet.validCompressionCodecs.contains(compression.toLowerCase),
                   (),
                   DecodingFailure(
                     s"Invalid Parquet compression codec '$compression'. " +
                       s"Valid values: ${Parquet.validCompressionCodecs.toSeq.sorted.mkString(", ")}",
                     cursor.history
                   )
                 )
            _ <- Either.cond(
                   Parquet.validModes.contains(mode.toLowerCase),
                   (),
                   DecodingFailure(
                     s"Invalid Parquet write mode '$mode'. " +
                       s"Valid values: ${Parquet.validModes.toSeq.sorted.mkString(", ")}",
                     cursor.history
                   )
                 )
          } yield Parquet(path.trim, compression.toLowerCase, mode.toLowerCase)
        case "dynamodb-s3-export" =>
          for {
            path <- cursor.get[String]("path")
            _ <- Either.cond(
                   path.trim.nonEmpty,
                   (),
                   DecodingFailure(
                     "DynamoDB S3 Export target 'path' must not be empty",
                     cursor.history
                   )
                 )
          } yield DynamoDBS3Export(path.trim)
        case otherwise =>
          Left(DecodingFailure(s"Invalid target type: ${otherwise}", cursor.history))
      }
    }

  implicit val encoder: Encoder[TargetSettings] =
    Encoder.instance {
      case t: Scylla =>
        Scylla.encoder
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString("scylla"))
          .asJson

      case t: DynamoDB =>
        deriveEncoder[DynamoDB]
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString("dynamodb"))
          .asJson

      case t: Alternator =>
        val baseObj = deriveEncoder[Alternator]
          .encodeObject(t)
          .remove("alternatorConfig")
          .remove("alternatorEndpoint")
          .add("endpoint", Encoder[DynamoDBEndpoint].apply(t.alternatorEndpoint))
        val altObj = AlternatorSettings.asObjectEncoder.encodeObject(t.alternatorConfig)
        altObj.toList
          .foldLeft(baseObj) { case (acc, (k, v)) => acc.add(k, v) }
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString("alternator"))
          .asJson

      case t: Parquet =>
        deriveEncoder[Parquet]
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString("parquet"))
          .asJson

      case t: DynamoDBS3Export =>
        deriveEncoder[DynamoDBS3Export]
          .encodeObject(t)
          .filter { case (_, v) => !v.isNull }
          .add("type", Json.fromString("dynamodb-s3-export"))
          .asJson
    }
}
