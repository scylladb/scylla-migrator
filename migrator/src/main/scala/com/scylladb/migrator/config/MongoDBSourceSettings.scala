package com.scylladb.migrator.config

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

/**
 * Configuration for MongoDB as a source for migration to ScyllaDB.
 *
 * @param host         MongoDB host (e.g., "localhost" or "mongodb.example.com")
 * @param port         MongoDB port (default: 27017)
 * @param database     MongoDB database name
 * @param collection   MongoDB collection name
 * @param credentials  Optional authentication credentials
 * @param replicaSet   Optional replica set name (required for oplog tailing)
 * @param authSource   Optional authentication database (default: admin)
 * @param ssl          Optional SSL/TLS configuration
 * @param readPreference Read preference for MongoDB queries
 * @param batchSize    Number of documents to fetch per batch
 * @param splitCount   Number of partitions for parallel reading
 * @param sampleSize   Number of documents to sample for schema inference
 * @param streamChanges Whether to stream oplog changes after initial migration
 */
final case class MongoDBSourceSettings(
    host: String,
    port: Int = 27017,
    database: String,
    collection: String,
    credentials: Option[MongoDBCredentials] = None,
    replicaSet: Option[String] = None,
    authSource: Option[String] = None,
    ssl: Option[MongoDBSSLSettings] = None,
    readPreference: String = "secondaryPreferred",
    batchSize: Int = 1000,
    splitCount: Int = 256,
    sampleSize: Int = 10000,
    streamChanges: Boolean = false,
    partitionKeyField: Option[String] = None,
    clusteringKeyFields: Option[List[String]] = None
) {

  /**
   * Build a MongoDB connection URI from the settings
   */
  def connectionUri: String = {
    val credPart = credentials.map { c =>
      s"${c.username}:${c.password}@"
    }.getOrElse("")

    val authSourcePart = authSource.map(s => s"?authSource=$s").getOrElse("")
    val replicaSetPart = replicaSet.map { rs =>
      if (authSourcePart.nonEmpty) s"&replicaSet=$rs" else s"?replicaSet=$rs"
    }.getOrElse("")
    val sslPart = ssl.filter(_.enabled).map { _ =>
      val prefix = if (authSourcePart.nonEmpty || replicaSetPart.nonEmpty) "&" else "?"
      s"${prefix}ssl=true"
    }.getOrElse("")

    s"mongodb://$credPart$host:$port/$database$authSourcePart$replicaSetPart$sslPart"
  }
}

final case class MongoDBCredentials(
    username: String,
    password: String
)

final case class MongoDBSSLSettings(
    enabled: Boolean = false,
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    keyStorePath: Option[String] = None,
    keyStorePassword: Option[String] = None,
    invalidHostNameAllowed: Boolean = false
)

object MongoDBSourceSettings {
  implicit val mongoDBCredentialsDecoder: Decoder[MongoDBCredentials] = deriveDecoder
  implicit val mongoDBCredentialsEncoder: Encoder[MongoDBCredentials] = deriveEncoder

  implicit val mongoDBSSLSettingsDecoder: Decoder[MongoDBSSLSettings] = deriveDecoder
  implicit val mongoDBSSLSettingsEncoder: Encoder[MongoDBSSLSettings] = deriveEncoder

  implicit val mongoDBSourceSettingsDecoder: Decoder[MongoDBSourceSettings] = deriveDecoder
  implicit val mongoDBSourceSettingsEncoder: Encoder[MongoDBSourceSettings] = deriveEncoder
}
