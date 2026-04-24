package com.scylladb.migrator

import com.scylladb.migrator.config.{MigratorConfig, SourceSettings, TargetSettings}
import com.scylladb.migrator.source.mongodb.{MongoDBMigrationCoordinator, MongoDBMigrator, MongoDBSchemaInference, MongoDBChangeStreamer}
import org.apache.spark.sql.SparkSession
import org.bson.BsonTimestamp
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

/**
 * MongoDB migration handler - integrate this into the existing Migrator object.
 * 
 * This file shows how to add MongoDB support to the existing Scylla Migrator.
 * The actual integration requires modifying Migrator.scala to add the MongoDB
 * case to the pattern match on source type.
 */
object MongoDBMigrationHandler {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Execute MongoDB to ScyllaDB migration.
   * 
   * Call this from the main Migrator.migrate() method when source type is MongoDB.
   */
  def migrate(
      spark: SparkSession,
      config: MigratorConfig
  ): Unit = {
    val sourceSettings = config.source match {
      case SourceSettings.MongoDB(settings) => settings
      case other =>
        throw new IllegalArgumentException(s"Expected MongoDB source, got: $other")
    }

    val targetSettings = config.target match {
      case scylla: TargetSettings.Scylla => scylla
      case other =>
        throw new IllegalArgumentException(s"MongoDB migration requires ScyllaDB target, got: $other")
    }

    log.info("=" * 80)
    log.info("MongoDB to ScyllaDB Migration")
    log.info("=" * 80)
    log.info(s"Source: mongodb://${sourceSettings.host}:${sourceSettings.port}/${sourceSettings.database}.${sourceSettings.collection}")
    log.info(s"Target: ${targetSettings.host}:${targetSettings.port}/${targetSettings.keyspace}.${targetSettings.table}")
    log.info(s"Stream changes: ${sourceSettings.streamChanges}")
    log.info("=" * 80)

    // Initialize coordination infrastructure
    MongoDBMigrationCoordinator.initialize(spark)

    // Generate migration ID for coordination
    val migrationId = MongoDBMigrationCoordinator.generateMigrationId(
      sourceSettings.database,
      sourceSettings.collection,
      targetSettings.keyspace,
      targetSettings.table
    )

    log.info(s"Migration ID: $migrationId")

    // Try to acquire leadership for setup phase
    val isLeader = MongoDBMigrationCoordinator.tryAcquireLeadership(
      spark,
      migrationId,
      targetSettings.keyspace,
      targetSettings.table
    )

    val oplogTimestamp: Option[BsonTimestamp] = if (isLeader) {
      // This worker is the leader - perform setup
      log.info("This worker is the leader - performing schema inference and table creation")

      // Phase 1: Infer schema
      MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.INFERRING_SCHEMA)
      val schema = MongoDBSchemaInference.inferSchema(spark, sourceSettings)

      log.info(s"Inferred schema:")
      log.info(s"  Fields: ${schema.fields.length}")
      schema.fields.foreach { f =>
        log.info(s"    ${f.name}: ${f.cqlType} (nullable: ${f.isNullable})")
      }
      log.info(s"  Partition key: ${schema.partitionKey.mkString(", ")}")
      log.info(s"  Clustering keys: ${schema.clusteringKeys.mkString(", ")}")
      log.info(s"  Total documents: ${schema.totalDocuments}")

      // Phase 2: Capture oplog position (before creating table to avoid race)
      val capturedTimestamp = if (sourceSettings.streamChanges) {
        MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.CAPTURING_OPLOG)
        val ts = captureOplogPosition(sourceSettings)
        log.info(s"Captured oplog position: $ts")
        MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.CAPTURING_OPLOG, Some(ts.getTime))
        Some(ts)
      } else {
        None
      }

      // Phase 3: Create ScyllaDB table
      MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.CREATING_TABLE)
      createTargetTable(spark, schema, targetSettings)
      log.info(s"Created table ${targetSettings.keyspace}.${targetSettings.table}")

      // Phase 4: Signal ready for migration
      MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.READY_FOR_MIGRATION)
      log.info("Setup complete - signaling other workers to proceed")

      capturedTimestamp

    } else {
      // This worker is a follower - wait for leader to complete setup
      log.info("This worker is a follower - waiting for leader to complete setup")

      val state = MongoDBMigrationCoordinator.waitForPhase(
        spark,
        migrationId,
        MongoDBMigrationCoordinator.Phase.READY_FOR_MIGRATION
      )

      state.flatMap(_.oplogTimestamp).map(ts => new BsonTimestamp((ts / 1000).toInt, 0))
    }

    // All workers proceed with data migration
    log.info("Starting data migration...")
    MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.MIGRATING_DATA)

    val migratedCount = MongoDBDataMigration.migrateData(spark, sourceSettings, targetSettings)
    log.info(s"Initial migration complete: $migratedCount documents migrated")

    // If streaming is enabled, start change stream processing
    if (sourceSettings.streamChanges) {
      log.info("Starting change stream processing...")
      MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.STREAMING_CHANGES)

      val stopSignal = new AtomicBoolean(false)

      // Register shutdown hook
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        log.info("Shutdown signal received - stopping change stream")
        stopSignal.set(true)
      }))

      MongoDBChangeStreamer.streamChanges(
        spark,
        sourceSettings,
        targetSettings,
        oplogTimestamp,
        stopSignal
      )
    }

    // Mark complete
    MongoDBMigrationCoordinator.updatePhase(spark, migrationId, MongoDBMigrationCoordinator.Phase.COMPLETE)
    log.info("=" * 80)
    log.info("MongoDB migration completed successfully!")
    log.info("=" * 80)
  }

  /**
   * Capture current oplog position
   */
  private def captureOplogPosition(settings: com.scylladb.migrator.config.MongoDBSourceSettings): BsonTimestamp = {
    import com.mongodb.client.MongoClients
    import org.bson.Document

    val client = MongoClients.create(settings.connectionUri)
    try {
      val adminDb = client.getDatabase("admin")
      val result = adminDb.runCommand(new Document("serverStatus", 1))

      // Get the latest optime
      val replSetStatus = result.get("oplog", classOf[Document])
      if (replSetStatus != null) {
        replSetStatus.get("latestOpTime", classOf[BsonTimestamp])
      } else {
        // Fallback: use current timestamp
        new BsonTimestamp((System.currentTimeMillis() / 1000).toInt, 0)
      }
    } finally {
      client.close()
    }
  }

  /**
   * Create the target ScyllaDB table
   */
  private def createTargetTable(
      spark: SparkSession,
      schema: MongoDBSchemaInference.InferredSchema,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    import com.datastax.spark.connector.cql.CassandraConnector

    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      // Ensure keyspace exists
      val createKeyspace = targetSettings.keyspaceReplicationStrategy match {
        case Some(strategy) =>
          s"""CREATE KEYSPACE IF NOT EXISTS ${targetSettings.keyspace}
             |WITH replication = $strategy""".stripMargin
        case None =>
          s"""CREATE KEYSPACE IF NOT EXISTS ${targetSettings.keyspace}
             |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}""".stripMargin
      }
      session.execute(createKeyspace)

      // Create table
      val createTable = schema.toCQL(targetSettings.keyspace, targetSettings.table)
      log.info(s"Creating table with CQL:\n$createTable")
      session.execute(createTable)
    }
  }
}

/**
 * Data migration logic - reads from MongoDB and writes to ScyllaDB
 */
object MongoDBDataMigration {
  private val log = LoggerFactory.getLogger(getClass)

  def migrateData(
      spark: SparkSession,
      sourceSettings: com.scylladb.migrator.config.MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla
  ): Long = {
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql.CassandraConnector
    import com.mongodb.spark.MongoSpark
    import com.mongodb.spark.config.ReadConfig
    import scala.collection.JavaConverters._

    val readConfig = ReadConfig(Map(
      "uri" -> sourceSettings.connectionUri,
      "database" -> sourceSettings.database,
      "collection" -> sourceSettings.collection,
      "readPreference.name" -> sourceSettings.readPreference,
      "partitioner" -> "MongoSamplePartitioner",
      "partitionerOptions.partitionSizeMB" -> "64"
    ))

    // Load data from MongoDB
    val mongoRdd = MongoSpark.load(spark.sparkContext, readConfig)

    // Get target table schema
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val tableDef = connector.withSessionDo { session =>
      val metadata = session.getMetadata
      val keyspaceMeta = metadata.getKeyspace(targetSettings.keyspace).get()
      val tableMeta = keyspaceMeta.getTable(targetSettings.table).get()
      tableMeta.getColumns.keySet().asScala.toSeq.map(_.toString)
    }

    val columnNames = tableDef
    val broadcastColumns = spark.sparkContext.broadcast(columnNames)
    val rowsWritten = spark.sparkContext.longAccumulator("MongoDB rows written")

    // Transform and write
    mongoRdd.foreachPartition { partition =>
      val conn = CassandraConnector(spark.sparkContext.getConf)
      val columns = broadcastColumns.value

      conn.withSessionDo { session =>
        val insertQuery = s"""INSERT INTO ${targetSettings.keyspace}.${targetSettings.table}
                            |(${columns.mkString(", ")})
                            |VALUES (${columns.map(_ => "?").mkString(", ")})""".stripMargin

        val preparedStmt = session.prepare(insertQuery)

        partition.foreach { doc =>
          try {
            val values = columns.map { col =>
              extractAndConvertValue(doc, col)
            }

            session.execute(preparedStmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
            rowsWritten.add(1)

          } catch {
            case e: Exception =>
              log.warn(s"Failed to write document: ${e.getMessage}")
          }
        }
      }
    }

    rowsWritten.value
  }

  private def extractAndConvertValue(doc: org.bson.Document, columnName: String): Any = {
    import scala.collection.JavaConverters._

    // Handle escaped field names
    val cleanName = columnName
      .replace("_dollar_", "$")
      .replace("_dot_", ".")
      .stripPrefix("\"")
      .stripSuffix("\"")

    // Handle flattened nested fields
    val value = if (cleanName.contains("_") && !cleanName.startsWith("_")) {
      val parts = cleanName.split("_")
      var current: Any = doc
      for (part <- parts if current != null) {
        current = current match {
          case d: org.bson.Document => d.get(part)
          case _ => null
        }
      }
      current
    } else {
      doc.get(cleanName)
    }

    convertValue(value)
  }

  private def convertValue(value: Any): Any = {
    import scala.collection.JavaConverters._

    if (value == null) return null

    value match {
      case oid: org.bson.types.ObjectId => oid.toHexString
      case doc: org.bson.Document => doc.toJson()
      case list: java.util.List[_] => list.asScala.map(convertValue).asJava
      case binary: org.bson.types.Binary => java.nio.ByteBuffer.wrap(binary.getData)
      case decimal: org.bson.types.Decimal128 => decimal.bigDecimalValue()
      case date: java.util.Date => new java.sql.Timestamp(date.getTime)
      case ts: org.bson.BsonTimestamp => new java.sql.Timestamp(ts.getTime * 1000L)
      case other => other
    }
  }
}
