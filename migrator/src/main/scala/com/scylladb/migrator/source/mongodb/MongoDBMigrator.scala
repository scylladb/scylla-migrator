package com.scylladb.migrator.source.mongodb

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType}
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.scylladb.migrator.config.{MigratorConfig, MongoDBSourceSettings, TargetSettings}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.bson.{BsonDocument, BsonTimestamp, BsonValue, Document}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * MongoDB to ScyllaDB migration coordinator.
 *
 * This handles the complete migration process:
 * 1. Schema inference and ScyllaDB table creation
 * 2. Initial data migration with parallel reading
 * 3. Oplog tailing for ongoing synchronization
 */
object MongoDBMigrator {
  private val log = LoggerFactory.getLogger(getClass)

  // State for coordinating between workers
  private val migrationState = new AtomicReference[MigrationState](MigrationState.Idle)
  private val tableCreated = new AtomicBoolean(false)
  private val oplogStartTimestamp = new AtomicReference[BsonTimestamp](null)
  private val initialMigrationComplete = new CountDownLatch(1)

  sealed trait MigrationState
  object MigrationState {
    case object Idle extends MigrationState
    case object InferringSchema extends MigrationState
    case object CreatingTable extends MigrationState
    case object CapturingOplogPosition extends MigrationState
    case object MigratingInitialData extends MigrationState
    case object StreamingChanges extends MigrationState
    case object Complete extends MigrationState
    case class Failed(error: Throwable) extends MigrationState
  }

  /**
   * Main entry point for MongoDB migration
   */
  def migrate(
      spark: SparkSession,
      sourceSettings: MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla,
      config: MigratorConfig
  ): Unit = {
    try {
      log.info(s"Starting MongoDB migration: ${sourceSettings.database}.${sourceSettings.collection} -> ${targetSettings.keyspace}.${targetSettings.table}")

      // Step 1: One worker performs schema inference and table creation
      // Other workers wait for this to complete
      synchronized {
        if (!tableCreated.get()) {
          performSetup(spark, sourceSettings, targetSettings)
        }
      }

      // Wait for setup to complete (for other workers)
      while (!tableCreated.get() && !migrationState.get().isInstanceOf[MigrationState.Failed]) {
        Thread.sleep(1000)
      }

      // Check if setup failed
      migrationState.get() match {
        case MigrationState.Failed(err) =>
          throw new RuntimeException("Migration setup failed", err)
        case _ => // continue
      }

      // Step 2: Migrate initial data
      migrationState.set(MigrationState.MigratingInitialData)
      val migratedCount = migrateInitialData(spark, sourceSettings, targetSettings)
      log.info(s"Initial migration complete: $migratedCount documents migrated")

      // Signal completion of initial migration
      initialMigrationComplete.countDown()

      // Step 3: Stream oplog changes if enabled
      if (sourceSettings.streamChanges) {
        migrationState.set(MigrationState.StreamingChanges)
        streamOplogChanges(spark, sourceSettings, targetSettings)
      }

      migrationState.set(MigrationState.Complete)
      log.info("MongoDB migration completed successfully")

    } catch {
      case e: Exception =>
        migrationState.set(MigrationState.Failed(e))
        log.error("MongoDB migration failed", e)
        throw e
    }
  }

  /**
   * Performs initial setup: schema inference, oplog position capture, and table creation.
   * This must be executed by exactly one worker while others wait.
   */
  private def performSetup(
      spark: SparkSession,
      sourceSettings: MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    log.info("Performing migration setup (schema inference, table creation)")

    // Step 1.1: Infer schema
    migrationState.set(MigrationState.InferringSchema)
    val inferredSchema = MongoDBSchemaInference.inferSchema(spark, sourceSettings)
    log.info(s"Inferred schema with ${inferredSchema.fields.length} fields")
    log.info(s"Partition key: ${inferredSchema.partitionKey.mkString(", ")}")
    log.info(s"Clustering keys: ${inferredSchema.clusteringKeys.mkString(", ")}")

    // Step 1.2: Capture oplog position BEFORE table creation to avoid race conditions
    // This ensures we don't miss any changes that occur between now and when we start streaming
    if (sourceSettings.streamChanges) {
      migrationState.set(MigrationState.CapturingOplogPosition)
      val startTs = captureOplogPosition(sourceSettings)
      oplogStartTimestamp.set(startTs)
      log.info(s"Captured oplog start position: $startTs")
    }

    // Step 1.3: Create ScyllaDB table
    migrationState.set(MigrationState.CreatingTable)
    createScyllaTable(spark, inferredSchema, targetSettings)
    log.info(s"Created ScyllaDB table ${targetSettings.keyspace}.${targetSettings.table}")

    tableCreated.set(true)
  }

  /**
   * Captures the current oplog position for later streaming
   */
  private def captureOplogPosition(settings: MongoDBSourceSettings): BsonTimestamp = {
    val client = createMongoClient(settings)
    try {
      val adminDb = client.getDatabase("admin")
      val result = adminDb.runCommand(new Document("serverStatus", 1))
      val opTime = result.get("oplog", classOf[Document])
        .get("latestOpTime", classOf[BsonTimestamp])
      opTime
    } finally {
      client.close()
    }
  }

  /**
   * Creates the ScyllaDB table based on inferred schema
   */
  private def createScyllaTable(
      spark: SparkSession,
      schema: MongoDBSchemaInference.InferredSchema,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      // Create keyspace if it doesn't exist
      val createKeyspaceStmt =
        s"""CREATE KEYSPACE IF NOT EXISTS ${targetSettings.keyspace}
           |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}""".stripMargin

      session.execute(createKeyspaceStmt)

      // Create table
      val createTableStmt = schema.toCQL(targetSettings.keyspace, targetSettings.table)
      log.info(s"Creating table with CQL:\n$createTableStmt")
      session.execute(createTableStmt)
    }
  }

  /**
   * Migrates initial data from MongoDB to ScyllaDB
   */
  private def migrateInitialData(
      spark: SparkSession,
      sourceSettings: MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla
  ): Long = {
    log.info("Starting initial data migration")

    val readConfig = ReadConfig(Map(
      "uri" -> sourceSettings.connectionUri,
      "database" -> sourceSettings.database,
      "collection" -> sourceSettings.collection,
      "readPreference.name" -> sourceSettings.readPreference,
      "partitioner" -> "MongoSamplePartitioner",
      "partitionerOptions.partitionSizeMB" -> "64"
    ))

    // Load data from MongoDB using Spark connector
    val mongoRdd = MongoSpark.load(spark.sparkContext, readConfig)

    // Convert to ScyllaDB-compatible format and write
    val rowsWritten = spark.sparkContext.longAccumulator("rows_written")

    // Get the actual schema from ScyllaDB to ensure we write correctly
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val tableDef = connector.withSessionDo { session =>
      Schema.fromCassandra(connector, Some(targetSettings.keyspace), Some(targetSettings.table))
        .tables
        .headOption
        .getOrElse(throw new RuntimeException(s"Table ${targetSettings.keyspace}.${targetSettings.table} not found"))
    }

    val columnNames = tableDef.columns.map(_.columnName)
    val broadcastColumnNames = spark.sparkContext.broadcast(columnNames)

    // Transform and save
    val transformedRdd = mongoRdd.mapPartitions { partition =>
      val columns = broadcastColumnNames.value
      partition.map { doc =>
        val values = columns.map { colName =>
          extractValue(doc, colName)
        }
        values
      }
    }

    // Write to ScyllaDB
    transformedRdd.foreachPartition { partition =>
      val conn = CassandraConnector(spark.sparkContext.getConf)
      val cols = broadcastColumnNames.value

      conn.withSessionDo { session =>
        val insertStmt = session.prepare(
          s"""INSERT INTO ${targetSettings.keyspace}.${targetSettings.table}
             |(${cols.mkString(", ")})
             |VALUES (${cols.map(_ => "?").mkString(", ")})""".stripMargin
        )

        partition.grouped(targetSettings.writeBatchSize.getOrElse(100)).foreach { batch =>
          batch.foreach { values =>
            try {
              session.execute(insertStmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
              rowsWritten.add(1)
            } catch {
              case e: Exception =>
                log.warn(s"Failed to insert row: ${e.getMessage}")
            }
          }
        }
      }
    }

    rowsWritten.value
  }

  /**
   * Extract a value from a MongoDB document for a given column name
   */
  private def extractValue(doc: Document, columnName: String): Any = {
    val cleanName = columnName
      .replace("_dollar_", "$")
      .replace("_dot_", ".")
      .stripPrefix("\"")
      .stripSuffix("\"")

    val value = if (cleanName.contains("_")) {
      // Handle flattened nested documents
      val parts = cleanName.split("_")
      var current: Any = doc
      for (part <- parts if current != null) {
        current = current match {
          case d: Document => d.get(part)
          case _ => null
        }
      }
      current
    } else {
      doc.get(cleanName)
    }

    convertBsonValue(value)
  }

  /**
   * Convert MongoDB/BSON values to ScyllaDB-compatible types
   */
  private def convertBsonValue(value: Any): Any = {
    if (value == null) return null

    value match {
      case oid: org.bson.types.ObjectId => oid.toHexString
      case doc: Document => doc.toJson()
      case list: java.util.List[_] => list.asScala.map(convertBsonValue).asJava
      case binary: org.bson.types.Binary => java.nio.ByteBuffer.wrap(binary.getData)
      case decimal: org.bson.types.Decimal128 => decimal.bigDecimalValue()
      case date: java.util.Date => new java.sql.Timestamp(date.getTime)
      case ts: BsonTimestamp => new java.sql.Timestamp(ts.getTime * 1000L)
      case other => other
    }
  }

  /**
   * Streams oplog changes from MongoDB to ScyllaDB
   */
  private def streamOplogChanges(
      spark: SparkSession,
      sourceSettings: MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    log.info("Starting oplog streaming")

    val startTs = oplogStartTimestamp.get()
    if (startTs == null) {
      log.warn("No oplog start timestamp captured - starting from current position")
    }

    val client = createMongoClient(sourceSettings)
    try {
      val db = client.getDatabase(sourceSettings.database)
      val collection = db.getCollection(sourceSettings.collection)

      // Create change stream starting from captured position
      val changeStream = if (startTs != null) {
        val resumeToken = new BsonDocument("_data", new org.bson.BsonString(startTs.toString))
        collection.watch().startAtOperationTime(startTs)
      } else {
        collection.watch()
      }

      val connector = CassandraConnector(spark.sparkContext.getConf)

      log.info("Change stream started - processing oplog events")

      // Process change stream events
      val cursor = changeStream.iterator()
      while (cursor.hasNext) {
        val change = cursor.next()
        processChangeEvent(change, connector, targetSettings)
      }

    } finally {
      client.close()
    }
  }

  /**
   * Process a single change stream event
   */
  private def processChangeEvent(
      change: ChangeStreamDocument[Document],
      connector: CassandraConnector,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    val operationType = change.getOperationType

    operationType match {
      case OperationType.INSERT | OperationType.REPLACE =>
        val doc = change.getFullDocument
        if (doc != null) {
          upsertDocument(doc, connector, targetSettings)
        }

      case OperationType.UPDATE =>
        val docKey = change.getDocumentKey
        val updateDesc = change.getUpdateDescription
        if (docKey != null && updateDesc != null) {
          applyUpdate(docKey, updateDesc, connector, targetSettings)
        }

      case OperationType.DELETE =>
        val docKey = change.getDocumentKey
        if (docKey != null) {
          deleteDocument(docKey, connector, targetSettings)
        }

      case other =>
        log.debug(s"Ignoring change stream event: $other")
    }
  }

  private def upsertDocument(
      doc: Document,
      connector: CassandraConnector,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    connector.withSessionDo { session =>
      // Get table metadata for column information
      val metadata = session.getMetadata
      val tableMeta = metadata.getKeyspace(targetSettings.keyspace).get()
        .getTable(targetSettings.table).get()

      val columns = tableMeta.getColumns.keySet().asScala.toSeq.map(_.toString)

      val values = columns.map { col => extractValue(doc, col) }
      val insertStmt = session.prepare(
        s"""INSERT INTO ${targetSettings.keyspace}.${targetSettings.table}
           |(${columns.mkString(", ")})
           |VALUES (${columns.map(_ => "?").mkString(", ")})""".stripMargin
      )

      try {
        session.execute(insertStmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
      } catch {
        case e: Exception =>
          log.warn(s"Failed to upsert document: ${e.getMessage}")
      }
    }
  }

  private def applyUpdate(
      docKey: BsonDocument,
      updateDesc: com.mongodb.client.model.changestream.UpdateDescription,
      connector: CassandraConnector,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    connector.withSessionDo { session =>
      val keyId = extractValue(new Document(docKey), "_id")

      // Build UPDATE statement for changed fields
      val updatedFields = updateDesc.getUpdatedFields
      if (updatedFields != null && !updatedFields.isEmpty) {
        val setClauses = updatedFields.keySet().asScala.map { field =>
          val cleanField = field
            .replace("$", "_dollar_")
            .replace(".", "_dot_")
          s"$cleanField = ?"
        }.mkString(", ")

        val values = updatedFields.keySet().asScala.map { field =>
          convertBsonValue(updatedFields.get(field))
        }.toSeq :+ keyId

        val updateStmt = session.prepare(
          s"""UPDATE ${targetSettings.keyspace}.${targetSettings.table}
             |SET $setClauses
             |WHERE _id = ?""".stripMargin
        )

        try {
          session.execute(updateStmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
        } catch {
          case e: Exception =>
            log.warn(s"Failed to apply update: ${e.getMessage}")
        }
      }

      // Handle removed fields by setting them to null
      val removedFields = updateDesc.getRemovedFields
      if (removedFields != null && !removedFields.isEmpty) {
        removedFields.asScala.foreach { field =>
          val cleanField = field
            .replace("$", "_dollar_")
            .replace(".", "_dot_")

          val nullStmt = session.prepare(
            s"""UPDATE ${targetSettings.keyspace}.${targetSettings.table}
               |SET $cleanField = null
               |WHERE _id = ?""".stripMargin
          )

          try {
            session.execute(nullStmt.bind(keyId.asInstanceOf[AnyRef]))
          } catch {
            case e: Exception =>
              log.warn(s"Failed to remove field $field: ${e.getMessage}")
          }
        }
      }
    }
  }

  private def deleteDocument(
      docKey: BsonDocument,
      connector: CassandraConnector,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    connector.withSessionDo { session =>
      val keyId = extractValue(new Document(docKey), "_id")

      val deleteStmt = session.prepare(
        s"DELETE FROM ${targetSettings.keyspace}.${targetSettings.table} WHERE _id = ?"
      )

      try {
        session.execute(deleteStmt.bind(keyId.asInstanceOf[AnyRef]))
      } catch {
        case e: Exception =>
          log.warn(s"Failed to delete document: ${e.getMessage}")
      }
    }
  }

  private def createMongoClient(settings: MongoDBSourceSettings): MongoClient = {
    MongoClients.create(settings.connectionUri)
  }
}
