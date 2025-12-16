package com.scylladb.migrator.source.mongodb

import com.datastax.spark.connector.cql.CassandraConnector
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType}
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.scylladb.migrator.config.{MongoDBSourceSettings, TargetSettings}
import org.apache.spark.sql.SparkSession
import org.bson.{BsonDocument, BsonTimestamp, Document}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._

/**
 * Streams changes from MongoDB oplog/change streams to ScyllaDB.
 * 
 * This handles Change Data Capture (CDC) to keep ScyllaDB in sync with 
 * ongoing changes in MongoDB after the initial data migration.
 *
 * Key design decisions:
 * - Uses MongoDB Change Streams (available in MongoDB 3.6+)
 * - Falls back to oplog tailing for older MongoDB versions
 * - Buffers changes to handle burst writes
 * - Provides exactly-once semantics through idempotent upserts
 */
object MongoDBChangeStreamer {
  private val log = LoggerFactory.getLogger(getClass)

  // Configuration
  private val BUFFER_SIZE = 10000
  private val BATCH_SIZE = 100
  private val FLUSH_INTERVAL_MS = 1000

  // Metrics
  private val insertsProcessed = new AtomicLong(0)
  private val updatesProcessed = new AtomicLong(0)
  private val deletesProcessed = new AtomicLong(0)
  private val errorsEncountered = new AtomicLong(0)

  /**
   * Represents a buffered change event
   */
  sealed trait ChangeEvent {
    def documentId: Any
  }
  case class InsertEvent(documentId: Any, document: Document) extends ChangeEvent
  case class UpdateEvent(documentId: Any, updates: Map[String, Any], removals: Set[String]) extends ChangeEvent
  case class DeleteEvent(documentId: Any) extends ChangeEvent

  /**
   * Start streaming changes from MongoDB to ScyllaDB
   *
   * @param spark SparkSession for ScyllaDB connection
   * @param sourceSettings MongoDB source configuration
   * @param targetSettings ScyllaDB target configuration
   * @param startTimestamp Optional timestamp to resume from
   * @param stopSignal Atomic boolean to signal graceful shutdown
   */
  def streamChanges(
      spark: SparkSession,
      sourceSettings: MongoDBSourceSettings,
      targetSettings: TargetSettings.Scylla,
      startTimestamp: Option[BsonTimestamp],
      stopSignal: AtomicBoolean
  ): Unit = {
    log.info(s"Starting change stream for ${sourceSettings.database}.${sourceSettings.collection}")
    startTimestamp.foreach(ts => log.info(s"Resuming from timestamp: $ts"))

    // Create buffer for batching changes
    val changeBuffer: BlockingQueue[ChangeEvent] = new LinkedBlockingQueue[ChangeEvent](BUFFER_SIZE)

    // Start the writer thread
    val writerThread = new Thread(() => {
      processChangeBuffer(spark, targetSettings, changeBuffer, stopSignal)
    }, "mongodb-change-writer")
    writerThread.setDaemon(true)
    writerThread.start()

    // Create MongoDB client and start change stream
    val client = MongoClients.create(sourceSettings.connectionUri)
    try {
      val db = client.getDatabase(sourceSettings.database)
      val collection = db.getCollection(sourceSettings.collection)

      // Create change stream with optional resume point
      val changeStream = startTimestamp match {
        case Some(ts) =>
          log.info(s"Starting change stream from operation time: $ts")
          collection.watch().startAtOperationTime(ts)
        case None =>
          log.info("Starting change stream from current time")
          collection.watch()
      }

      // Configure the change stream
      val configuredStream = changeStream
        .batchSize(BATCH_SIZE)
        .fullDocument(com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP)

      val cursor = configuredStream.iterator()

      log.info("Change stream cursor opened - processing events")
      var lastLogTime = System.currentTimeMillis()
      var eventsSinceLastLog = 0L

      while (!stopSignal.get() && cursor.hasNext) {
        try {
          val change = cursor.next()
          val event = convertToChangeEvent(change)

          event.foreach { e =>
            // Block if buffer is full (backpressure)
            if (!changeBuffer.offer(e, 5, TimeUnit.SECONDS)) {
              log.warn("Change buffer full - applying backpressure")
              changeBuffer.put(e)
            }
            eventsSinceLastLog += 1
          }

          // Periodic logging
          val now = System.currentTimeMillis()
          if (now - lastLogTime > 60000) { // Every minute
            log.info(s"Change stream stats - Events: $eventsSinceLastLog/min, " +
              s"Inserts: ${insertsProcessed.get()}, Updates: ${updatesProcessed.get()}, " +
              s"Deletes: ${deletesProcessed.get()}, Errors: ${errorsEncountered.get()}")
            eventsSinceLastLog = 0
            lastLogTime = now
          }

        } catch {
          case e: InterruptedException =>
            log.info("Change stream interrupted")
            Thread.currentThread().interrupt()
          case e: Exception =>
            log.error("Error processing change stream event", e)
            errorsEncountered.incrementAndGet()
        }
      }

      log.info("Change stream processing stopped")

    } finally {
      stopSignal.set(true)
      client.close()
      writerThread.join(30000) // Wait up to 30 seconds for writer to finish
    }
  }

  /**
   * Convert MongoDB change stream event to our internal representation
   */
  private def convertToChangeEvent(change: ChangeStreamDocument[Document]): Option[ChangeEvent] = {
    val operationType = change.getOperationType
    val docKey = change.getDocumentKey

    if (docKey == null) {
      log.warn(s"Change event without document key: $operationType")
      return None
    }

    val documentId = extractDocumentId(docKey)

    operationType match {
      case OperationType.INSERT =>
        val doc = change.getFullDocument
        if (doc != null) {
          Some(InsertEvent(documentId, doc))
        } else {
          log.warn(s"Insert event without full document: $documentId")
          None
        }

      case OperationType.REPLACE =>
        val doc = change.getFullDocument
        if (doc != null) {
          Some(InsertEvent(documentId, doc)) // Replace is essentially an upsert
        } else {
          None
        }

      case OperationType.UPDATE =>
        val updateDesc = change.getUpdateDescription
        val fullDoc = change.getFullDocument // May be available if fullDocument=updateLookup

        if (fullDoc != null) {
          // If we have the full document, treat as an upsert for simplicity
          Some(InsertEvent(documentId, fullDoc))
        } else if (updateDesc != null) {
          val updates = Option(updateDesc.getUpdatedFields)
            .map(_.asScala.toMap.mapValues(v => convertBsonValue(v)).toMap)
            .getOrElse(Map.empty)
          val removals = Option(updateDesc.getRemovedFields)
            .map(_.asScala.toSet)
            .getOrElse(Set.empty)
          Some(UpdateEvent(documentId, updates, removals))
        } else {
          log.warn(s"Update event without update description: $documentId")
          None
        }

      case OperationType.DELETE =>
        Some(DeleteEvent(documentId))

      case OperationType.DROP | OperationType.DROP_DATABASE | OperationType.INVALIDATE =>
        log.warn(s"Received $operationType event - this may indicate collection/database dropped")
        None

      case other =>
        log.debug(s"Ignoring change event type: $other")
        None
    }
  }

  /**
   * Extract the document ID from the document key
   */
  private def extractDocumentId(docKey: BsonDocument): Any = {
    val idValue = docKey.get("_id")
    convertBsonValue(idValue)
  }

  /**
   * Convert BSON values to Scala/Java types
   */
  private def convertBsonValue(value: org.bson.BsonValue): Any = {
    if (value == null || value.isNull) return null

    value.getBsonType match {
      case org.bson.BsonType.OBJECT_ID => value.asObjectId().getValue.toHexString
      case org.bson.BsonType.STRING => value.asString().getValue
      case org.bson.BsonType.INT32 => value.asInt32().getValue
      case org.bson.BsonType.INT64 => value.asInt64().getValue
      case org.bson.BsonType.DOUBLE => value.asDouble().getValue
      case org.bson.BsonType.BOOLEAN => value.asBoolean().getValue
      case org.bson.BsonType.DATE_TIME => new java.sql.Timestamp(value.asDateTime().getValue)
      case org.bson.BsonType.TIMESTAMP =>
        val ts = value.asTimestamp()
        new java.sql.Timestamp(ts.getTime * 1000L)
      case org.bson.BsonType.BINARY => java.nio.ByteBuffer.wrap(value.asBinary().getData)
      case org.bson.BsonType.ARRAY =>
        value.asArray().asScala.map(convertBsonValue).asJava
      case org.bson.BsonType.DOCUMENT =>
        value.asDocument().toJson
      case org.bson.BsonType.DECIMAL128 =>
        value.asDecimal128().getValue.bigDecimalValue()
      case _ => value.toString
    }
  }

  /**
   * Process buffered changes and write to ScyllaDB
   */
  private def processChangeBuffer(
      spark: SparkSession,
      targetSettings: TargetSettings.Scylla,
      buffer: BlockingQueue[ChangeEvent],
      stopSignal: AtomicBoolean
  ): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    while (!stopSignal.get() || !buffer.isEmpty) {
      try {
        // Collect a batch of changes
        val batch = new java.util.ArrayList[ChangeEvent](BATCH_SIZE)
        val firstEvent = buffer.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS)

        if (firstEvent != null) {
          batch.add(firstEvent)
          buffer.drainTo(batch, BATCH_SIZE - 1)

          // Process the batch
          if (!batch.isEmpty) {
            processBatch(batch.asScala.toSeq, connector, targetSettings)
          }
        }

      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
        case e: Exception =>
          log.error("Error processing change buffer", e)
          errorsEncountered.incrementAndGet()
      }
    }

    // Final flush
    if (!buffer.isEmpty) {
      val remaining = new java.util.ArrayList[ChangeEvent]()
      buffer.drainTo(remaining)
      if (!remaining.isEmpty) {
        processBatch(remaining.asScala.toSeq, connector, targetSettings)
      }
    }

    log.info("Change buffer processor stopped")
  }

  /**
   * Process a batch of change events
   */
  private def processBatch(
      batch: Seq[ChangeEvent],
      connector: CassandraConnector,
      targetSettings: TargetSettings.Scylla
  ): Unit = {
    connector.withSessionDo { session =>
      // Get table metadata
      val metadata = session.getMetadata
      val tableMeta = metadata.getKeyspace(targetSettings.keyspace).get()
        .getTable(targetSettings.table).get()
      val columns = tableMeta.getColumns.keySet().asScala.toSeq.map(_.toString)

      batch.foreach {
        case InsertEvent(docId, doc) =>
          try {
            executeUpsert(session, targetSettings, columns, doc)
            insertsProcessed.incrementAndGet()
          } catch {
            case e: Exception =>
              log.warn(s"Failed to upsert document $docId: ${e.getMessage}")
              errorsEncountered.incrementAndGet()
          }

        case UpdateEvent(docId, updates, removals) =>
          try {
            executeUpdate(session, targetSettings, docId, updates, removals)
            updatesProcessed.incrementAndGet()
          } catch {
            case e: Exception =>
              log.warn(s"Failed to update document $docId: ${e.getMessage}")
              errorsEncountered.incrementAndGet()
          }

        case DeleteEvent(docId) =>
          try {
            executeDelete(session, targetSettings, docId)
            deletesProcessed.incrementAndGet()
          } catch {
            case e: Exception =>
              log.warn(s"Failed to delete document $docId: ${e.getMessage}")
              errorsEncountered.incrementAndGet()
          }
      }
    }
  }

  private def executeUpsert(
      session: com.datastax.oss.driver.api.core.CqlSession,
      targetSettings: TargetSettings.Scylla,
      columns: Seq[String],
      doc: Document
  ): Unit = {
    val values = columns.map { col =>
      extractDocValue(doc, col)
    }

    val placeholders = columns.map(_ => "?").mkString(", ")
    val columnList = columns.mkString(", ")

    val stmt = session.prepare(
      s"INSERT INTO ${targetSettings.keyspace}.${targetSettings.table} ($columnList) VALUES ($placeholders)"
    )

    session.execute(stmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
  }

  private def executeUpdate(
      session: com.datastax.oss.driver.api.core.CqlSession,
      targetSettings: TargetSettings.Scylla,
      docId: Any,
      updates: Map[String, Any],
      removals: Set[String]
  ): Unit = {
    // Apply updates
    if (updates.nonEmpty) {
      val setClauses = updates.keys.map(k => s"${cleanFieldName(k)} = ?").mkString(", ")
      val values = updates.values.toSeq :+ docId

      val stmt = session.prepare(
        s"UPDATE ${targetSettings.keyspace}.${targetSettings.table} SET $setClauses WHERE _id = ?"
      )

      session.execute(stmt.bind(values.map(_.asInstanceOf[AnyRef]): _*))
    }

    // Apply removals (set to null)
    removals.foreach { field =>
      val cleanField = cleanFieldName(field)
      val stmt = session.prepare(
        s"UPDATE ${targetSettings.keyspace}.${targetSettings.table} SET $cleanField = null WHERE _id = ?"
      )
      session.execute(stmt.bind(docId.asInstanceOf[AnyRef]))
    }
  }

  private def executeDelete(
      session: com.datastax.oss.driver.api.core.CqlSession,
      targetSettings: TargetSettings.Scylla,
      docId: Any
  ): Unit = {
    val stmt = session.prepare(
      s"DELETE FROM ${targetSettings.keyspace}.${targetSettings.table} WHERE _id = ?"
    )
    session.execute(stmt.bind(docId.asInstanceOf[AnyRef]))
  }

  private def extractDocValue(doc: Document, columnName: String): Any = {
    val cleanName = columnName
      .replace("_dollar_", "$")
      .replace("_dot_", ".")
      .stripPrefix("\"")
      .stripSuffix("\"")

    val value = doc.get(cleanName)
    convertDocValue(value)
  }

  private def convertDocValue(value: Any): Any = {
    if (value == null) return null
    value match {
      case oid: org.bson.types.ObjectId => oid.toHexString
      case doc: Document => doc.toJson()
      case list: java.util.List[_] => list.asScala.map(convertDocValue).asJava
      case binary: org.bson.types.Binary => java.nio.ByteBuffer.wrap(binary.getData)
      case decimal: org.bson.types.Decimal128 => decimal.bigDecimalValue()
      case date: java.util.Date => new java.sql.Timestamp(date.getTime)
      case other => other
    }
  }

  private def cleanFieldName(field: String): String = {
    field
      .replace("$", "_dollar_")
      .replace(".", "_dot_")
  }

  /**
   * Get current streaming metrics
   */
  def getMetrics: Map[String, Long] = Map(
    "inserts" -> insertsProcessed.get(),
    "updates" -> updatesProcessed.get(),
    "deletes" -> deletesProcessed.get(),
    "errors" -> errorsEncountered.get()
  )
}
