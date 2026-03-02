package com.scylladb.migrator

import com.scylladb.migrator.config._
import com.scylladb.migrator.mariadb.NativeBridge
import com.scylladb.migrator.mariadb.NativeBridge._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

/**
 * MariaDB to ScyllaDB migrator using native C++ bridge.
 * 
 * This migrator performs a consistent snapshot migration followed by
 * continuous binlog streaming for CDC.
 * 
 * Migration phases:
 * 1. Worker 0 acquires backup stage lock and captures GTID position
 * 2. Worker 0 creates target table in ScyllaDB
 * 3. All workers perform parallel snapshot transfer
 * 4. Worker 0 releases backup stage lock
 * 5. Worker 0 streams binlog for ongoing changes
 */
object MariaDBMigrator {
  private val log = LoggerFactory.getLogger(classOf[MariaDBMigrator.type])
  
  /**
   * State shared across workers via Spark broadcast
   */
  case class MigrationState(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    schema: MariaDBTableSchema,
    startGtid: GTIDPosition,
    pkRanges: List[PKRange]
  )
  
  /**
   * Run the MariaDB to ScyllaDB migration
   */
  def migrate(
    spark: SparkSession,
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): Unit = {
    val sc = spark.sparkContext
    
    log.info(s"Starting MariaDB migration from ${sourceSettings.host}:${sourceSettings.port}/${sourceSettings.database}.${sourceSettings.table}")
    log.info(s"Target: ${targetSettings.host}:${targetSettings.port}/${targetSettings.keyspace}.${targetSettings.table}")
    
    // Phase 1: Initialize on driver (acts as worker 0)
    val migrationState = initializeMigration(sourceSettings, targetSettings, renames)
    
    // Broadcast state to all workers
    val broadcastState = sc.broadcast(migrationState)
    
    // Phase 2: Parallel snapshot transfer
    log.info(s"Starting parallel snapshot transfer with ${migrationState.pkRanges.size} ranges")
    runSnapshotPhase(sc, broadcastState)
    
    // Phase 3: Release backup stage lock (driver only)
    releaseBackupStageLock(sourceSettings)
    
    // Phase 4: Binlog streaming (driver only, optional)
    if (sourceSettings.streamBinlog) {
      log.info("Starting binlog streaming phase")
      runBinlogStreamingPhase(sourceSettings, targetSettings, migrationState.startGtid)
    } else {
      log.info("Binlog streaming disabled, migration complete")
    }
    
    log.info("Migration completed successfully")
  }
  
  /**
   * Phase 1: Initialize migration - executed on driver (worker 0)
   * - Acquire backup stage lock
   * - Capture GTID position
   * - Get schema and create target table
   * - Calculate PK ranges
   */
  private def initializeMigration(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): MigrationState = {
    log.info("Initializing migration on driver node")
    
    val mariadb = new MariaDBConnectionWrapper(sourceSettings)
    
    try {
      // Connect to MariaDB
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Failed to connect to MariaDB: ${mariadb.getError()}")
      }
      log.info("Connected to MariaDB")
      
      // Acquire backup stage lock
      if (sourceSettings.useBackupStage) {
        log.info("Executing BACKUP STAGE START")
        if (!mariadb.executeBackupStageStart()) {
          throw new RuntimeException(s"BACKUP STAGE START failed: ${mariadb.getError()}")
        }
        
        log.info("Executing BACKUP STAGE BLOCK_COMMIT")
        if (!mariadb.executeBackupStageBlockCommit()) {
          throw new RuntimeException(s"BACKUP STAGE BLOCK_COMMIT failed: ${mariadb.getError()}")
        }
      } else {
        log.warn("BACKUP STAGE disabled - snapshot may not be consistent")
      }
      
      // Capture GTID position
      val startGtid = mariadb.getGTIDPosition() match {
        case Right(gtid) =>
          log.info(s"Captured GTID position: ${gtid.gtidBinlogPos}")
          gtid
        case Left(err) =>
          throw new RuntimeException(s"Failed to get GTID position: $err")
      }
      
      // Get table schema
      val schema = mariadb.getSchema() match {
        case Right(s) =>
          log.info(s"Retrieved schema: ${s.columns.size} columns, PK: ${s.primaryKeyColumns.mkString(", ")}")
          s
        case Left(err) =>
          throw new RuntimeException(s"Failed to get schema: $err")
      }
      
      // Create target table in ScyllaDB
      createTargetTable(targetSettings, schema, renames)
      
      // Calculate PK ranges for parallel scanning
      val pkRanges = mariadb.calculateRanges(sourceSettings.splitCount) match {
        case Right(ranges) =>
          log.info(s"Calculated ${ranges.size} PK ranges for parallel scanning")
          ranges
        case Left(err) =>
          throw new RuntimeException(s"Failed to calculate PK ranges: $err")
      }
      
      MigrationState(sourceSettings, targetSettings, schema, startGtid, pkRanges)
    } finally {
      mariadb.close()
    }
  }
  
  /**
   * Create the target table in ScyllaDB
   */
  private def createTargetTable(
    targetSettings: TargetSettings.Scylla,
    schema: MariaDBTableSchema,
    renames: Option[Renames]
  ): Unit = {
    log.info(s"Creating target table ${targetSettings.keyspace}.${targetSettings.table}")
    
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = targetSettings.host.split(",").toSeq,
      port = targetSettings.port,
      keyspace = targetSettings.keyspace,
      table = targetSettings.table,
      user = targetSettings.credentials.map(_.username),
      password = targetSettings.credentials.map(_.password)
    )
    
    try {
      if (!scylla.connect()) {
        throw new RuntimeException(s"Failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      // Check if table already exists
      if (scylla.doesTableExist()) {
        log.info("Target table already exists, skipping creation")
        return
      }
      
      // Generate and execute CREATE TABLE statement
      val targetTableName = renames.flatMap(_.table).getOrElse(targetSettings.table)
      val cql = NativeBridge.SchemaConverter.generateCreateTable(
        applyColumnRenames(schema, renames),
        targetSettings.keyspace,
        targetTableName
      )
      
      log.info(s"Creating table with CQL: $cql")
      if (!scylla.createTableWithCql(cql)) {
        throw new RuntimeException(s"Failed to create table: ${scylla.getError()}")
      }
      
      log.info("Target table created successfully")
    } finally {
      scylla.close()
    }
  }
  
  /**
   * Apply column renames to schema
   */
  private def applyColumnRenames(
    schema: MariaDBTableSchema,
    renames: Option[Renames]
  ): MariaDBTableSchema = {
    renames match {
      case Some(r) if r.columns.nonEmpty =>
        val renameMap = r.columns.map(cr => cr.from -> cr.to).toMap
        val renamedColumns = schema.columns.map { col =>
          renameMap.get(col.name) match {
            case Some(newName) => col.copy(name = newName)
            case None => col
          }
        }
        val renamedPKs = schema.primaryKeyColumns.map { pk =>
          renameMap.getOrElse(pk, pk)
        }
        schema.copy(columns = renamedColumns, primaryKeyColumns = renamedPKs)
      case _ => schema
    }
  }
  
  /**
   * Phase 2: Parallel snapshot transfer
   */
  private def runSnapshotPhase(
    sc: SparkContext,
    broadcastState: Broadcast[MigrationState]
  ): Unit = {
    val state = broadcastState.value
    
    // Create RDD of PK ranges to process
    val rangesRDD = sc.parallelize(state.pkRanges, state.pkRanges.size)
    
    // Process each range in parallel
    val stats = rangesRDD.mapPartitions { rangeIterator =>
      val ranges = rangeIterator.toList
      if (ranges.isEmpty) {
        Iterator.single((0L, 0L, 0L))
      } else {
        // Each partition processes its assigned range(s)
        val localState = broadcastState.value
        processRanges(localState, ranges)
      }
    }.reduce { case ((r1, w1, e1), (r2, w2, e2)) =>
      (r1 + r2, w1 + w2, e1 + e2)
    }
    
    log.info(s"Snapshot phase complete: ${stats._1} rows read, ${stats._2} rows written, ${stats._3} errors")
  }
  
  /**
   * Process PK ranges on a worker
   */
  private def processRanges(
    state: MigrationState,
    ranges: List[PKRange]
  ): Iterator[(Long, Long, Long)] = {
    var totalRead = 0L
    var totalWritten = 0L
    var totalErrors = 0L
    
    // Create connections for this worker
    val mariadb = new MariaDBConnectionWrapper(state.sourceSettings)
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = state.targetSettings.host.split(",").toSeq,
      port = state.targetSettings.port,
      keyspace = state.targetSettings.keyspace,
      table = state.targetSettings.table,
      user = state.targetSettings.credentials.map(_.username),
      password = state.targetSettings.credentials.map(_.password)
    )
    
    try {
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Worker failed to connect to MariaDB: ${mariadb.getError()}")
      }
      if (!scylla.connect()) {
        throw new RuntimeException(s"Worker failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      // Process each assigned range
      for (range <- ranges) {
        val batchBuffer = new ArrayBuffer[Map[String, String]]()
        
        val rowsRead = mariadb.scanTableRange(range, state.sourceSettings.fetchSize) { row =>
          batchBuffer += row
          totalRead += 1
          
          // Write batch when buffer is full
          if (batchBuffer.size >= state.sourceSettings.fetchSize) {
            val written = scylla.writeBatchRows(batchBuffer.toSeq)
            totalWritten += written
            if (written < batchBuffer.size) {
              totalErrors += (batchBuffer.size - written)
            }
            batchBuffer.clear()
          }
        }
        
        // Write remaining rows
        if (batchBuffer.nonEmpty) {
          val written = scylla.writeBatchRows(batchBuffer.toSeq)
          totalWritten += written
          if (written < batchBuffer.size) {
            totalErrors += (batchBuffer.size - written)
          }
        }
        
        LoggerFactory.getLogger(classOf[MariaDBMigrator.type])
          .info(s"Completed range ${range.rangeId}: $rowsRead rows")
      }
    } finally {
      mariadb.close()
      scylla.close()
    }
    
    Iterator.single((totalRead, totalWritten, totalErrors))
  }
  
  /**
   * Release backup stage lock
   */
  private def releaseBackupStageLock(sourceSettings: MariaDBSourceSettings): Unit = {
    if (!sourceSettings.useBackupStage) {
      return
    }
    
    log.info("Releasing backup stage lock")
    
    val mariadb = new MariaDBConnectionWrapper(sourceSettings)
    try {
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Failed to connect for BACKUP STAGE END: ${mariadb.getError()}")
      }
      
      if (!mariadb.executeBackupStageEnd()) {
        throw new RuntimeException(s"BACKUP STAGE END failed: ${mariadb.getError()}")
      }
      
      log.info("Backup stage lock released")
    } finally {
      mariadb.close()
    }
  }
  
  /**
   * Phase 4: Binlog streaming for CDC
   * This runs on the driver only and streams continuously until stopped
   */
  private def runBinlogStreamingPhase(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    startGtid: GTIDPosition
  ): Unit = {
    log.info(s"Starting binlog streaming from GTID: ${startGtid.gtidBinlogPos}")
    
    val binlogStreamer = new BinlogStreamerWrapper(
      sourceSettings.host,
      sourceSettings.port,
      sourceSettings.credentials.user,
      sourceSettings.credentials.password
    )
    
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = targetSettings.host.split(",").toSeq,
      port = targetSettings.port,
      keyspace = targetSettings.keyspace,
      table = targetSettings.table,
      user = targetSettings.credentials.map(_.username),
      password = targetSettings.credentials.map(_.password)
    )
    
    val running = new AtomicBoolean(true)
    val eventsProcessed = new AtomicLong(0)
    
    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      log.info("Shutdown signal received, stopping binlog streaming")
      running.set(false)
    }))
    
    try {
      if (!scylla.connect()) {
        throw new RuntimeException(s"Failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      if (!binlogStreamer.start(startGtid)) {
        throw new RuntimeException("Failed to start binlog streaming")
      }
      
      log.info("Binlog streaming started")
      
      var lastGroupCommitId = 0L
      val groupBuffer = new ArrayBuffer[BinlogEvent]()
      
      while (running.get()) {
        binlogStreamer.getEvents(batchSize = 100, timeoutMs = 1000) match {
          case Right(events) if events.nonEmpty =>
            for (event <- events) {
              // Buffer events in the same commit group
              if (event.groupCommitId != lastGroupCommitId && groupBuffer.nonEmpty) {
                // Process previous group (can be parallel within group)
                processCommitGroup(scylla, groupBuffer.toList)
                groupBuffer.clear()
              }
              
              groupBuffer += event
              lastGroupCommitId = event.groupCommitId
              
              if (event.isLastInGroup) {
                processCommitGroup(scylla, groupBuffer.toList)
                groupBuffer.clear()
              }
              
              eventsProcessed.incrementAndGet()
            }
            
            if (eventsProcessed.get() % 10000 == 0) {
              log.info(s"Processed ${eventsProcessed.get()} binlog events")
            }
            
          case Right(_) =>
            // No events, continue waiting
            
          case Left(err) =>
            log.warn(s"Error getting binlog events: $err")
            Thread.sleep(1000) // Back off on error
        }
      }
      
      // Process any remaining buffered events
      if (groupBuffer.nonEmpty) {
        processCommitGroup(scylla, groupBuffer.toList)
      }
      
      log.info(s"Binlog streaming stopped. Total events processed: ${eventsProcessed.get()}")
      
    } finally {
      binlogStreamer.close()
      scylla.close()
    }
  }
  
  /**
   * Process a commit group of binlog events
   * Events within the same commit group can be applied in parallel
   */
  private def processCommitGroup(
    scylla: ScyllaDBConnectionWrapper,
    events: List[BinlogEvent]
  ): Unit = {
    // For now, apply sequentially
    // In production, could parallelize within commit group
    for (event <- events) {
      if (!scylla.applyEvent(event)) {
        log.warn(s"Failed to apply binlog event: ${event.eventType} on ${event.tableName}")
      }
    }
  }
}

/**
 * Entry point for MariaDB migration
 */
class MariaDBMigrator
package com.scylladb.migrator

import com.scylladb.migrator.config._
import com.scylladb.migrator.mariadb.NativeBridge
import com.scylladb.migrator.mariadb.NativeBridge._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

/**
 * MariaDB to ScyllaDB migrator using native C++ bridge.
 * 
 * This migrator performs a consistent snapshot migration followed by
 * continuous binlog streaming for CDC.
 * 
 * Migration phases:
 * 1. Worker 0 acquires backup stage lock and captures GTID position
 * 2. Worker 0 creates target table in ScyllaDB
 * 3. All workers perform parallel snapshot transfer
 * 4. Worker 0 releases backup stage lock
 * 5. Worker 0 streams binlog for ongoing changes
 */
object MariaDBMigrator {
  private val log = LoggerFactory.getLogger(classOf[MariaDBMigrator.type])
  
  /**
   * State shared across workers via Spark broadcast
   */
  case class MigrationState(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    schema: MariaDBTableSchema,
    startGtid: GTIDPosition,
    pkRanges: List[PKRange]
  )
  
  /**
   * Run the MariaDB to ScyllaDB migration
   */
  def migrate(
    spark: SparkSession,
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): Unit = {
    val sc = spark.sparkContext
    
    log.info(s"Starting MariaDB migration from ${sourceSettings.host}:${sourceSettings.port}/${sourceSettings.database}.${sourceSettings.table}")
    log.info(s"Target: ${targetSettings.host}:${targetSettings.port}/${targetSettings.keyspace}.${targetSettings.table}")
    
    // Phase 1: Initialize on driver (acts as worker 0)
    val migrationState = initializeMigration(sourceSettings, targetSettings, renames)
    
    // Broadcast state to all workers
    val broadcastState = sc.broadcast(migrationState)
    
    // Phase 2: Parallel snapshot transfer
    log.info(s"Starting parallel snapshot transfer with ${migrationState.pkRanges.size} ranges")
    runSnapshotPhase(sc, broadcastState)
    
    // Phase 3: Release backup stage lock (driver only)
    releaseBackupStageLock(sourceSettings)
    
    // Phase 4: Binlog streaming (driver only, optional)
    if (sourceSettings.streamBinlog) {
      log.info("Starting binlog streaming phase")
      runBinlogStreamingPhase(sourceSettings, targetSettings, migrationState.startGtid)
    } else {
      log.info("Binlog streaming disabled, migration complete")
    }
    
    log.info("Migration completed successfully")
  }
  
  /**
   * Phase 1: Initialize migration - executed on driver (worker 0)
   * - Acquire backup stage lock
   * - Capture GTID position
   * - Get schema and create target table
   * - Calculate PK ranges
   */
  private def initializeMigration(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): MigrationState = {
    log.info("Initializing migration on driver node")
    
    val mariadb = new MariaDBConnectionWrapper(sourceSettings)
    
    try {
      // Connect to MariaDB
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Failed to connect to MariaDB: ${mariadb.getError()}")
      }
      log.info("Connected to MariaDB")
      
      // Acquire backup stage lock
      if (sourceSettings.useBackupStage) {
        log.info("Executing BACKUP STAGE START")
        if (!mariadb.executeBackupStageStart()) {
          throw new RuntimeException(s"BACKUP STAGE START failed: ${mariadb.getError()}")
        }
        
        log.info("Executing BACKUP STAGE BLOCK_COMMIT")
        if (!mariadb.executeBackupStageBlockCommit()) {
          throw new RuntimeException(s"BACKUP STAGE BLOCK_COMMIT failed: ${mariadb.getError()}")
        }
      } else {
        log.warn("BACKUP STAGE disabled - snapshot may not be consistent")
      }
      
      // Capture GTID position
      val startGtid = mariadb.getGTIDPosition() match {
        case Right(gtid) =>
          log.info(s"Captured GTID position: ${gtid.gtidBinlogPos}")
          gtid
        case Left(err) =>
          throw new RuntimeException(s"Failed to get GTID position: $err")
      }
      
      // Get table schema
      val schema = mariadb.getSchema() match {
        case Right(s) =>
          log.info(s"Retrieved schema: ${s.columns.size} columns, PK: ${s.primaryKeyColumns.mkString(", ")}")
          s
        case Left(err) =>
          throw new RuntimeException(s"Failed to get schema: $err")
      }
      
      // Create target table in ScyllaDB
      createTargetTable(targetSettings, schema, renames)
      
      // Calculate PK ranges for parallel scanning
      val pkRanges = mariadb.calculateRanges(sourceSettings.splitCount) match {
        case Right(ranges) =>
          log.info(s"Calculated ${ranges.size} PK ranges for parallel scanning")
          ranges
        case Left(err) =>
          throw new RuntimeException(s"Failed to calculate PK ranges: $err")
      }
      
      MigrationState(sourceSettings, targetSettings, schema, startGtid, pkRanges)
    } finally {
      mariadb.close()
    }
  }
  
  /**
   * Create the target table in ScyllaDB
   */
  private def createTargetTable(
    targetSettings: TargetSettings.Scylla,
    schema: MariaDBTableSchema,
    renames: Option[Renames]
  ): Unit = {
    log.info(s"Creating target table ${targetSettings.keyspace}.${targetSettings.table}")
    
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = targetSettings.host.split(",").toSeq,
      port = targetSettings.port,
      keyspace = targetSettings.keyspace,
      table = targetSettings.table,
      user = targetSettings.credentials.map(_.username),
      password = targetSettings.credentials.map(_.password)
    )
    
    try {
      if (!scylla.connect()) {
        throw new RuntimeException(s"Failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      // Check if table already exists
      if (scylla.doesTableExist()) {
        log.info("Target table already exists, skipping creation")
        return
      }
      
      // Generate and execute CREATE TABLE statement
      val targetTableName = renames.flatMap(_.table).getOrElse(targetSettings.table)
      val cql = NativeBridge.SchemaConverter.generateCreateTable(
        applyColumnRenames(schema, renames),
        targetSettings.keyspace,
        targetTableName
      )
      
      log.info(s"Creating table with CQL: $cql")
      if (!scylla.createTableWithCql(cql)) {
        throw new RuntimeException(s"Failed to create table: ${scylla.getError()}")
      }
      
      log.info("Target table created successfully")
    } finally {
      scylla.close()
    }
  }
  
  /**
   * Apply column renames to schema
   */
  private def applyColumnRenames(
    schema: MariaDBTableSchema,
    renames: Option[Renames]
  ): MariaDBTableSchema = {
    renames match {
      case Some(r) if r.columns.nonEmpty =>
        val renameMap = r.columns.map(cr => cr.from -> cr.to).toMap
        val renamedColumns = schema.columns.map { col =>
          renameMap.get(col.name) match {
            case Some(newName) => col.copy(name = newName)
            case None => col
          }
        }
        val renamedPKs = schema.primaryKeyColumns.map { pk =>
          renameMap.getOrElse(pk, pk)
        }
        schema.copy(columns = renamedColumns, primaryKeyColumns = renamedPKs)
      case _ => schema
    }
  }
  
  /**
   * Phase 2: Parallel snapshot transfer
   */
  private def runSnapshotPhase(
    sc: SparkContext,
    broadcastState: Broadcast[MigrationState]
  ): Unit = {
    val state = broadcastState.value
    
    // Create RDD of PK ranges to process
    val rangesRDD = sc.parallelize(state.pkRanges, state.pkRanges.size)
    
    // Process each range in parallel
    val stats = rangesRDD.mapPartitions { rangeIterator =>
      val ranges = rangeIterator.toList
      if (ranges.isEmpty) {
        Iterator.single((0L, 0L, 0L))
      } else {
        // Each partition processes its assigned range(s)
        val localState = broadcastState.value
        processRanges(localState, ranges)
      }
    }.reduce { case ((r1, w1, e1), (r2, w2, e2)) =>
      (r1 + r2, w1 + w2, e1 + e2)
    }
    
    log.info(s"Snapshot phase complete: ${stats._1} rows read, ${stats._2} rows written, ${stats._3} errors")
  }
  
  /**
   * Process PK ranges on a worker
   */
  private def processRanges(
    state: MigrationState,
    ranges: List[PKRange]
  ): Iterator[(Long, Long, Long)] = {
    var totalRead = 0L
    var totalWritten = 0L
    var totalErrors = 0L
    
    // Create connections for this worker
    val mariadb = new MariaDBConnectionWrapper(state.sourceSettings)
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = state.targetSettings.host.split(",").toSeq,
      port = state.targetSettings.port,
      keyspace = state.targetSettings.keyspace,
      table = state.targetSettings.table,
      user = state.targetSettings.credentials.map(_.username),
      password = state.targetSettings.credentials.map(_.password)
    )
    
    try {
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Worker failed to connect to MariaDB: ${mariadb.getError()}")
      }
      if (!scylla.connect()) {
        throw new RuntimeException(s"Worker failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      // Process each assigned range
      for (range <- ranges) {
        val batchBuffer = new ArrayBuffer[Map[String, String]]()
        
        val rowsRead = mariadb.scanTableRange(range, state.sourceSettings.fetchSize) { row =>
          batchBuffer += row
          totalRead += 1
          
          // Write batch when buffer is full
          if (batchBuffer.size >= state.sourceSettings.fetchSize) {
            val written = scylla.writeBatchRows(batchBuffer.toSeq)
            totalWritten += written
            if (written < batchBuffer.size) {
              totalErrors += (batchBuffer.size - written)
            }
            batchBuffer.clear()
          }
        }
        
        // Write remaining rows
        if (batchBuffer.nonEmpty) {
          val written = scylla.writeBatchRows(batchBuffer.toSeq)
          totalWritten += written
          if (written < batchBuffer.size) {
            totalErrors += (batchBuffer.size - written)
          }
        }
        
        LoggerFactory.getLogger(classOf[MariaDBMigrator.type])
          .info(s"Completed range ${range.rangeId}: $rowsRead rows")
      }
    } finally {
      mariadb.close()
      scylla.close()
    }
    
    Iterator.single((totalRead, totalWritten, totalErrors))
  }
  
  /**
   * Release backup stage lock
   */
  private def releaseBackupStageLock(sourceSettings: MariaDBSourceSettings): Unit = {
    if (!sourceSettings.useBackupStage) {
      return
    }
    
    log.info("Releasing backup stage lock")
    
    val mariadb = new MariaDBConnectionWrapper(sourceSettings)
    try {
      if (!mariadb.connect()) {
        throw new RuntimeException(s"Failed to connect for BACKUP STAGE END: ${mariadb.getError()}")
      }
      
      if (!mariadb.executeBackupStageEnd()) {
        throw new RuntimeException(s"BACKUP STAGE END failed: ${mariadb.getError()}")
      }
      
      log.info("Backup stage lock released")
    } finally {
      mariadb.close()
    }
  }
  
  /**
   * Phase 4: Binlog streaming for CDC
   * This runs on the driver only and streams continuously until stopped
   */
  private def runBinlogStreamingPhase(
    sourceSettings: MariaDBSourceSettings,
    targetSettings: TargetSettings.Scylla,
    startGtid: GTIDPosition
  ): Unit = {
    log.info(s"Starting binlog streaming from GTID: ${startGtid.gtidBinlogPos}")
    
    val binlogStreamer = new BinlogStreamerWrapper(
      sourceSettings.host,
      sourceSettings.port,
      sourceSettings.credentials.user,
      sourceSettings.credentials.password
    )
    
    val scylla = new ScyllaDBConnectionWrapper(
      contactPoints = targetSettings.host.split(",").toSeq,
      port = targetSettings.port,
      keyspace = targetSettings.keyspace,
      table = targetSettings.table,
      user = targetSettings.credentials.map(_.username),
      password = targetSettings.credentials.map(_.password)
    )
    
    val running = new AtomicBoolean(true)
    val eventsProcessed = new AtomicLong(0)
    
    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      log.info("Shutdown signal received, stopping binlog streaming")
      running.set(false)
    }))
    
    try {
      if (!scylla.connect()) {
        throw new RuntimeException(s"Failed to connect to ScyllaDB: ${scylla.getError()}")
      }
      
      if (!binlogStreamer.start(startGtid)) {
        throw new RuntimeException("Failed to start binlog streaming")
      }
      
      log.info("Binlog streaming started")
      
      var lastGroupCommitId = 0L
      val groupBuffer = new ArrayBuffer[BinlogEvent]()
      
      while (running.get()) {
        binlogStreamer.getEvents(batchSize = 100, timeoutMs = 1000) match {
          case Right(events) if events.nonEmpty =>
            for (event <- events) {
              // Buffer events in the same commit group
              if (event.groupCommitId != lastGroupCommitId && groupBuffer.nonEmpty) {
                // Process previous group (can be parallel within group)
                processCommitGroup(scylla, groupBuffer.toList)
                groupBuffer.clear()
              }
              
              groupBuffer += event
              lastGroupCommitId = event.groupCommitId
              
              if (event.isLastInGroup) {
                processCommitGroup(scylla, groupBuffer.toList)
                groupBuffer.clear()
              }
              
              eventsProcessed.incrementAndGet()
            }
            
            if (eventsProcessed.get() % 10000 == 0) {
              log.info(s"Processed ${eventsProcessed.get()} binlog events")
            }
            
          case Right(_) =>
            // No events, continue waiting
            
          case Left(err) =>
            log.warn(s"Error getting binlog events: $err")
            Thread.sleep(1000) // Back off on error
        }
      }
      
      // Process any remaining buffered events
      if (groupBuffer.nonEmpty) {
        processCommitGroup(scylla, groupBuffer.toList)
      }
      
      log.info(s"Binlog streaming stopped. Total events processed: ${eventsProcessed.get()}")
      
    } finally {
      binlogStreamer.close()
      scylla.close()
    }
  }
  
  /**
   * Process a commit group of binlog events
   * Events within the same commit group can be applied in parallel
   */
  private def processCommitGroup(
    scylla: ScyllaDBConnectionWrapper,
    events: List[BinlogEvent]
  ): Unit = {
    // For now, apply sequentially
    // In production, could parallelize within commit group
    for (event <- events) {
      if (!scylla.applyEvent(event)) {
        log.warn(s"Failed to apply binlog event: ${event.eventType} on ${event.tableName}")
      }
    }
  }
}

/**
 * Entry point for MariaDB migration
 */
class MariaDBMigrator
