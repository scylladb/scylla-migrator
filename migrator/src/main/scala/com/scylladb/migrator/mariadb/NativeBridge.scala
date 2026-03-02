package com.scylladb.migrator.mariadb

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import com.scylladb.migrator.config._

import scala.util.{Try, Success, Failure}

/**
 * Callback interface for receiving rows during table scan
 */
trait RowCallback {
  def onRow(rowJson: String): Unit
}

/**
 * Native bridge to C++ MariaDB/ScyllaDB migrator library.
 * 
 * This class provides JNI bindings to the native C++ code that performs
 * the actual database operations using MariaDB Connector/C and cpp-rs-driver.
 */
object NativeBridge {
  
  // Load the native library
  private var libraryLoaded = false
  
  def ensureLoaded(): Unit = synchronized {
    if (!libraryLoaded) {
      try {
        System.loadLibrary("mariadb-scylla-migrator-jni")
        nativeInit()
        libraryLoaded = true
      } catch {
        case e: UnsatisfiedLinkError =>
          throw new RuntimeException(
            "Failed to load native library. Ensure libmariadb-scylla-migrator-jni is in java.library.path", 
            e
          )
      }
    }
  }
  
  // Native method declarations
  @native private def nativeInit(): Unit
  @native private def nativeShutdown(): Unit
  
  // MariaDB connection methods
  @native private def createMariaDBConnection(
    host: String,
    port: Int,
    user: String,
    password: String,
    database: String,
    table: String
  ): Long
  
  @native private def destroyMariaDBConnection(handle: Long): Unit
  @native private def connectMariaDB(handle: Long): Boolean
  @native private def getTableSchema(handle: Long, database: String, table: String): String
  @native private def backupStageStart(handle: Long): Boolean
  @native private def backupStageBlockCommit(handle: Long): Boolean
  @native private def backupStageEnd(handle: Long): Boolean
  @native private def getGtidPosition(handle: Long): String
  @native private def calculatePKRanges(handle: Long, numRanges: Int): String
  @native private def scanRange(
    handle: Long,
    minValue: String,
    maxValue: String,
    batchSize: Int,
    callback: RowCallback
  ): Long
  
  // ScyllaDB connection methods
  @native private def createScyllaDBConnection(
    contactPoints: String,
    port: Int,
    keyspace: String,
    table: String,
    user: String,
    password: String
  ): Long
  
  @native private def destroyScyllaDBConnection(handle: Long): Unit
  @native private def connectScyllaDB(handle: Long): Boolean
  @native private def createTable(handle: Long, createStmt: String): Boolean
  @native private def tableExists(handle: Long, keyspace: String, table: String): Boolean
  @native private def writeBatch(handle: Long, rowsJson: String): Long
  
  // Schema conversion
  @native private def generateCqlCreateTable(
    schemaJson: String,
    keyspace: String,
    table: String
  ): String
  
  // Error handling
  @native private def getLastError(handle: Long): String
  
  // Binlog streaming
  @native private def createBinlogStreamer(
    host: String,
    port: Int,
    user: String,
    password: String
  ): Long
  
  @native private def destroyBinlogStreamer(handle: Long): Unit
  @native private def startStreaming(handle: Long, gtidJson: String): Boolean
  @native private def stopStreaming(handle: Long): Unit
  @native private def getBinlogEvents(handle: Long, batchSize: Int, timeoutMs: Int): String
  @native private def applyBinlogEvent(scyllaHandle: Long, eventJson: String): Boolean
  
  /**
   * High-level wrapper for MariaDB connection
   */
  class MariaDBConnectionWrapper(settings: MariaDBSourceSettings) extends AutoCloseable {
    ensureLoaded()
    
    private val handle = createMariaDBConnection(
      settings.host,
      settings.port,
      settings.credentials.user,
      settings.credentials.password,
      settings.database,
      settings.table
    )
    
    if (handle == 0) {
      throw new RuntimeException("Failed to create MariaDB connection handle")
    }
    
    def connect(): Boolean = connectMariaDB(handle)
    
    def getSchema(): Either[String, MariaDBTableSchema] = {
      val json = getTableSchema(handle, settings.database, settings.table)
      if (json == null) {
        Left(getError())
      } else {
        decode[MariaDBTableSchema](json).left.map(_.getMessage)
      }
    }
    
    def executeBackupStageStart(): Boolean = backupStageStart(handle)
    def executeBackupStageBlockCommit(): Boolean = backupStageBlockCommit(handle)
    def executeBackupStageEnd(): Boolean = backupStageEnd(handle)
    
    def getGTIDPosition(): Either[String, GTIDPosition] = {
      val json = getGtidPosition(handle)
      if (json == null) {
        Left(getError())
      } else {
        decode[GTIDPosition](json).left.map(_.getMessage)
      }
    }
    
    def calculateRanges(numRanges: Int): Either[String, List[PKRange]] = {
      val json = calculatePKRanges(handle, numRanges)
      if (json == null) {
        Left(getError())
      } else {
        decode[List[PKRange]](json).left.map(_.getMessage)
      }
    }
    
    def scanTableRange(
      range: PKRange,
      batchSize: Int
    )(callback: Map[String, String] => Unit): Long = {
      val rowCallback = new RowCallback {
        override def onRow(rowJson: String): Unit = {
          decode[Map[String, String]](rowJson) match {
            case Right(row) => callback(row)
            case Left(err) => 
              System.err.println(s"Failed to parse row JSON: ${err.getMessage}")
          }
        }
      }
      
      scanRange(
        handle,
        range.minValue,
        range.maxValue,
        batchSize,
        rowCallback
      )
    }
    
    def getError(): String = Option(getLastError(handle)).getOrElse("Unknown error")
    
    override def close(): Unit = {
      destroyMariaDBConnection(handle)
    }
  }
  
  /**
   * High-level wrapper for ScyllaDB connection
   */
  class ScyllaDBConnectionWrapper(
    contactPoints: Seq[String],
    port: Int,
    keyspace: String,
    table: String,
    user: Option[String] = None,
    password: Option[String] = None
  ) extends AutoCloseable {
    ensureLoaded()
    
    private val handle = createScyllaDBConnection(
      contactPoints.mkString(","),
      port,
      keyspace,
      table,
      user.orNull,
      password.orNull
    )
    
    if (handle == 0) {
      throw new RuntimeException("Failed to create ScyllaDB connection handle")
    }
    
    def connect(): Boolean = connectScyllaDB(handle)
    
    def createTableFromSchema(schema: MariaDBTableSchema): Boolean = {
      val cql = generateCqlCreateTable(
        schema.asJson.noSpaces,
        keyspace,
        table
      )
      createTable(handle, cql)
    }
    
    def createTableWithCql(cql: String): Boolean = createTable(handle, cql)
    
    def doesTableExist(): Boolean = tableExists(handle, keyspace, table)
    
    def writeBatchRows(rows: Seq[Map[String, String]]): Long = {
      val json = rows.asJson.noSpaces
      writeBatch(handle, json)
    }
    
    def applyEvent(event: BinlogEvent): Boolean = {
      applyBinlogEvent(handle, event.asJson.noSpaces)
    }
    
    def getError(): String = Option(getLastError(handle)).getOrElse("Unknown error")
    
    override def close(): Unit = {
      destroyScyllaDBConnection(handle)
    }
  }
  
  /**
   * High-level wrapper for Binlog streaming
   */
  class BinlogStreamerWrapper(
    host: String,
    port: Int,
    user: String,
    password: String
  ) extends AutoCloseable {
    ensureLoaded()
    
    private val handle = createBinlogStreamer(host, port, user, password)
    
    if (handle == 0) {
      throw new RuntimeException("Failed to create binlog streamer handle")
    }
    
    def start(gtid: GTIDPosition): Boolean = {
      startStreaming(handle, gtid.asJson.noSpaces)
    }
    
    def stop(): Unit = stopStreaming(handle)
    
    def getEvents(batchSize: Int, timeoutMs: Int): Either[String, List[BinlogEvent]] = {
      val json = getBinlogEvents(handle, batchSize, timeoutMs)
      if (json == null) {
        Left("Failed to get binlog events")
      } else {
        decode[List[BinlogEvent]](json).left.map(_.getMessage)
      }
    }
    
    override def close(): Unit = {
      stopStreaming(handle)
      destroyBinlogStreamer(handle)
    }
  }
  
  /**
   * Utility methods
   */
  object SchemaConverter {
    def mariaDBToCqlType(column: MariaDBColumn): String = {
      val dataType = column.dataType.toLowerCase
      dataType match {
        case "tinyint" => "tinyint"
        case "smallint" => "smallint"
        case "mediumint" | "int" | "integer" => "int"
        case "bigint" => "bigint"
        case "float" => "float"
        case "double" | "real" => "double"
        case "decimal" | "numeric" => "decimal"
        case "boolean" | "bool" => "boolean"
        case "date" => "date"
        case "time" => "time"
        case "datetime" | "timestamp" => "timestamp"
        case "year" => "int"
        case "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" => "text"
        case "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "blob"
        case "uuid" => "uuid"
        case "json" => "text"
        case "enum" | "set" => "text"
        case _ => "text"
      }
    }
    
    def generateCreateTable(
      schema: MariaDBTableSchema,
      keyspace: String,
      tableName: String
    ): String = {
      val columns = schema.columns.map { col =>
        s""""${col.name}" ${mariaDBToCqlType(col)}"""
      }.mkString(",\n    ")
      
      val pk = schema.primaryKeyColumns.map(c => s""""$c"""").mkString(", ")
      
      s"""CREATE TABLE IF NOT EXISTS "$keyspace"."$tableName" (
         |    $columns,
         |    PRIMARY KEY ($pk)
         |);""".stripMargin
    }
  }
  
  def shutdown(): Unit = synchronized {
    if (libraryLoaded) {
      nativeShutdown()
      libraryLoaded = false
    }
  }
}
