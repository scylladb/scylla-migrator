/**
 * ScyllaDB Migrator - JNI Interface for Scala Integration
 * 
 * This header defines the JNI (Java Native Interface) bindings that allow
 * the Scala/Spark code to call into the native C++ MariaDB migrator.
 * 
 * Copyright (c) 2025 ScyllaDB
 * Licensed under Apache License 2.0
 */

#ifndef MARIADB_SCYLLA_MIGRATOR_JNI_H
#define MARIADB_SCYLLA_MIGRATOR_JNI_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    nativeInit
 * Signature: ()V
 * 
 * Initialize the native library. Must be called once before any other methods.
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_nativeInit
  (JNIEnv *, jclass);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    nativeShutdown
 * Signature: ()V
 * 
 * Shutdown the native library and release all resources.
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_nativeShutdown
  (JNIEnv *, jclass);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    createMariaDBConnection
 * Signature: (Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 * 
 * Create a MariaDB connection and return a handle (pointer as long).
 * 
 * @param host MariaDB server hostname
 * @param port MariaDB server port
 * @param user Username for authentication
 * @param password Password for authentication
 * @param database Database name
 * @param table Table name
 * @return Handle to native connection object (0 on failure)
 */
JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createMariaDBConnection
  (JNIEnv *, jclass, jstring, jint, jstring, jstring, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    destroyMariaDBConnection
 * Signature: (J)V
 * 
 * Destroy a MariaDB connection.
 * 
 * @param handle Handle from createMariaDBConnection
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyMariaDBConnection
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    connectMariaDB
 * Signature: (J)Z
 * 
 * Connect to MariaDB server.
 * 
 * @param handle Handle from createMariaDBConnection
 * @return true if connected successfully
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_connectMariaDB
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    getTableSchema
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 * 
 * Get table schema as JSON string.
 * 
 * @param handle Handle from createMariaDBConnection
 * @param database Database name
 * @param table Table name
 * @return JSON string with schema information
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getTableSchema
  (JNIEnv *, jclass, jlong, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    backupStageStart
 * Signature: (J)Z
 * 
 * Execute BACKUP STAGE START.
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageStart
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    backupStageBlockCommit
 * Signature: (J)Z
 * 
 * Execute BACKUP STAGE BLOCK_COMMIT.
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageBlockCommit
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    backupStageEnd
 * Signature: (J)Z
 * 
 * Execute BACKUP STAGE END.
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageEnd
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    getGtidPosition
 * Signature: (J)Ljava/lang/String;
 * 
 * Get current GTID position as JSON string.
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getGtidPosition
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    calculatePKRanges
 * Signature: (JI)Ljava/lang/String;
 * 
 * Calculate primary key ranges for parallel scanning.
 * 
 * @param handle Handle from createMariaDBConnection
 * @param numRanges Number of ranges to split into
 * @return JSON array of range objects
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_calculatePKRanges
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    scanRange
 * Signature: (JLjava/lang/String;Ljava/lang/String;ILcom/scylladb/migrator/mariadb/RowCallback;)J
 * 
 * Scan a range of rows and call the callback for each row.
 * 
 * @param handle Handle from createMariaDBConnection
 * @param minValue Minimum PK value (inclusive)
 * @param maxValue Maximum PK value (exclusive for non-last range)
 * @param batchSize Number of rows to fetch at once
 * @param callback Java callback object to receive rows
 * @return Number of rows scanned
 */
JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_scanRange
  (JNIEnv *, jclass, jlong, jstring, jstring, jint, jobject);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    createScyllaDBConnection
 * Signature: (Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 * 
 * Create a ScyllaDB connection.
 * 
 * @param contactPoints Comma-separated list of contact points
 * @param port Native transport port
 * @param keyspace Keyspace name
 * @param table Table name
 * @param user Username (can be null)
 * @param password Password (can be null)
 * @return Handle to native connection object
 */
JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createScyllaDBConnection
  (JNIEnv *, jclass, jstring, jint, jstring, jstring, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    destroyScyllaDBConnection
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyScyllaDBConnection
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    connectScyllaDB
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_connectScyllaDB
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    createTable
 * Signature: (JLjava/lang/String;)Z
 * 
 * Create a table with the given CQL statement.
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createTable
  (JNIEnv *, jclass, jlong, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    tableExists
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_tableExists
  (JNIEnv *, jclass, jlong, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    writeBatch
 * Signature: (JLjava/lang/String;)J
 * 
 * Write a batch of rows. Rows are passed as JSON array.
 * 
 * @param handle Handle from createScyllaDBConnection
 * @param rowsJson JSON array of row objects
 * @return Number of rows written
 */
JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_writeBatch
  (JNIEnv *, jclass, jlong, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    generateCqlCreateTable
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 * 
 * Generate CQL CREATE TABLE statement from MariaDB schema.
 * 
 * @param schemaJson MariaDB schema as JSON
 * @param keyspace Target keyspace
 * @param table Target table name
 * @return CQL CREATE TABLE statement
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_generateCqlCreateTable
  (JNIEnv *, jclass, jstring, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    getLastError
 * Signature: (J)Ljava/lang/String;
 * 
 * Get last error message from a connection handle.
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getLastError
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    createBinlogStreamer
 * Signature: (Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)J
 * 
 * Create a binlog streamer for CDC.
 */
JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createBinlogStreamer
  (JNIEnv *, jclass, jstring, jint, jstring, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    destroyBinlogStreamer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyBinlogStreamer
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    startStreaming
 * Signature: (JLjava/lang/String;)Z
 * 
 * Start streaming from a GTID position (JSON format).
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_startStreaming
  (JNIEnv *, jclass, jlong, jstring);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    stopStreaming
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_stopStreaming
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    getBinlogEvents
 * Signature: (JII)Ljava/lang/String;
 * 
 * Get batch of binlog events as JSON array.
 * 
 * @param handle Binlog streamer handle
 * @param batchSize Maximum events to return
 * @param timeoutMs Timeout in milliseconds
 * @return JSON array of binlog events
 */
JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getBinlogEvents
  (JNIEnv *, jclass, jlong, jint, jint);

/*
 * Class:     com_scylladb_migrator_mariadb_NativeBridge
 * Method:    applyBinlogEvent
 * Signature: (JLjava/lang/String;)Z
 * 
 * Apply a single binlog event (JSON format) to ScyllaDB.
 */
JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_applyBinlogEvent
  (JNIEnv *, jclass, jlong, jstring);

#ifdef __cplusplus
}
#endif

#endif // MARIADB_SCYLLA_MIGRATOR_JNI_H
