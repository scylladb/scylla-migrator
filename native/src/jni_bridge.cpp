/**
 * ScyllaDB Migrator - JNI Implementation
 * 
 * Implements the JNI bindings for calling native C++ code from Scala/Java.
 * 
 * Copyright (c) 2025 ScyllaDB
 * Licensed under Apache License 2.0
 */

#include "mariadb_scylla_migrator_jni.h"
#include "mariadb_scylla_migrator.h"

#include <string>
#include <sstream>
#include <memory>
#include <mutex>
#include <unordered_map>

using namespace scylla_migrator::mariadb;

// Global state for the native library
static std::mutex g_mutex;
static bool g_initialized = false;

// Helper to convert jstring to std::string
static std::string jstring_to_string(JNIEnv* env, jstring jstr) {
    if (!jstr) return "";
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
}

// Helper to convert std::string to jstring
static jstring string_to_jstring(JNIEnv* env, const std::string& str) {
    return env->NewStringUTF(str.c_str());
}

// Simple JSON serialization helpers (production would use a proper JSON library)
static std::string escape_json_string(const std::string& s) {
    std::ostringstream o;
    for (char c : s) {
        switch (c) {
            case '"': o << "\\\""; break;
            case '\\': o << "\\\\"; break;
            case '\b': o << "\\b"; break;
            case '\f': o << "\\f"; break;
            case '\n': o << "\\n"; break;
            case '\r': o << "\\r"; break;
            case '\t': o << "\\t"; break;
            default:
                if ('\x00' <= c && c <= '\x1f') {
                    o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
                } else {
                    o << c;
                }
        }
    }
    return o.str();
}

static std::string schema_to_json(const MariaDBTableSchema& schema) {
    std::ostringstream json;
    json << "{";
    json << "\"database_name\":\"" << escape_json_string(schema.database_name) << "\",";
    json << "\"table_name\":\"" << escape_json_string(schema.table_name) << "\",";
    json << "\"engine\":\"" << escape_json_string(schema.engine) << "\",";
    
    json << "\"columns\":[";
    bool first = true;
    for (const auto& col : schema.columns) {
        if (!first) json << ",";
        json << "{";
        json << "\"name\":\"" << escape_json_string(col.name) << "\",";
        json << "\"data_type\":\"" << escape_json_string(col.data_type) << "\",";
        json << "\"max_length\":" << col.max_length << ",";
        json << "\"precision\":" << col.precision << ",";
        json << "\"scale\":" << col.scale << ",";
        json << "\"is_nullable\":" << (col.is_nullable ? "true" : "false") << ",";
        json << "\"is_primary_key\":" << (col.is_primary_key ? "true" : "false") << ",";
        json << "\"is_auto_increment\":" << (col.is_auto_increment ? "true" : "false") << ",";
        json << "\"ordinal_position\":" << col.ordinal_position;
        json << "}";
        first = false;
    }
    json << "],";
    
    json << "\"primary_key_columns\":[";
    first = true;
    for (const auto& pk : schema.primary_key_columns) {
        if (!first) json << ",";
        json << "\"" << escape_json_string(pk) << "\"";
        first = false;
    }
    json << "],";
    
    json << "\"create_table_stmt\":\"" << escape_json_string(schema.create_table_stmt) << "\"";
    json << "}";
    
    return json.str();
}

static std::string gtid_to_json(const GTIDPosition& gtid) {
    std::ostringstream json;
    json << "{";
    json << "\"gtid_binlog_pos\":\"" << escape_json_string(gtid.gtid_binlog_pos) << "\",";
    json << "\"binlog_file\":\"" << escape_json_string(gtid.binlog_file) << "\",";
    json << "\"binlog_position\":" << gtid.binlog_position;
    json << "}";
    return json.str();
}

static std::string ranges_to_json(const std::vector<PKRange>& ranges) {
    std::ostringstream json;
    json << "[";
    bool first = true;
    for (const auto& range : ranges) {
        if (!first) json << ",";
        json << "{";
        json << "\"min_value\":\"" << escape_json_string(range.min_value) << "\",";
        json << "\"max_value\":\"" << escape_json_string(range.max_value) << "\",";
        json << "\"is_first_range\":" << (range.is_first_range ? "true" : "false") << ",";
        json << "\"is_last_range\":" << (range.is_last_range ? "true" : "false") << ",";
        json << "\"range_id\":" << range.range_id;
        json << "}";
        first = false;
    }
    json << "]";
    return json.str();
}

static std::string row_to_json(const std::vector<std::pair<std::string, std::string>>& row) {
    std::ostringstream json;
    json << "{";
    bool first = true;
    for (const auto& col : row) {
        if (!first) json << ",";
        json << "\"" << escape_json_string(col.first) << "\":\"" 
             << escape_json_string(col.second) << "\"";
        first = false;
    }
    json << "}";
    return json.str();
}

// ============================================================================
// JNI Function Implementations
// ============================================================================

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_nativeInit
  (JNIEnv* env, jclass cls) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (!g_initialized) {
        // Initialize any global state
        g_initialized = true;
    }
}

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_nativeShutdown
  (JNIEnv* env, jclass cls) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_initialized) {
        // Cleanup global state
        g_initialized = false;
    }
}

JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createMariaDBConnection
  (JNIEnv* env, jclass cls, jstring host, jint port, jstring user, 
   jstring password, jstring database, jstring table) {
    
    MariaDBConfig config;
    config.host = jstring_to_string(env, host);
    config.port = static_cast<uint16_t>(port);
    config.user = jstring_to_string(env, user);
    config.password = jstring_to_string(env, password);
    config.database = jstring_to_string(env, database);
    config.table = jstring_to_string(env, table);
    
    auto* conn = new MariaDBConnection(config);
    return reinterpret_cast<jlong>(conn);
}

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyMariaDBConnection
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    delete conn;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_connectMariaDB
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    return conn->connect() ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getTableSchema
  (JNIEnv* env, jclass cls, jlong handle, jstring database, jstring table) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    std::string db = jstring_to_string(env, database);
    std::string tbl = jstring_to_string(env, table);
    
    auto schema = conn->get_table_schema(db, tbl);
    if (!schema) {
        return nullptr;
    }
    
    return string_to_jstring(env, schema_to_json(*schema));
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageStart
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    return conn->backup_stage_start() ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageBlockCommit
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    return conn->backup_stage_block_commit() ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_backupStageEnd
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    return conn->backup_stage_end() ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getGtidPosition
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    auto gtid = conn->get_gtid_position();
    if (!gtid) {
        return nullptr;
    }
    return string_to_jstring(env, gtid_to_json(*gtid));
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_calculatePKRanges
  (JNIEnv* env, jclass cls, jlong handle, jint numRanges) {
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    auto ranges = conn->calculate_pk_ranges(numRanges);
    return string_to_jstring(env, ranges_to_json(ranges));
}

JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_scanRange
  (JNIEnv* env, jclass cls, jlong handle, jstring minValue, jstring maxValue, 
   jint batchSize, jobject callback) {
    
    auto* conn = reinterpret_cast<MariaDBConnection*>(handle);
    
    PKRange range;
    range.min_value = jstring_to_string(env, minValue);
    range.max_value = jstring_to_string(env, maxValue);
    range.is_first_range = range.min_value.empty();
    range.is_last_range = range.max_value.empty();
    
    // Get callback class and method
    jclass callbackClass = env->GetObjectClass(callback);
    jmethodID onRowMethod = env->GetMethodID(callbackClass, "onRow", "(Ljava/lang/String;)V");
    
    if (!onRowMethod) {
        return 0;
    }
    
    // Global ref to callback for use in lambda
    jobject globalCallback = env->NewGlobalRef(callback);
    
    uint64_t count = conn->scan_range(
        range,
        batchSize,
        [env, globalCallback, onRowMethod](const std::vector<std::pair<std::string, std::string>>& row) {
            jstring rowJson = string_to_jstring(env, row_to_json(row));
            env->CallVoidMethod(globalCallback, onRowMethod, rowJson);
            env->DeleteLocalRef(rowJson);
        }
    );
    
    env->DeleteGlobalRef(globalCallback);
    return static_cast<jlong>(count);
}

JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createScyllaDBConnection
  (JNIEnv* env, jclass cls, jstring contactPoints, jint port, 
   jstring keyspace, jstring table, jstring user, jstring password) {
    
    ScyllaDBConfig config;
    
    // Parse contact points
    std::string cp = jstring_to_string(env, contactPoints);
    std::stringstream ss(cp);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            config.contact_points.push_back(token);
        }
    }
    
    config.port = static_cast<uint16_t>(port);
    config.keyspace = jstring_to_string(env, keyspace);
    config.table = jstring_to_string(env, table);
    
    if (user) config.user = jstring_to_string(env, user);
    if (password) config.password = jstring_to_string(env, password);
    
    auto* conn = new ScyllaDBConnection(config);
    return reinterpret_cast<jlong>(conn);
}

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyScyllaDBConnection
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(handle);
    delete conn;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_connectScyllaDB
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(handle);
    return conn->connect() ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createTable
  (JNIEnv* env, jclass cls, jlong handle, jstring createStmt) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(handle);
    std::string stmt = jstring_to_string(env, createStmt);
    return conn->create_table(stmt) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_tableExists
  (JNIEnv* env, jclass cls, jlong handle, jstring keyspace, jstring table) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(handle);
    std::string ks = jstring_to_string(env, keyspace);
    std::string tbl = jstring_to_string(env, table);
    return conn->table_exists(ks, tbl) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_writeBatch
  (JNIEnv* env, jclass cls, jlong handle, jstring rowsJson) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(handle);
    
    // Parse JSON array of rows
    // This is a simplified parser - production would use a proper JSON library
    std::string json = jstring_to_string(env, rowsJson);
    
    std::vector<std::vector<std::pair<std::string, std::string>>> rows;
    // ... JSON parsing would go here ...
    // For now, this is a placeholder
    
    return static_cast<jlong>(conn->write_batch(rows));
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_generateCqlCreateTable
  (JNIEnv* env, jclass cls, jstring schemaJson, jstring keyspace, jstring table) {
    
    // Parse schema JSON and generate CQL
    // This is simplified - production would properly parse the JSON
    std::string ks = jstring_to_string(env, keyspace);
    std::string tbl = jstring_to_string(env, table);
    
    // Create a dummy schema for now
    MariaDBTableSchema schema;
    schema.database_name = "";
    schema.table_name = tbl;
    
    std::string cql = SchemaConverter::generate_create_table(schema, ks, tbl);
    return string_to_jstring(env, cql);
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getLastError
  (JNIEnv* env, jclass cls, jlong handle) {
    // Try MariaDB connection first
    auto* mariaConn = reinterpret_cast<MariaDBConnection*>(handle);
    std::string error = mariaConn->get_last_error();
    
    if (error.empty()) {
        auto* scyllaConn = reinterpret_cast<ScyllaDBConnection*>(handle);
        error = scyllaConn->get_last_error();
    }
    
    return string_to_jstring(env, error);
}

JNIEXPORT jlong JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_createBinlogStreamer
  (JNIEnv* env, jclass cls, jstring host, jint port, jstring user, jstring password) {
    
    MariaDBConfig config;
    config.host = jstring_to_string(env, host);
    config.port = static_cast<uint16_t>(port);
    config.user = jstring_to_string(env, user);
    config.password = jstring_to_string(env, password);
    
    auto* streamer = new BinlogStreamer(config);
    return reinterpret_cast<jlong>(streamer);
}

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_destroyBinlogStreamer
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* streamer = reinterpret_cast<BinlogStreamer*>(handle);
    delete streamer;
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_startStreaming
  (JNIEnv* env, jclass cls, jlong handle, jstring gtidJson) {
    auto* streamer = reinterpret_cast<BinlogStreamer*>(handle);
    
    // Parse GTID from JSON
    GTIDPosition gtid;
    std::string json = jstring_to_string(env, gtidJson);
    // ... JSON parsing would go here ...
    
    return streamer->start_streaming(gtid) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_stopStreaming
  (JNIEnv* env, jclass cls, jlong handle) {
    auto* streamer = reinterpret_cast<BinlogStreamer*>(handle);
    streamer->stop_streaming();
}

JNIEXPORT jstring JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_getBinlogEvents
  (JNIEnv* env, jclass cls, jlong handle, jint batchSize, jint timeoutMs) {
    auto* streamer = reinterpret_cast<BinlogStreamer*>(handle);
    auto events = streamer->get_events(batchSize, timeoutMs);
    
    // Convert events to JSON array
    std::ostringstream json;
    json << "[";
    bool first = true;
    for (const auto& event : events) {
        if (!first) json << ",";
        json << "{";
        json << "\"type\":" << static_cast<int>(event.type) << ",";
        json << "\"table_name\":\"" << escape_json_string(event.table_name) << "\",";
        json << "\"timestamp_us\":" << event.timestamp_us << ",";
        json << "\"group_commit_id\":" << event.group_commit_id << ",";
        json << "\"is_last_in_group\":" << (event.is_last_in_group ? "true" : "false") << ",";
        
        json << "\"column_values\":{";
        bool first_col = true;
        for (const auto& col : event.column_values) {
            if (!first_col) json << ",";
            json << "\"" << escape_json_string(col.first) << "\":\"" 
                 << escape_json_string(col.second) << "\"";
            first_col = false;
        }
        json << "}";
        
        json << "}";
        first = false;
    }
    json << "]";
    
    return string_to_jstring(env, json.str());
}

JNIEXPORT jboolean JNICALL Java_com_scylladb_migrator_mariadb_NativeBridge_applyBinlogEvent
  (JNIEnv* env, jclass cls, jlong scyllaHandle, jstring eventJson) {
    auto* conn = reinterpret_cast<ScyllaDBConnection*>(scyllaHandle);
    
    // Parse event from JSON
    BinlogEvent event;
    std::string json = jstring_to_string(env, eventJson);
    // ... JSON parsing would go here ...
    
    return conn->apply_binlog_event(event) ? JNI_TRUE : JNI_FALSE;
}
