/**
 * ScyllaDB Migrator - MariaDB to ScyllaDB Native Bridge
 * 
 * This header defines the C++ interface for migrating data from MariaDB to ScyllaDB.
 * It uses MariaDB Connector/C (3.4.8) for the source and cpp-rs-driver (0.5.1) for the target.
 * 
 * Copyright (c) 2025 ScyllaDB
 * Licensed under Apache License 2.0
 */

#ifndef MARIADB_SCYLLA_MIGRATOR_H
#define MARIADB_SCYLLA_MIGRATOR_H

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <optional>
#include <mutex>
#include <atomic>
#include <condition_variable>

// Forward declarations for C libraries
#ifdef __cplusplus
extern "C" {
#endif

// MariaDB Connector/C headers
#include <mysql.h>

#ifdef __cplusplus
}
#endif

// ScyllaDB cpp-rs-driver headers
#include <cassandra.h>

namespace scylla_migrator {
namespace mariadb {

/**
 * Represents a column definition from MariaDB schema
 */
struct MariaDBColumn {
    std::string name;
    std::string data_type;      // MariaDB type name (e.g., "INT", "VARCHAR")
    int32_t max_length;
    int32_t precision;
    int32_t scale;
    bool is_nullable;
    bool is_primary_key;
    bool is_auto_increment;
    int32_t ordinal_position;
    std::string charset;
    std::string collation;
};

/**
 * Represents a table schema from MariaDB
 */
struct MariaDBTableSchema {
    std::string database_name;
    std::string table_name;
    std::string engine;         // Usually "InnoDB"
    std::vector<MariaDBColumn> columns;
    std::vector<std::string> primary_key_columns;
    std::string create_table_stmt;
};

/**
 * Represents a GTID position for binlog tracking
 */
struct GTIDPosition {
    std::string gtid_binlog_pos;
    std::string binlog_file;
    uint64_t binlog_position;
    
    bool operator==(const GTIDPosition& other) const {
        return gtid_binlog_pos == other.gtid_binlog_pos;
    }
    
    bool operator<(const GTIDPosition& other) const {
        return gtid_binlog_pos < other.gtid_binlog_pos;
    }
};

/**
 * Represents a primary key range for parallel scanning
 */
struct PKRange {
    std::string min_value;      // Inclusive
    std::string max_value;      // Exclusive (except for last range)
    bool is_first_range;
    bool is_last_range;
    int32_t range_id;
};

/**
 * Configuration for MariaDB connection
 */
struct MariaDBConfig {
    std::string host;
    uint16_t port = 3306;
    std::string user;
    std::string password;
    std::string database;
    std::string table;
    
    // SSL configuration
    bool use_ssl = false;
    std::string ssl_ca;
    std::string ssl_cert;
    std::string ssl_key;
    
    // Connection pool settings
    int32_t connection_pool_size = 4;
    int32_t connection_timeout_ms = 30000;
    int32_t read_timeout_ms = 300000;
    
    // Backup stage settings
    bool use_backup_stage = true;
    
    // Binlog settings
    bool stream_binlog = true;
    std::string server_id;      // For binlog streaming
};

/**
 * Configuration for ScyllaDB connection
 */
struct ScyllaDBConfig {
    std::vector<std::string> contact_points;
    uint16_t port = 9042;
    std::string local_dc;
    std::string keyspace;
    std::string table;
    
    // Authentication
    std::string user;
    std::string password;
    
    // SSL configuration
    bool use_ssl = false;
    std::string ssl_ca;
    std::string ssl_cert;
    std::string ssl_key;
    
    // Performance settings
    int32_t connections_per_host = 4;
    int32_t write_batch_size = 100;
    int32_t concurrent_writes = 16;
    std::string consistency_level = "LOCAL_QUORUM";
};

/**
 * Binlog event types
 */
enum class BinlogEventType {
    INSERT,
    UPDATE,
    DELETE,
    UNKNOWN
};

/**
 * Represents a binlog event (change data capture)
 */
struct BinlogEvent {
    BinlogEventType type;
    std::string table_name;
    GTIDPosition gtid;
    uint64_t timestamp_us;
    
    // For INSERT/UPDATE: new values; For DELETE: old values
    std::vector<std::pair<std::string, std::string>> column_values;
    
    // For UPDATE only: old values (for WHERE clause)
    std::vector<std::pair<std::string, std::string>> old_column_values;
    
    // Group commit information
    uint64_t group_commit_id;
    bool is_last_in_group;
};

/**
 * CQL type mapping result
 */
struct CQLColumnDef {
    std::string name;
    std::string cql_type;       // e.g., "text", "int", "bigint", "decimal"
    bool is_partition_key;
    bool is_clustering_key;
};

/**
 * Statistics for migration progress
 */
struct MigrationStats {
    std::atomic<uint64_t> rows_read{0};
    std::atomic<uint64_t> rows_written{0};
    std::atomic<uint64_t> binlog_events_processed{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> bytes_transferred{0};
    
    std::chrono::steady_clock::time_point start_time;
    std::optional<std::chrono::steady_clock::time_point> snapshot_complete_time;
};

/**
 * Main class for MariaDB connection and operations
 */
class MariaDBConnection {
public:
    explicit MariaDBConnection(const MariaDBConfig& config);
    ~MariaDBConnection();
    
    // Prevent copying
    MariaDBConnection(const MariaDBConnection&) = delete;
    MariaDBConnection& operator=(const MariaDBConnection&) = delete;
    
    // Allow moving
    MariaDBConnection(MariaDBConnection&& other) noexcept;
    MariaDBConnection& operator=(MariaDBConnection&& other) noexcept;
    
    /**
     * Connect to MariaDB server
     */
    bool connect();
    
    /**
     * Disconnect from MariaDB server
     */
    void disconnect();
    
    /**
     * Check if connected
     */
    bool is_connected() const;
    
    /**
     * Get table schema
     */
    std::optional<MariaDBTableSchema> get_table_schema(
        const std::string& database,
        const std::string& table
    );
    
    /**
     * Execute BACKUP STAGE START
     */
    bool backup_stage_start();
    
    /**
     * Execute BACKUP STAGE BLOCK_COMMIT
     */
    bool backup_stage_block_commit();
    
    /**
     * Execute BACKUP STAGE END
     */
    bool backup_stage_end();
    
    /**
     * Get current GTID position
     */
    std::optional<GTIDPosition> get_gtid_position();
    
    /**
     * Calculate primary key ranges for parallel scanning
     * @param num_ranges Number of ranges to split into
     */
    std::vector<PKRange> calculate_pk_ranges(int32_t num_ranges);
    
    /**
     * Execute a SELECT query and iterate over results
     * @param range The PK range to scan
     * @param batch_size Number of rows per batch
     * @param callback Called for each row with column name/value pairs
     * @return Number of rows processed
     */
    uint64_t scan_range(
        const PKRange& range,
        int32_t batch_size,
        std::function<void(const std::vector<std::pair<std::string, std::string>>&)> callback
    );
    
    /**
     * Get last error message
     */
    std::string get_last_error() const;

private:
    MariaDBConfig config_;
    MYSQL* conn_ = nullptr;
    std::string last_error_;
    std::optional<MariaDBTableSchema> cached_schema_;
    
    bool execute_query(const std::string& query);
    std::string build_range_query(const PKRange& range, int32_t batch_size);
};

/**
 * Binlog streamer for CDC
 */
class BinlogStreamer {
public:
    explicit BinlogStreamer(const MariaDBConfig& config);
    ~BinlogStreamer();
    
    /**
     * Start streaming from a specific GTID position
     */
    bool start_streaming(const GTIDPosition& start_position);
    
    /**
     * Stop streaming
     */
    void stop_streaming();
    
    /**
     * Get next batch of events
     * @param batch_size Maximum number of events to return
     * @param timeout_ms Timeout in milliseconds
     * @return Vector of binlog events
     */
    std::vector<BinlogEvent> get_events(int32_t batch_size, int32_t timeout_ms);
    
    /**
     * Get current GTID position
     */
    GTIDPosition get_current_position() const;
    
    /**
     * Check if streamer is running
     */
    bool is_running() const;
    
    /**
     * Get last error
     */
    std::string get_last_error() const;

private:
    MariaDBConfig config_;
    MYSQL* conn_ = nullptr;
    std::atomic<bool> running_{false};
    GTIDPosition current_position_;
    std::string last_error_;
    std::mutex mutex_;
    
    bool parse_binlog_event(const unsigned char* data, size_t len, BinlogEvent& event);
};

/**
 * Main class for ScyllaDB connection and operations
 */
class ScyllaDBConnection {
public:
    explicit ScyllaDBConnection(const ScyllaDBConfig& config);
    ~ScyllaDBConnection();
    
    // Prevent copying
    ScyllaDBConnection(const ScyllaDBConnection&) = delete;
    ScyllaDBConnection& operator=(const ScyllaDBConnection&) = delete;
    
    /**
     * Connect to ScyllaDB cluster
     */
    bool connect();
    
    /**
     * Disconnect from ScyllaDB cluster
     */
    void disconnect();
    
    /**
     * Check if connected
     */
    bool is_connected() const;
    
    /**
     * Create table with the specified schema
     */
    bool create_table(const std::string& create_statement);
    
    /**
     * Check if table exists
     */
    bool table_exists(const std::string& keyspace, const std::string& table);
    
    /**
     * Write a batch of rows
     * @param rows Vector of column name/value pairs
     * @return Number of rows successfully written
     */
    uint64_t write_batch(
        const std::vector<std::vector<std::pair<std::string, std::string>>>& rows
    );
    
    /**
     * Apply a single binlog event
     */
    bool apply_binlog_event(const BinlogEvent& event);
    
    /**
     * Get last error
     */
    std::string get_last_error() const;

private:
    ScyllaDBConfig config_;
    CassCluster* cluster_ = nullptr;
    CassSession* session_ = nullptr;
    std::string last_error_;
    std::string prepared_insert_stmt_;
    const CassPrepared* prepared_insert_ = nullptr;
    
    bool prepare_statements();
    CassConsistency parse_consistency(const std::string& level);
};

/**
 * Schema converter - converts MariaDB schema to ScyllaDB CQL
 */
class SchemaConverter {
public:
    /**
     * Convert MariaDB type to CQL type
     */
    static std::string mariadb_to_cql_type(const MariaDBColumn& column);
    
    /**
     * Generate CREATE TABLE statement for ScyllaDB
     * @param schema MariaDB source schema
     * @param keyspace Target ScyllaDB keyspace
     * @param table Target table name (can be different from source)
     * @return CQL CREATE TABLE statement
     */
    static std::string generate_create_table(
        const MariaDBTableSchema& schema,
        const std::string& keyspace,
        const std::string& table
    );
    
    /**
     * Convert a MariaDB value to CQL literal
     */
    static std::string value_to_cql_literal(
        const std::string& value,
        const std::string& mariadb_type,
        bool is_null
    );
    
    /**
     * Generate column definitions for CQL
     */
    static std::vector<CQLColumnDef> generate_column_defs(
        const MariaDBTableSchema& schema
    );

private:
    static std::string escape_cql_identifier(const std::string& name);
    static std::string escape_cql_string(const std::string& value);
};

/**
 * Coordination manager for multi-worker migration
 */
class MigrationCoordinator {
public:
    enum class State {
        INITIALIZING,
        ACQUIRING_LOCK,
        CREATING_TABLE,
        SNAPSHOT_IN_PROGRESS,
        SNAPSHOT_COMPLETE,
        STREAMING_BINLOG,
        COMPLETED,
        ERROR
    };
    
    MigrationCoordinator(int32_t num_workers, int32_t worker_id);
    ~MigrationCoordinator();
    
    /**
     * For worker 0: Initialize migration (create table, get GTID position)
     * For other workers: Wait for initialization to complete
     */
    bool initialize(
        MariaDBConnection& mariadb,
        ScyllaDBConnection& scylladb
    );
    
    /**
     * Get the PK range assigned to this worker
     */
    std::optional<PKRange> get_assigned_range() const;
    
    /**
     * Report that this worker has completed snapshot
     */
    void report_snapshot_complete();
    
    /**
     * Wait for all workers to complete snapshot
     * @return True if all workers completed, false on timeout
     */
    bool wait_for_all_snapshot_complete(int32_t timeout_ms);
    
    /**
     * Get current state
     */
    State get_state() const;
    
    /**
     * Get the GTID position captured at start
     */
    GTIDPosition get_start_gtid() const;
    
    /**
     * Get migration statistics
     */
    MigrationStats& get_stats();
    
    /**
     * Check if this worker should handle binlog streaming
     * (Only worker 0 does this)
     */
    bool should_stream_binlog() const;

private:
    int32_t num_workers_;
    int32_t worker_id_;
    State state_ = State::INITIALIZING;
    GTIDPosition start_gtid_;
    std::vector<PKRange> pk_ranges_;
    MigrationStats stats_;
    
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<int32_t> workers_completed_snapshot_{0};
    
    // For distributed coordination (placeholder - would use ZK/etcd in production)
    bool acquire_initialization_lock();
    void release_initialization_lock();
};

/**
 * Main migrator class that orchestrates the entire migration
 */
class MariaDBToScyllaDBMigrator {
public:
    MariaDBToScyllaDBMigrator(
        const MariaDBConfig& source_config,
        const ScyllaDBConfig& target_config,
        int32_t num_workers,
        int32_t worker_id
    );
    ~MariaDBToScyllaDBMigrator();
    
    /**
     * Execute the migration
     * @return True if migration completed successfully
     */
    bool migrate();
    
    /**
     * Stop the migration
     */
    void stop();
    
    /**
     * Get current statistics
     */
    MigrationStats get_stats() const;
    
    /**
     * Get last error
     */
    std::string get_last_error() const;

private:
    MariaDBConfig source_config_;
    ScyllaDBConfig target_config_;
    int32_t num_workers_;
    int32_t worker_id_;
    
    std::unique_ptr<MariaDBConnection> mariadb_;
    std::unique_ptr<ScyllaDBConnection> scylladb_;
    std::unique_ptr<MigrationCoordinator> coordinator_;
    std::unique_ptr<BinlogStreamer> binlog_streamer_;
    
    std::atomic<bool> running_{false};
    std::string last_error_;
    
    bool run_snapshot_phase();
    bool run_binlog_streaming_phase();
};

} // namespace mariadb
} // namespace scylla_migrator

#endif // MARIADB_SCYLLA_MIGRATOR_H
