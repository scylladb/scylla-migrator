/**
 * ScyllaDB Migrator - MariaDB Connection Implementation
 * 
 * Uses MariaDB Connector/C (3.4.8) for database operations
 * 
 * Copyright (c) 2025 ScyllaDB
 * Licensed under Apache License 2.0
 */

#include "mariadb_scylla_migrator.h"

#include <sstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <cstring>
#include <iomanip>

namespace scylla_migrator {
namespace mariadb {

// ============================================================================
// MariaDBConnection Implementation
// ============================================================================

MariaDBConnection::MariaDBConnection(const MariaDBConfig& config)
    : config_(config) {
}

MariaDBConnection::~MariaDBConnection() {
    disconnect();
}

MariaDBConnection::MariaDBConnection(MariaDBConnection&& other) noexcept
    : config_(std::move(other.config_)),
      conn_(other.conn_),
      last_error_(std::move(other.last_error_)),
      cached_schema_(std::move(other.cached_schema_)) {
    other.conn_ = nullptr;
}

MariaDBConnection& MariaDBConnection::operator=(MariaDBConnection&& other) noexcept {
    if (this != &other) {
        disconnect();
        config_ = std::move(other.config_);
        conn_ = other.conn_;
        last_error_ = std::move(other.last_error_);
        cached_schema_ = std::move(other.cached_schema_);
        other.conn_ = nullptr;
    }
    return *this;
}

bool MariaDBConnection::connect() {
    if (conn_) {
        disconnect();
    }
    
    conn_ = mysql_init(nullptr);
    if (!conn_) {
        last_error_ = "Failed to initialize MySQL connection";
        return false;
    }
    
    // Set connection options
    unsigned int timeout = config_.connection_timeout_ms / 1000;
    mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    
    unsigned int read_timeout = config_.read_timeout_ms / 1000;
    mysql_options(conn_, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
    
    // SSL configuration
    if (config_.use_ssl) {
        mysql_ssl_set(conn_,
                      config_.ssl_key.empty() ? nullptr : config_.ssl_key.c_str(),
                      config_.ssl_cert.empty() ? nullptr : config_.ssl_cert.c_str(),
                      config_.ssl_ca.empty() ? nullptr : config_.ssl_ca.c_str(),
                      nullptr,  // CApath
                      nullptr); // cipher
    }
    
    // Enable multi-statements for BACKUP STAGE commands
    unsigned long client_flag = CLIENT_MULTI_STATEMENTS;
    
    if (!mysql_real_connect(conn_,
                            config_.host.c_str(),
                            config_.user.c_str(),
                            config_.password.c_str(),
                            config_.database.c_str(),
                            config_.port,
                            nullptr,  // unix_socket
                            client_flag)) {
        last_error_ = std::string("Connection failed: ") + mysql_error(conn_);
        mysql_close(conn_);
        conn_ = nullptr;
        return false;
    }
    
    // Set character set to UTF-8
    if (mysql_set_character_set(conn_, "utf8mb4") != 0) {
        last_error_ = std::string("Failed to set charset: ") + mysql_error(conn_);
        mysql_close(conn_);
        conn_ = nullptr;
        return false;
    }
    
    return true;
}

void MariaDBConnection::disconnect() {
    if (conn_) {
        mysql_close(conn_);
        conn_ = nullptr;
    }
}

bool MariaDBConnection::is_connected() const {
    return conn_ != nullptr && mysql_ping(const_cast<MYSQL*>(conn_)) == 0;
}

bool MariaDBConnection::execute_query(const std::string& query) {
    if (!is_connected()) {
        last_error_ = "Not connected to database";
        return false;
    }
    
    if (mysql_real_query(conn_, query.c_str(), query.length()) != 0) {
        last_error_ = std::string("Query failed: ") + mysql_error(conn_);
        return false;
    }
    
    // Consume any result sets
    do {
        MYSQL_RES* result = mysql_store_result(conn_);
        if (result) {
            mysql_free_result(result);
        }
    } while (mysql_next_result(conn_) == 0);
    
    return true;
}

std::optional<MariaDBTableSchema> MariaDBConnection::get_table_schema(
    const std::string& database,
    const std::string& table
) {
    if (!is_connected()) {
        last_error_ = "Not connected to database";
        return std::nullopt;
    }
    
    MariaDBTableSchema schema;
    schema.database_name = database;
    schema.table_name = table;
    
    // Get table engine
    std::string table_info_query = 
        "SELECT ENGINE FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "'";
    
    if (mysql_real_query(conn_, table_info_query.c_str(), table_info_query.length()) != 0) {
        last_error_ = std::string("Failed to get table info: ") + mysql_error(conn_);
        return std::nullopt;
    }
    
    MYSQL_RES* result = mysql_store_result(conn_);
    if (result) {
        MYSQL_ROW row = mysql_fetch_row(result);
        if (row && row[0]) {
            schema.engine = row[0];
        }
        mysql_free_result(result);
    }
    
    // Get column information
    std::string column_query = 
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_KEY, "
        "EXTRA, ORDINAL_POSITION, CHARACTER_SET_NAME, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "' "
        "ORDER BY ORDINAL_POSITION";
    
    if (mysql_real_query(conn_, column_query.c_str(), column_query.length()) != 0) {
        last_error_ = std::string("Failed to get columns: ") + mysql_error(conn_);
        return std::nullopt;
    }
    
    result = mysql_store_result(conn_);
    if (!result) {
        last_error_ = "No column information returned";
        return std::nullopt;
    }
    
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        unsigned long* lengths = mysql_fetch_lengths(result);
        
        MariaDBColumn column;
        column.name = row[0] ? row[0] : "";
        column.data_type = row[1] ? row[1] : "";
        column.max_length = row[2] ? std::stoi(row[2]) : 0;
        column.precision = row[3] ? std::stoi(row[3]) : 0;
        column.scale = row[4] ? std::stoi(row[4]) : 0;
        column.is_nullable = row[5] && std::string(row[5]) == "YES";
        column.is_primary_key = row[6] && std::string(row[6]) == "PRI";
        column.is_auto_increment = row[7] && std::string(row[7]).find("auto_increment") != std::string::npos;
        column.ordinal_position = row[8] ? std::stoi(row[8]) : 0;
        column.charset = row[9] ? row[9] : "";
        column.collation = row[10] ? row[10] : "";
        
        if (column.is_primary_key) {
            schema.primary_key_columns.push_back(column.name);
        }
        
        schema.columns.push_back(column);
    }
    mysql_free_result(result);
    
    // Get CREATE TABLE statement
    std::string show_create = "SHOW CREATE TABLE `" + database + "`.`" + table + "`";
    if (mysql_real_query(conn_, show_create.c_str(), show_create.length()) == 0) {
        result = mysql_store_result(conn_);
        if (result) {
            row = mysql_fetch_row(result);
            if (row && row[1]) {
                schema.create_table_stmt = row[1];
            }
            mysql_free_result(result);
        }
    }
    
    cached_schema_ = schema;
    return schema;
}

bool MariaDBConnection::backup_stage_start() {
    return execute_query("BACKUP STAGE START");
}

bool MariaDBConnection::backup_stage_block_commit() {
    return execute_query("BACKUP STAGE BLOCK_COMMIT");
}

bool MariaDBConnection::backup_stage_end() {
    return execute_query("BACKUP STAGE END");
}

std::optional<GTIDPosition> MariaDBConnection::get_gtid_position() {
    if (!is_connected()) {
        last_error_ = "Not connected to database";
        return std::nullopt;
    }
    
    const char* query = "SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'";
    
    if (mysql_real_query(conn_, query, strlen(query)) != 0) {
        last_error_ = std::string("Failed to get GTID: ") + mysql_error(conn_);
        return std::nullopt;
    }
    
    GTIDPosition position;
    
    MYSQL_RES* result = mysql_store_result(conn_);
    if (result) {
        MYSQL_ROW row = mysql_fetch_row(result);
        if (row && row[1]) {
            position.gtid_binlog_pos = row[1];
        }
        mysql_free_result(result);
    }
    
    // Also get binlog file and position for compatibility
    query = "SHOW MASTER STATUS";
    if (mysql_real_query(conn_, query, strlen(query)) == 0) {
        result = mysql_store_result(conn_);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (row) {
                if (row[0]) position.binlog_file = row[0];
                if (row[1]) position.binlog_position = std::stoull(row[1]);
            }
            mysql_free_result(result);
        }
    }
    
    return position;
}

std::vector<PKRange> MariaDBConnection::calculate_pk_ranges(int32_t num_ranges) {
    std::vector<PKRange> ranges;
    
    if (!cached_schema_ || cached_schema_->primary_key_columns.empty()) {
        last_error_ = "No schema or primary key information available";
        return ranges;
    }
    
    // For simplicity, we'll use the first PK column
    // A production implementation should handle composite keys
    const std::string& pk_col = cached_schema_->primary_key_columns[0];
    
    // Get min and max values
    std::string minmax_query = "SELECT MIN(`" + pk_col + "`), MAX(`" + pk_col + "`) FROM `" +
                               config_.database + "`.`" + config_.table + "`";
    
    if (mysql_real_query(conn_, minmax_query.c_str(), minmax_query.length()) != 0) {
        last_error_ = std::string("Failed to get PK range: ") + mysql_error(conn_);
        return ranges;
    }
    
    std::string min_val, max_val;
    MYSQL_RES* result = mysql_store_result(conn_);
    if (result) {
        MYSQL_ROW row = mysql_fetch_row(result);
        if (row) {
            if (row[0]) min_val = row[0];
            if (row[1]) max_val = row[1];
        }
        mysql_free_result(result);
    }
    
    if (min_val.empty() || max_val.empty()) {
        // Empty table, return single range
        PKRange range;
        range.min_value = "";
        range.max_value = "";
        range.is_first_range = true;
        range.is_last_range = true;
        range.range_id = 0;
        ranges.push_back(range);
        return ranges;
    }
    
    // For numeric PKs, calculate evenly distributed ranges
    // Find the PK column type
    std::string pk_type;
    for (const auto& col : cached_schema_->columns) {
        if (col.name == pk_col) {
            pk_type = col.data_type;
            break;
        }
    }
    
    // Convert to lowercase for comparison
    std::transform(pk_type.begin(), pk_type.end(), pk_type.begin(), ::tolower);
    
    bool is_numeric = (pk_type.find("int") != std::string::npos ||
                       pk_type.find("decimal") != std::string::npos ||
                       pk_type.find("numeric") != std::string::npos ||
                       pk_type.find("float") != std::string::npos ||
                       pk_type.find("double") != std::string::npos);
    
    if (is_numeric) {
        // Numeric range splitting
        int64_t min_i = std::stoll(min_val);
        int64_t max_i = std::stoll(max_val);
        int64_t range_size = (max_i - min_i + num_ranges) / num_ranges;
        
        for (int32_t i = 0; i < num_ranges; i++) {
            PKRange range;
            range.min_value = std::to_string(min_i + (i * range_size));
            range.max_value = (i == num_ranges - 1) ? 
                              std::to_string(max_i + 1) :  // Include last value
                              std::to_string(min_i + ((i + 1) * range_size));
            range.is_first_range = (i == 0);
            range.is_last_range = (i == num_ranges - 1);
            range.range_id = i;
            ranges.push_back(range);
        }
    } else {
        // For string PKs, use LIMIT/OFFSET or sample-based splitting
        // This is a simplified version - production would use more sophisticated methods
        
        // Get total row count
        std::string count_query = "SELECT COUNT(*) FROM `" + config_.database + "`.`" + config_.table + "`";
        uint64_t total_rows = 0;
        
        if (mysql_real_query(conn_, count_query.c_str(), count_query.length()) == 0) {
            result = mysql_store_result(conn_);
            if (result) {
                MYSQL_ROW row = mysql_fetch_row(result);
                if (row && row[0]) {
                    total_rows = std::stoull(row[0]);
                }
                mysql_free_result(result);
            }
        }
        
        // Sample boundary values
        uint64_t rows_per_range = (total_rows + num_ranges - 1) / num_ranges;
        
        std::vector<std::string> boundaries;
        boundaries.push_back(min_val);
        
        for (int32_t i = 1; i < num_ranges; i++) {
            uint64_t offset = i * rows_per_range;
            std::string sample_query = "SELECT `" + pk_col + "` FROM `" + 
                                       config_.database + "`.`" + config_.table + 
                                       "` ORDER BY `" + pk_col + "` LIMIT 1 OFFSET " + 
                                       std::to_string(offset);
            
            if (mysql_real_query(conn_, sample_query.c_str(), sample_query.length()) == 0) {
                result = mysql_store_result(conn_);
                if (result) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (row && row[0]) {
                        boundaries.push_back(row[0]);
                    }
                    mysql_free_result(result);
                }
            }
        }
        
        // Create ranges from boundaries
        for (size_t i = 0; i < boundaries.size(); i++) {
            PKRange range;
            range.min_value = boundaries[i];
            range.max_value = (i == boundaries.size() - 1) ? "" : boundaries[i + 1];
            range.is_first_range = (i == 0);
            range.is_last_range = (i == boundaries.size() - 1);
            range.range_id = static_cast<int32_t>(i);
            ranges.push_back(range);
        }
    }
    
    return ranges;
}

std::string MariaDBConnection::build_range_query(const PKRange& range, int32_t batch_size) {
    if (!cached_schema_ || cached_schema_->primary_key_columns.empty()) {
        return "";
    }
    
    const std::string& pk_col = cached_schema_->primary_key_columns[0];
    
    std::ostringstream query;
    query << "SELECT * FROM `" << config_.database << "`.`" << config_.table << "` WHERE ";
    
    if (!range.min_value.empty()) {
        query << "`" << pk_col << "` >= " << range.min_value;
    }
    
    if (!range.max_value.empty()) {
        if (!range.min_value.empty()) {
            query << " AND ";
        }
        if (range.is_last_range) {
            query << "`" << pk_col << "` <= " << range.max_value;
        } else {
            query << "`" << pk_col << "` < " << range.max_value;
        }
    }
    
    query << " ORDER BY `" << pk_col << "`";
    
    return query.str();
}

uint64_t MariaDBConnection::scan_range(
    const PKRange& range,
    int32_t batch_size,
    std::function<void(const std::vector<std::pair<std::string, std::string>>&)> callback
) {
    if (!is_connected() || !cached_schema_) {
        last_error_ = "Not connected or no schema available";
        return 0;
    }
    
    std::string query = build_range_query(range, batch_size);
    if (query.empty()) {
        return 0;
    }
    
    // Use streaming result (mysql_use_result) for large tables
    if (mysql_real_query(conn_, query.c_str(), query.length()) != 0) {
        last_error_ = std::string("Range query failed: ") + mysql_error(conn_);
        return 0;
    }
    
    MYSQL_RES* result = mysql_use_result(conn_);
    if (!result) {
        last_error_ = std::string("Failed to get result: ") + mysql_error(conn_);
        return 0;
    }
    
    uint64_t rows_processed = 0;
    unsigned int num_fields = mysql_num_fields(result);
    MYSQL_FIELD* fields = mysql_fetch_fields(result);
    
    // Get field names
    std::vector<std::string> field_names;
    for (unsigned int i = 0; i < num_fields; i++) {
        field_names.push_back(fields[i].name);
    }
    
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        unsigned long* lengths = mysql_fetch_lengths(result);
        
        std::vector<std::pair<std::string, std::string>> row_data;
        for (unsigned int i = 0; i < num_fields; i++) {
            std::string value;
            if (row[i]) {
                value = std::string(row[i], lengths[i]);
            }
            row_data.emplace_back(field_names[i], value);
        }
        
        callback(row_data);
        rows_processed++;
    }
    
    mysql_free_result(result);
    return rows_processed;
}

std::string MariaDBConnection::get_last_error() const {
    return last_error_;
}

// ============================================================================
// BinlogStreamer Implementation
// ============================================================================

BinlogStreamer::BinlogStreamer(const MariaDBConfig& config)
    : config_(config) {
}

BinlogStreamer::~BinlogStreamer() {
    stop_streaming();
    if (conn_) {
        mysql_close(conn_);
        conn_ = nullptr;
    }
}

bool BinlogStreamer::start_streaming(const GTIDPosition& start_position) {
    if (running_) {
        return true;
    }
    
    conn_ = mysql_init(nullptr);
    if (!conn_) {
        last_error_ = "Failed to initialize MySQL connection for binlog streaming";
        return false;
    }
    
    // Set server ID for replication
    unsigned int server_id = config_.server_id.empty() ? 
                             (std::hash<std::string>{}(config_.host) % 1000000) :
                             std::stoul(config_.server_id);
    
    // Connect with replication flags
    if (!mysql_real_connect(conn_,
                            config_.host.c_str(),
                            config_.user.c_str(),
                            config_.password.c_str(),
                            nullptr,  // no database needed
                            config_.port,
                            nullptr,
                            0)) {
        last_error_ = std::string("Binlog connection failed: ") + mysql_error(conn_);
        mysql_close(conn_);
        conn_ = nullptr;
        return false;
    }
    
    // Set GTID slave position
    std::string set_gtid = "SET @slave_connect_state='" + start_position.gtid_binlog_pos + "'";
    if (mysql_real_query(conn_, set_gtid.c_str(), set_gtid.length()) != 0) {
        last_error_ = std::string("Failed to set GTID position: ") + mysql_error(conn_);
        return false;
    }
    
    // Register as a replica
    std::string register_query = "SET @master_binlog_checksum='NONE'";
    mysql_real_query(conn_, register_query.c_str(), register_query.length());
    
    // Request binlog dump with GTID
    // Note: This is simplified - production would use proper BINLOG_DUMP_GTID command
    std::string dump_cmd = "BINLOG_DUMP_GTID";
    // In production, you would use mysql_binlog_open() and mysql_binlog_fetch()
    
    current_position_ = start_position;
    running_ = true;
    
    return true;
}

void BinlogStreamer::stop_streaming() {
    running_ = false;
}

std::vector<BinlogEvent> BinlogStreamer::get_events(int32_t batch_size, int32_t timeout_ms) {
    std::vector<BinlogEvent> events;
    
    if (!running_ || !conn_) {
        return events;
    }
    
    // This is a simplified implementation
    // In production, you would use:
    // 1. mysql_binlog_open() to set up the binlog stream
    // 2. mysql_binlog_fetch() to get events
    // 3. Parse ROW events to get actual data changes
    
    // For now, we'll simulate reading from binlog
    // Real implementation would parse binary log events properly
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Placeholder: In production, read actual binlog events here
    // The MariaDB Connector/C provides mysql_binlog_* functions
    
    return events;
}

GTIDPosition BinlogStreamer::get_current_position() const {
    return current_position_;
}

bool BinlogStreamer::is_running() const {
    return running_;
}

std::string BinlogStreamer::get_last_error() const {
    return last_error_;
}

bool BinlogStreamer::parse_binlog_event(const unsigned char* data, size_t len, BinlogEvent& event) {
    // Placeholder for binlog event parsing
    // Real implementation would parse the binary format of MariaDB binlog events
    return false;
}

} // namespace mariadb
} // namespace scylla_migrator
