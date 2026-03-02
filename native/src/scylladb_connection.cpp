/**
 * ScyllaDB Migrator - ScyllaDB Connection Implementation
 * 
 * Uses cpp-rs-driver (0.5.1) for ScyllaDB operations
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

namespace scylla_migrator {
namespace mariadb {

// ============================================================================
// ScyllaDBConnection Implementation
// ============================================================================

ScyllaDBConnection::ScyllaDBConnection(const ScyllaDBConfig& config)
    : config_(config) {
}

ScyllaDBConnection::~ScyllaDBConnection() {
    disconnect();
}

bool ScyllaDBConnection::connect() {
    if (session_) {
        disconnect();
    }
    
    // Create cluster configuration
    cluster_ = cass_cluster_new();
    if (!cluster_) {
        last_error_ = "Failed to create cluster object";
        return false;
    }
    
    // Set contact points
    std::string contact_points;
    for (size_t i = 0; i < config_.contact_points.size(); i++) {
        if (i > 0) contact_points += ",";
        contact_points += config_.contact_points[i];
    }
    
    CassError rc = cass_cluster_set_contact_points(cluster_, contact_points.c_str());
    if (rc != CASS_OK) {
        last_error_ = std::string("Failed to set contact points: ") + cass_error_desc(rc);
        cass_cluster_free(cluster_);
        cluster_ = nullptr;
        return false;
    }
    
    // Set port
    rc = cass_cluster_set_port(cluster_, config_.port);
    if (rc != CASS_OK) {
        last_error_ = std::string("Failed to set port: ") + cass_error_desc(rc);
        cass_cluster_free(cluster_);
        cluster_ = nullptr;
        return false;
    }
    
    // Set local DC if specified
    if (!config_.local_dc.empty()) {
        rc = cass_cluster_set_load_balance_dc_aware(cluster_, config_.local_dc.c_str(), 0, cass_false);
        if (rc != CASS_OK) {
            last_error_ = std::string("Failed to set local DC: ") + cass_error_desc(rc);
            cass_cluster_free(cluster_);
            cluster_ = nullptr;
            return false;
        }
    }
    
    // Set authentication if provided
    if (!config_.user.empty()) {
        cass_cluster_set_credentials(cluster_, config_.user.c_str(), config_.password.c_str());
    }
    
    // Set connections per host
    cass_cluster_set_core_connections_per_host(cluster_, config_.connections_per_host);
    
    // SSL configuration
    if (config_.use_ssl) {
        CassSsl* ssl = cass_ssl_new();
        
        if (!config_.ssl_ca.empty()) {
            cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);
            cass_ssl_add_trusted_cert(ssl, config_.ssl_ca.c_str());
        }
        
        if (!config_.ssl_cert.empty() && !config_.ssl_key.empty()) {
            cass_ssl_set_cert(ssl, config_.ssl_cert.c_str());
            cass_ssl_set_private_key(ssl, config_.ssl_key.c_str(), nullptr);
        }
        
        cass_cluster_set_ssl(cluster_, ssl);
        cass_ssl_free(ssl);
    }
    
    // Create session and connect
    session_ = cass_session_new();
    
    CassFuture* connect_future = cass_session_connect(session_, cluster_);
    cass_future_wait(connect_future);
    
    rc = cass_future_error_code(connect_future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        last_error_ = std::string("Connection failed: ") + std::string(message, message_length);
        
        cass_future_free(connect_future);
        cass_session_free(session_);
        cass_cluster_free(cluster_);
        session_ = nullptr;
        cluster_ = nullptr;
        return false;
    }
    
    cass_future_free(connect_future);
    return true;
}

void ScyllaDBConnection::disconnect() {
    if (prepared_insert_) {
        cass_prepared_free(prepared_insert_);
        prepared_insert_ = nullptr;
    }
    
    if (session_) {
        CassFuture* close_future = cass_session_close(session_);
        cass_future_wait(close_future);
        cass_future_free(close_future);
        cass_session_free(session_);
        session_ = nullptr;
    }
    
    if (cluster_) {
        cass_cluster_free(cluster_);
        cluster_ = nullptr;
    }
}

bool ScyllaDBConnection::is_connected() const {
    return session_ != nullptr;
}

CassConsistency ScyllaDBConnection::parse_consistency(const std::string& level) {
    std::string upper_level = level;
    std::transform(upper_level.begin(), upper_level.end(), upper_level.begin(), ::toupper);
    
    if (upper_level == "ANY") return CASS_CONSISTENCY_ANY;
    if (upper_level == "ONE") return CASS_CONSISTENCY_ONE;
    if (upper_level == "TWO") return CASS_CONSISTENCY_TWO;
    if (upper_level == "THREE") return CASS_CONSISTENCY_THREE;
    if (upper_level == "QUORUM") return CASS_CONSISTENCY_QUORUM;
    if (upper_level == "ALL") return CASS_CONSISTENCY_ALL;
    if (upper_level == "LOCAL_QUORUM") return CASS_CONSISTENCY_LOCAL_QUORUM;
    if (upper_level == "EACH_QUORUM") return CASS_CONSISTENCY_EACH_QUORUM;
    if (upper_level == "LOCAL_ONE") return CASS_CONSISTENCY_LOCAL_ONE;
    
    return CASS_CONSISTENCY_LOCAL_QUORUM;  // Default
}

bool ScyllaDBConnection::create_table(const std::string& create_statement) {
    if (!is_connected()) {
        last_error_ = "Not connected to ScyllaDB";
        return false;
    }
    
    CassStatement* statement = cass_statement_new(create_statement.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, statement);
    
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(future, &message, &message_length);
        last_error_ = std::string("Create table failed: ") + std::string(message, message_length);
        
        cass_future_free(future);
        cass_statement_free(statement);
        return false;
    }
    
    cass_future_free(future);
    cass_statement_free(statement);
    return true;
}

bool ScyllaDBConnection::table_exists(const std::string& keyspace, const std::string& table) {
    if (!is_connected()) {
        return false;
    }
    
    std::string query = "SELECT table_name FROM system_schema.tables "
                        "WHERE keyspace_name = ? AND table_name = ?";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 2);
    cass_statement_bind_string(statement, 0, keyspace.c_str());
    cass_statement_bind_string(statement, 1, table.c_str());
    
    CassFuture* future = cass_session_execute(session_, statement);
    cass_future_wait(future);
    
    bool exists = false;
    CassError rc = cass_future_error_code(future);
    
    if (rc == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        if (result && cass_result_row_count(result) > 0) {
            exists = true;
        }
        if (result) {
            cass_result_free(result);
        }
    }
    
    cass_future_free(future);
    cass_statement_free(statement);
    return exists;
}

bool ScyllaDBConnection::prepare_statements() {
    // This will be called after knowing the schema
    // Build the prepared INSERT statement dynamically
    return true;
}

uint64_t ScyllaDBConnection::write_batch(
    const std::vector<std::vector<std::pair<std::string, std::string>>>& rows
) {
    if (!is_connected() || rows.empty()) {
        return 0;
    }
    
    uint64_t rows_written = 0;
    
    // Create a batch
    CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    cass_batch_set_consistency(batch, parse_consistency(config_.consistency_level));
    
    for (const auto& row : rows) {
        if (row.empty()) continue;
        
        // Build INSERT statement dynamically
        std::ostringstream insert_query;
        std::ostringstream values_part;
        
        insert_query << "INSERT INTO " << config_.keyspace << "." << config_.table << " (";
        values_part << ") VALUES (";
        
        bool first = true;
        for (const auto& col : row) {
            if (!first) {
                insert_query << ", ";
                values_part << ", ";
            }
            insert_query << "\"" << col.first << "\"";
            values_part << "?";
            first = false;
        }
        
        std::string full_query = insert_query.str() + values_part.str() + ")";
        
        CassStatement* statement = cass_statement_new(full_query.c_str(), row.size());
        
        // Bind values
        size_t idx = 0;
        for (const auto& col : row) {
            if (col.second.empty()) {
                cass_statement_bind_null(statement, idx);
            } else {
                cass_statement_bind_string(statement, idx, col.second.c_str());
            }
            idx++;
        }
        
        cass_batch_add_statement(batch, statement);
        cass_statement_free(statement);
    }
    
    // Execute batch
    CassFuture* future = cass_session_execute_batch(session_, batch);
    cass_future_wait(future);
    
    CassError rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
        rows_written = rows.size();
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(future, &message, &message_length);
        last_error_ = std::string("Batch write failed: ") + std::string(message, message_length);
    }
    
    cass_future_free(future);
    cass_batch_free(batch);
    
    return rows_written;
}

bool ScyllaDBConnection::apply_binlog_event(const BinlogEvent& event) {
    if (!is_connected()) {
        last_error_ = "Not connected to ScyllaDB";
        return false;
    }
    
    std::string query;
    
    switch (event.type) {
        case BinlogEventType::INSERT: {
            std::ostringstream insert_query;
            std::ostringstream values_part;
            
            insert_query << "INSERT INTO " << config_.keyspace << "." << config_.table << " (";
            values_part << ") VALUES (";
            
            bool first = true;
            for (const auto& col : event.column_values) {
                if (!first) {
                    insert_query << ", ";
                    values_part << ", ";
                }
                insert_query << "\"" << col.first << "\"";
                values_part << "?";
                first = false;
            }
            
            query = insert_query.str() + values_part.str() + ")";
            break;
        }
        
        case BinlogEventType::UPDATE: {
            std::ostringstream update_query;
            update_query << "UPDATE " << config_.keyspace << "." << config_.table << " SET ";
            
            // For UPDATE, we'd need to know which columns are PKs
            // This is simplified - production would handle this properly
            bool first = true;
            for (const auto& col : event.column_values) {
                if (!first) {
                    update_query << ", ";
                }
                update_query << "\"" << col.first << "\" = ?";
                first = false;
            }
            
            // WHERE clause would use old_column_values for PK columns
            // Simplified for now
            query = update_query.str();
            break;
        }
        
        case BinlogEventType::DELETE: {
            std::ostringstream delete_query;
            delete_query << "DELETE FROM " << config_.keyspace << "." << config_.table << " WHERE ";
            
            // Use column_values (which contain old values for DELETE)
            bool first = true;
            for (const auto& col : event.column_values) {
                if (!first) {
                    delete_query << " AND ";
                }
                delete_query << "\"" << col.first << "\" = ?";
                first = false;
            }
            
            query = delete_query.str();
            break;
        }
        
        default:
            return true;  // Ignore unknown events
    }
    
    CassStatement* statement = cass_statement_new(query.c_str(), event.column_values.size());
    cass_statement_set_consistency(statement, parse_consistency(config_.consistency_level));
    
    // Bind values
    size_t idx = 0;
    for (const auto& col : event.column_values) {
        if (col.second.empty()) {
            cass_statement_bind_null(statement, idx);
        } else {
            cass_statement_bind_string(statement, idx, col.second.c_str());
        }
        idx++;
    }
    
    CassFuture* future = cass_session_execute(session_, statement);
    cass_future_wait(future);
    
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(future, &message, &message_length);
        last_error_ = std::string("Apply event failed: ") + std::string(message, message_length);
        
        cass_future_free(future);
        cass_statement_free(statement);
        return false;
    }
    
    cass_future_free(future);
    cass_statement_free(statement);
    return true;
}

std::string ScyllaDBConnection::get_last_error() const {
    return last_error_;
}

// ============================================================================
// SchemaConverter Implementation
// ============================================================================

std::string SchemaConverter::mariadb_to_cql_type(const MariaDBColumn& column) {
    std::string type = column.data_type;
    std::transform(type.begin(), type.end(), type.begin(), ::tolower);
    
    // Integer types
    if (type == "tinyint") return "tinyint";
    if (type == "smallint") return "smallint";
    if (type == "mediumint") return "int";
    if (type == "int" || type == "integer") return "int";
    if (type == "bigint") return "bigint";
    
    // Floating point
    if (type == "float") return "float";
    if (type == "double" || type == "real") return "double";
    
    // Decimal/Numeric
    if (type == "decimal" || type == "numeric") return "decimal";
    
    // Boolean
    if (type == "boolean" || type == "bool") return "boolean";
    
    // Date/Time types
    if (type == "date") return "date";
    if (type == "time") return "time";
    if (type == "datetime" || type == "timestamp") return "timestamp";
    if (type == "year") return "int";  // CQL doesn't have YEAR type
    
    // String types
    if (type == "char" || type == "varchar" || type == "text" ||
        type == "tinytext" || type == "mediumtext" || type == "longtext") {
        return "text";
    }
    
    // Binary types
    if (type == "binary" || type == "varbinary" || type == "blob" ||
        type == "tinyblob" || type == "mediumblob" || type == "longblob") {
        return "blob";
    }
    
    // UUID
    if (type == "uuid") return "uuid";
    
    // JSON
    if (type == "json") return "text";  // Store JSON as text
    
    // Enum and Set (store as text)
    if (type == "enum" || type == "set") return "text";
    
    // Default to text for unknown types
    return "text";
}

std::string SchemaConverter::escape_cql_identifier(const std::string& name) {
    // CQL keywords that need quoting
    static const std::vector<std::string> keywords = {
        "add", "allow", "alter", "and", "any", "apply", "asc", "authorize",
        "batch", "begin", "by", "columnfamily", "create", "delete", "desc",
        "drop", "from", "grant", "in", "index", "insert", "into", "keyspace",
        "limit", "modify", "norecursive", "of", "on", "order", "primary",
        "revoke", "schema", "select", "set", "table", "to", "token",
        "truncate", "unlogged", "update", "use", "using", "where", "with"
    };
    
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
    
    bool needs_quoting = std::find(keywords.begin(), keywords.end(), lower_name) != keywords.end();
    
    // Also quote if name contains special characters or starts with digit
    if (!needs_quoting) {
        for (char c : name) {
            if (!std::isalnum(c) && c != '_') {
                needs_quoting = true;
                break;
            }
        }
        if (!name.empty() && std::isdigit(name[0])) {
            needs_quoting = true;
        }
    }
    
    if (needs_quoting) {
        return "\"" + name + "\"";
    }
    return name;
}

std::string SchemaConverter::escape_cql_string(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped += '\'';
    for (char c : value) {
        if (c == '\'') {
            escaped += "''";
        } else {
            escaped += c;
        }
    }
    escaped += '\'';
    return escaped;
}

std::string SchemaConverter::generate_create_table(
    const MariaDBTableSchema& schema,
    const std::string& keyspace,
    const std::string& table
) {
    std::ostringstream cql;
    
    cql << "CREATE TABLE IF NOT EXISTS " << escape_cql_identifier(keyspace) << "."
        << escape_cql_identifier(table) << " (\n";
    
    // Column definitions
    bool first = true;
    for (const auto& col : schema.columns) {
        if (!first) {
            cql << ",\n";
        }
        cql << "    " << escape_cql_identifier(col.name) << " " 
            << mariadb_to_cql_type(col);
        first = false;
    }
    
    // Primary key definition
    if (!schema.primary_key_columns.empty()) {
        cql << ",\n    PRIMARY KEY (";
        
        // In ScyllaDB/CQL, the first PK column is the partition key
        // Additional columns become clustering columns
        bool first_pk = true;
        for (const auto& pk_col : schema.primary_key_columns) {
            if (!first_pk) {
                cql << ", ";
            }
            cql << escape_cql_identifier(pk_col);
            first_pk = false;
        }
        cql << ")";
    }
    
    cql << "\n)";
    
    // Add clustering order if we have multiple PK columns
    // For MariaDB to ScyllaDB migration, we typically want ASC order
    if (schema.primary_key_columns.size() > 1) {
        cql << " WITH CLUSTERING ORDER BY (";
        for (size_t i = 1; i < schema.primary_key_columns.size(); i++) {
            if (i > 1) cql << ", ";
            cql << escape_cql_identifier(schema.primary_key_columns[i]) << " ASC";
        }
        cql << ")";
    }
    
    cql << ";";
    
    return cql.str();
}

std::string SchemaConverter::value_to_cql_literal(
    const std::string& value,
    const std::string& mariadb_type,
    bool is_null
) {
    if (is_null) {
        return "NULL";
    }
    
    std::string type = mariadb_type;
    std::transform(type.begin(), type.end(), type.begin(), ::tolower);
    
    // Numeric types don't need quoting
    if (type == "tinyint" || type == "smallint" || type == "mediumint" ||
        type == "int" || type == "integer" || type == "bigint" ||
        type == "float" || type == "double" || type == "real" ||
        type == "decimal" || type == "numeric") {
        return value;
    }
    
    // Boolean
    if (type == "boolean" || type == "bool") {
        return (value == "1" || value == "true" || value == "TRUE") ? "true" : "false";
    }
    
    // Everything else needs string quoting
    return escape_cql_string(value);
}

std::vector<CQLColumnDef> SchemaConverter::generate_column_defs(
    const MariaDBTableSchema& schema
) {
    std::vector<CQLColumnDef> defs;
    
    for (const auto& col : schema.columns) {
        CQLColumnDef def;
        def.name = col.name;
        def.cql_type = mariadb_to_cql_type(col);
        
        // First PK column is partition key, rest are clustering
        auto it = std::find(schema.primary_key_columns.begin(),
                           schema.primary_key_columns.end(),
                           col.name);
        
        if (it != schema.primary_key_columns.end()) {
            size_t pk_index = std::distance(schema.primary_key_columns.begin(), it);
            def.is_partition_key = (pk_index == 0);
            def.is_clustering_key = (pk_index > 0);
        } else {
            def.is_partition_key = false;
            def.is_clustering_key = false;
        }
        
        defs.push_back(def);
    }
    
    return defs;
}

} // namespace mariadb
} // namespace scylla_migrator
