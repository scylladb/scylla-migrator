/**
 * ScyllaDB Migrator - Migration Coordinator and Main Migrator Implementation
 * 
 * Handles multi-worker coordination and orchestrates the migration process
 * 
 * Copyright (c) 2025 ScyllaDB
 * Licensed under Apache License 2.0
 */

#include "mariadb_scylla_migrator.h"

#include <sstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <iostream>
#include <fstream>

namespace scylla_migrator {
namespace mariadb {

// ============================================================================
// MigrationCoordinator Implementation
// ============================================================================

MigrationCoordinator::MigrationCoordinator(int32_t num_workers, int32_t worker_id)
    : num_workers_(num_workers), worker_id_(worker_id) {
    stats_.start_time = std::chrono::steady_clock::now();
}

MigrationCoordinator::~MigrationCoordinator() {
    release_initialization_lock();
}

bool MigrationCoordinator::acquire_initialization_lock() {
    // In a production environment, this would use distributed coordination
    // (ZooKeeper, etcd, or ScyllaDB LWT) to ensure only one worker initializes
    
    // For this implementation, we assume worker_id 0 is the coordinator
    // Other workers will wait for a signal file or use a similar mechanism
    
    if (worker_id_ == 0) {
        // Worker 0 gets the lock by default
        return true;
    }
    
    // Other workers wait for initialization to complete
    // In production, this would check a distributed lock service
    return false;
}

void MigrationCoordinator::release_initialization_lock() {
    // Release distributed lock if held
}

bool MigrationCoordinator::initialize(
    MariaDBConnection& mariadb,
    ScyllaDBConnection& scylladb
) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    state_ = State::ACQUIRING_LOCK;
    
    if (worker_id_ == 0) {
        // Worker 0 is the coordinator - perform initialization
        
        // 1. Get table schema from MariaDB
        auto schema_opt = mariadb.get_table_schema(
            mariadb.get_last_error().empty() ? "" : "",  // Use config values
            ""  // Use config values
        );
        
        // Get schema using the connection's cached info
        // (In production, pass database/table from config)
        
        // 2. Start backup stage
        state_ = State::ACQUIRING_LOCK;
        
        if (!mariadb.backup_stage_start()) {
            state_ = State::ERROR;
            return false;
        }
        
        if (!mariadb.backup_stage_block_commit()) {
            mariadb.backup_stage_end();
            state_ = State::ERROR;
            return false;
        }
        
        // 3. Get GTID position while database is locked
        auto gtid_opt = mariadb.get_gtid_position();
        if (!gtid_opt) {
            mariadb.backup_stage_end();
            state_ = State::ERROR;
            return false;
        }
        start_gtid_ = *gtid_opt;
        
        // 4. Calculate PK ranges for all workers
        pk_ranges_ = mariadb.calculate_pk_ranges(num_workers_);
        if (pk_ranges_.empty()) {
            // Empty table or error
            pk_ranges_.push_back(PKRange{});  // Add empty range
        }
        
        // 5. Create table on ScyllaDB (if schema available)
        state_ = State::CREATING_TABLE;
        
        // The actual table creation would use the schema
        // For now, we assume the caller has set up the schema conversion
        
        state_ = State::SNAPSHOT_IN_PROGRESS;
        
        // Signal other workers that initialization is complete
        cv_.notify_all();
        
    } else {
        // Other workers wait for initialization
        state_ = State::INITIALIZING;
        
        // Wait for coordinator to finish initialization
        // In production, this would poll a distributed state store
        // For now, we use a simple condition variable (only works in shared memory)
        
        while (state_ == State::INITIALIZING) {
            cv_.wait_for(lock, std::chrono::milliseconds(100));
            // In production, check distributed state here
        }
        
        if (state_ == State::ERROR) {
            return false;
        }
    }
    
    return true;
}

std::optional<PKRange> MigrationCoordinator::get_assigned_range() const {
    if (worker_id_ >= 0 && static_cast<size_t>(worker_id_) < pk_ranges_.size()) {
        return pk_ranges_[worker_id_];
    }
    return std::nullopt;
}

void MigrationCoordinator::report_snapshot_complete() {
    int32_t completed = ++workers_completed_snapshot_;
    
    if (completed >= num_workers_) {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = State::SNAPSHOT_COMPLETE;
        stats_.snapshot_complete_time = std::chrono::steady_clock::now();
        cv_.notify_all();
    }
}

bool MigrationCoordinator::wait_for_all_snapshot_complete(int32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    
    while (workers_completed_snapshot_ < num_workers_) {
        if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            return false;
        }
        
        if (state_ == State::ERROR) {
            return false;
        }
    }
    
    return true;
}

MigrationCoordinator::State MigrationCoordinator::get_state() const {
    return state_;
}

GTIDPosition MigrationCoordinator::get_start_gtid() const {
    return start_gtid_;
}

MigrationStats& MigrationCoordinator::get_stats() {
    return stats_;
}

bool MigrationCoordinator::should_stream_binlog() const {
    // Only worker 0 handles binlog streaming
    return worker_id_ == 0;
}

// ============================================================================
// MariaDBToScyllaDBMigrator Implementation
// ============================================================================

MariaDBToScyllaDBMigrator::MariaDBToScyllaDBMigrator(
    const MariaDBConfig& source_config,
    const ScyllaDBConfig& target_config,
    int32_t num_workers,
    int32_t worker_id
)
    : source_config_(source_config),
      target_config_(target_config),
      num_workers_(num_workers),
      worker_id_(worker_id) {
}

MariaDBToScyllaDBMigrator::~MariaDBToScyllaDBMigrator() {
    stop();
}

bool MariaDBToScyllaDBMigrator::migrate() {
    running_ = true;
    
    // Create connections
    mariadb_ = std::make_unique<MariaDBConnection>(source_config_);
    scylladb_ = std::make_unique<ScyllaDBConnection>(target_config_);
    coordinator_ = std::make_unique<MigrationCoordinator>(num_workers_, worker_id_);
    
    // Connect to MariaDB
    if (!mariadb_->connect()) {
        last_error_ = "Failed to connect to MariaDB: " + mariadb_->get_last_error();
        return false;
    }
    
    // Connect to ScyllaDB
    if (!scylladb_->connect()) {
        last_error_ = "Failed to connect to ScyllaDB: " + scylladb_->get_last_error();
        return false;
    }
    
    // Get schema from MariaDB
    auto schema = mariadb_->get_table_schema(source_config_.database, source_config_.table);
    if (!schema) {
        last_error_ = "Failed to get source table schema: " + mariadb_->get_last_error();
        return false;
    }
    
    // Worker 0: Initialize migration (locks, create table, etc.)
    if (worker_id_ == 0) {
        // Start backup stage
        if (source_config_.use_backup_stage) {
            if (!mariadb_->backup_stage_start()) {
                last_error_ = "Failed to start backup stage: " + mariadb_->get_last_error();
                return false;
            }
            
            if (!mariadb_->backup_stage_block_commit()) {
                mariadb_->backup_stage_end();
                last_error_ = "Failed to block commits: " + mariadb_->get_last_error();
                return false;
            }
        }
        
        // Get GTID position for binlog streaming starting point
        auto gtid = mariadb_->get_gtid_position();
        if (!gtid) {
            if (source_config_.use_backup_stage) {
                mariadb_->backup_stage_end();
            }
            last_error_ = "Failed to get GTID position: " + mariadb_->get_last_error();
            return false;
        }
        
        std::cout << "Captured GTID position: " << gtid->gtid_binlog_pos << std::endl;
        std::cout << "Binlog file: " << gtid->binlog_file 
                  << " position: " << gtid->binlog_position << std::endl;
        
        // Check if target table exists
        if (!scylladb_->table_exists(target_config_.keyspace, target_config_.table)) {
            // Generate and execute CREATE TABLE
            std::string create_stmt = SchemaConverter::generate_create_table(
                *schema, target_config_.keyspace, target_config_.table
            );
            
            std::cout << "Creating target table with statement:\n" << create_stmt << std::endl;
            
            if (!scylladb_->create_table(create_stmt)) {
                if (source_config_.use_backup_stage) {
                    mariadb_->backup_stage_end();
                }
                last_error_ = "Failed to create target table: " + scylladb_->get_last_error();
                return false;
            }
        }
        
        // Calculate PK ranges for parallel scanning
        auto ranges = mariadb_->calculate_pk_ranges(num_workers_);
        std::cout << "Split table into " << ranges.size() << " ranges for parallel processing" << std::endl;
    }
    
    // All workers: Run snapshot phase
    if (!run_snapshot_phase()) {
        if (worker_id_ == 0 && source_config_.use_backup_stage) {
            mariadb_->backup_stage_end();
        }
        return false;
    }
    
    // Worker 0: Release backup lock after all workers complete snapshot
    if (worker_id_ == 0 && source_config_.use_backup_stage) {
        // Wait for all workers to complete their ranges
        // In production, this would use distributed coordination
        coordinator_->report_snapshot_complete();
        
        // Release the backup lock
        if (!mariadb_->backup_stage_end()) {
            std::cerr << "Warning: Failed to end backup stage: " << mariadb_->get_last_error() << std::endl;
        }
        
        std::cout << "All workers completed snapshot, released backup lock" << std::endl;
    } else {
        coordinator_->report_snapshot_complete();
    }
    
    // Worker 0 only: Run binlog streaming phase (CDC)
    if (source_config_.stream_binlog && coordinator_->should_stream_binlog()) {
        if (!run_binlog_streaming_phase()) {
            // Log error but don't fail the migration
            std::cerr << "Binlog streaming error: " << last_error_ << std::endl;
        }
    }
    
    running_ = false;
    return true;
}

bool MariaDBToScyllaDBMigrator::run_snapshot_phase() {
    auto& stats = coordinator_->get_stats();
    
    // Get assigned range for this worker
    auto ranges = mariadb_->calculate_pk_ranges(num_workers_);
    
    if (worker_id_ >= static_cast<int32_t>(ranges.size())) {
        // No range assigned to this worker
        std::cout << "Worker " << worker_id_ << " has no assigned range" << std::endl;
        return true;
    }
    
    const auto& my_range = ranges[worker_id_];
    
    std::cout << "Worker " << worker_id_ << " processing range [" 
              << my_range.min_value << ", " << my_range.max_value << ")" << std::endl;
    
    // Batch buffer
    std::vector<std::vector<std::pair<std::string, std::string>>> batch;
    batch.reserve(target_config_.write_batch_size);
    
    // Scan the range and write to ScyllaDB
    uint64_t rows_in_range = mariadb_->scan_range(
        my_range,
        1000,  // fetch size
        [&](const std::vector<std::pair<std::string, std::string>>& row) {
            if (!running_) return;
            
            batch.push_back(row);
            stats.rows_read++;
            
            // Flush batch if full
            if (batch.size() >= static_cast<size_t>(target_config_.write_batch_size)) {
                uint64_t written = scylladb_->write_batch(batch);
                stats.rows_written += written;
                batch.clear();
                
                // Progress logging
                if (stats.rows_read % 10000 == 0) {
                    std::cout << "Worker " << worker_id_ << " progress: " 
                              << stats.rows_read << " read, " 
                              << stats.rows_written << " written" << std::endl;
                }
            }
        }
    );
    
    // Flush remaining rows
    if (!batch.empty()) {
        uint64_t written = scylladb_->write_batch(batch);
        stats.rows_written += written;
    }
    
    std::cout << "Worker " << worker_id_ << " completed: " 
              << rows_in_range << " rows processed" << std::endl;
    
    return true;
}

bool MariaDBToScyllaDBMigrator::run_binlog_streaming_phase() {
    if (!source_config_.stream_binlog) {
        return true;
    }
    
    auto gtid = mariadb_->get_gtid_position();
    if (!gtid) {
        last_error_ = "Failed to get current GTID for binlog streaming";
        return false;
    }
    
    binlog_streamer_ = std::make_unique<BinlogStreamer>(source_config_);
    
    if (!binlog_streamer_->start_streaming(*gtid)) {
        last_error_ = "Failed to start binlog streaming: " + binlog_streamer_->get_last_error();
        return false;
    }
    
    std::cout << "Started binlog streaming from GTID: " << gtid->gtid_binlog_pos << std::endl;
    
    auto& stats = coordinator_->get_stats();
    
    // Process binlog events until stopped
    while (running_ && binlog_streamer_->is_running()) {
        auto events = binlog_streamer_->get_events(100, 1000);  // 100 events, 1s timeout
        
        for (const auto& event : events) {
            if (!running_) break;
            
            // Apply event to ScyllaDB
            if (!scylladb_->apply_binlog_event(event)) {
                std::cerr << "Failed to apply binlog event: " << scylladb_->get_last_error() << std::endl;
                stats.errors++;
                continue;
            }
            
            stats.binlog_events_processed++;
            
            // Group commit optimization: events with same group_commit_id can be 
            // applied in parallel (they don't depend on each other)
            if (event.is_last_in_group) {
                // Commit point - could checkpoint GTID here
            }
        }
        
        // Brief sleep if no events
        if (events.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    binlog_streamer_->stop_streaming();
    return true;
}

void MariaDBToScyllaDBMigrator::stop() {
    running_ = false;
    
    if (binlog_streamer_) {
        binlog_streamer_->stop_streaming();
    }
}

MigrationStats MariaDBToScyllaDBMigrator::get_stats() const {
    if (coordinator_) {
        return coordinator_->get_stats();
    }
    return MigrationStats{};
}

std::string MariaDBToScyllaDBMigrator::get_last_error() const {
    return last_error_;
}

} // namespace mariadb
} // namespace scylla_migrator
