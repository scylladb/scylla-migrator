# MariaDB to ScyllaDB Migration Support

This PR adds support for migrating data from MariaDB (and MySQL-compatible databases) to ScyllaDB.

## Overview

The MariaDB migration feature enables:
- **Consistent snapshot migration** using MariaDB's `BACKUP STAGE` commands
- **Parallel data transfer** by splitting the source table into PK ranges
- **Change Data Capture (CDC)** via MariaDB binlog streaming
- **Automatic schema conversion** from MariaDB/SQL to ScyllaDB/CQL
- **Group commit awareness** for safe parallel binlog event application

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ScyllaDB Migrator                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Scala/Spark Layer                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐                    │
│  │  Migrator   │  │ MariaDBMigr- │  │  NativeBridge   │                    │
│  │  (Main)     │──│ ator         │──│  (JNI)          │                    │
│  └─────────────┘  └──────────────┘  └────────┬────────┘                    │
├──────────────────────────────────────────────┼──────────────────────────────┤
│  Native C++ Layer                            │                              │
│  ┌───────────────────────────────────────────┼──────────────────────────┐  │
│  │                    libmariadb-scylla-migrator-jni                    │  │
│  │  ┌─────────────────┐  ┌────────────────┐  ┌────────────────┐        │  │
│  │  │ MariaDBConn-    │  │ ScyllaDBConn-  │  │ BinlogStreamer │        │  │
│  │  │ ection          │  │ ection         │  │                │        │  │
│  │  └────────┬────────┘  └───────┬────────┘  └───────┬────────┘        │  │
│  └───────────┼───────────────────┼───────────────────┼──────────────────┘  │
├──────────────┼───────────────────┼───────────────────┼──────────────────────┤
│              │                   │                   │                      │
│  ┌───────────▼───────────┐  ┌────▼──────────┐  ┌─────▼────────┐            │
│  │ MariaDB Connector/C   │  │ cpp-rs-driver │  │ Binlog API   │            │
│  │ (3.4.8)               │  │ (0.5.1)       │  │              │            │
│  └───────────┬───────────┘  └───────┬───────┘  └──────┬───────┘            │
└──────────────┼──────────────────────┼────────────────┼──────────────────────┘
               │                      │                │
         ┌─────▼─────┐          ┌─────▼─────┐    ┌─────▼─────┐
         │  MariaDB  │          │  ScyllaDB │    │  Binlog   │
         │  Server   │          │  Cluster  │    │  Files    │
         └───────────┘          └───────────┘    └───────────┘
```

## Migration Process

### Phase 1: Initialization (Worker 0)

1. Connect to MariaDB
2. Execute `BACKUP STAGE START` and `BACKUP STAGE BLOCK_COMMIT`
3. Capture current GTID position via `SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'`
4. Retrieve source table schema
5. Create target table in ScyllaDB (auto-convert types)
6. Calculate PK ranges for parallel scanning

### Phase 2: Snapshot Transfer (All Workers)

1. Each worker receives assigned PK range
2. Workers connect to both MariaDB and ScyllaDB
3. Stream data using `mysql_use_result()` for memory efficiency
4. Batch writes to ScyllaDB (default: 100 rows per batch)
5. Progress logging every 10,000 rows

### Phase 3: Lock Release (Worker 0)

1. Wait for all workers to complete snapshot
2. Execute `BACKUP STAGE END` to release lock
3. Source database returns to normal operation

### Phase 4: Binlog Streaming (Worker 0, Optional)

1. Connect to MariaDB with replication flags
2. Set slave GTID position to captured value
3. Stream and parse binlog events (INSERT/UPDATE/DELETE)
4. Apply events to ScyllaDB in order
5. Respect group commit boundaries for safe parallelization
6. Continue until stopped

## Data Type Mapping

| MariaDB Type | CQL Type |
|--------------|----------|
| TINYINT | tinyint |
| SMALLINT | smallint |
| MEDIUMINT, INT | int |
| BIGINT | bigint |
| FLOAT | float |
| DOUBLE, REAL | double |
| DECIMAL, NUMERIC | decimal |
| BOOLEAN, BOOL | boolean |
| DATE | date |
| TIME | time |
| DATETIME, TIMESTAMP | timestamp |
| YEAR | int |
| CHAR, VARCHAR, TEXT | text |
| BINARY, VARBINARY, BLOB | blob |
| JSON | text |
| ENUM, SET | text |

## Primary Key Mapping

- First PK column → Partition Key
- Remaining PK columns → Clustering Columns
- Composite primary keys are supported

## Requirements

### MariaDB Server
- Version 10.4+ (for `BACKUP STAGE` support)
- GTID mode enabled (`gtid_mode=ON`)
- Binary logging enabled (`log_bin=ON`)

### Required Privileges
```sql
CREATE USER 'migrator_user'@'%' IDENTIFIED BY 'password';
GRANT SELECT ON source_db.* TO 'migrator_user'@'%';
GRANT RELOAD ON *.* TO 'migrator_user'@'%';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'migrator_user'@'%';
FLUSH PRIVILEGES;
```

### Build Dependencies
- CMake 3.16+
- C++17 compiler (GCC 9+, Clang 10+)
- MariaDB Connector/C 3.4.8
- ScyllaDB cpp-rs-driver 0.5.1
- JDK 8+ (for JNI headers)
- Scala 2.13 / sbt

## Configuration Example

```yaml
source:
  type: mariadb
  host: mariadb.example.com
  port: 3306
  credentials:
    user: migrator_user
    password: your_password
  database: source_db
  table: source_table
  useBackupStage: true
  streamBinlog: true
  splitCount: 256
  fetchSize: 1000

target:
  type: scylla
  host: scylla1.example.com,scylla2.example.com
  port: 9042
  credentials:
    username: scylla_user
    password: scylla_password
  keyspace: target_keyspace
  table: target_table
  connections: 16
  consistencyLevel: LOCAL_QUORUM
```

## Building

```bash
# Build native library
cd native
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Build Scala assembly
cd ../..
sbt assembly
```

## Running

```bash
spark-submit \
  --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.scylla.config=/path/to/config.yaml \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=/path/to/native/lib" \
  --driver-java-options "-Djava.library.path=/path/to/native/lib" \
  scylla-migrator-assembly.jar
```

## New Files

### Native C++ (`native/`)
- `include/mariadb_scylla_migrator.h` - Core C++ API
- `include/mariadb_scylla_migrator_jni.h` - JNI declarations
- `src/mariadb_connection.cpp` - MariaDB operations
- `src/scylladb_connection.cpp` - ScyllaDB operations
- `src/migrator.cpp` - Orchestration logic
- `src/jni_bridge.cpp` - JNI implementations
- `CMakeLists.txt` - Build configuration

### Scala (`migrator/src/main/scala/com/scylladb/migrator/`)
- `MariaDBMigrator.scala` - Migration coordinator
- `config/MariaDBSourceSettings.scala` - Configuration types
- `config/SourceSettings.scala` - Extended source types
- `config/MigratorConfig.scala` - Config parser
- `mariadb/NativeBridge.scala` - JNI wrapper

### Configuration
- `config.yaml.mariadb.example` - Example configuration

## TODO / Future Work

1. **Full Binlog Parsing** - Complete implementation of binlog event parsing using `mysql_binlog_*` functions
2. **Distributed Coordination** - Replace in-memory coordination with ZooKeeper/etcd for multi-node deployment
3. **Composite PK Optimization** - Better handling of multi-column primary keys for range splitting
4. **Schema Evolution** - Support for ALTER TABLE changes during CDC
5. **Metrics & Monitoring** - Prometheus metrics integration
6. **Savepoint Support** - Resume capability for interrupted migrations
7. **MySQL Compatibility** - Testing and fixes for MySQL 8.x (without `BACKUP STAGE`)

## License

Apache License 2.0 (same as scylla-migrator)

## References

- [MariaDB Connector/C Documentation](https://mariadb.com/kb/en/mariadb-connector-c/)
- [MariaDB BACKUP STAGE](https://mariadb.com/kb/en/backup-stage/)
- [MariaDB GTID](https://mariadb.com/kb/en/gtid/)
- [ScyllaDB cpp-rs-driver](https://github.com/scylladb/cpp-rs-driver)
- [ScyllaDB Migrator](https://github.com/scylladb/scylla-migrator)
# MariaDB to ScyllaDB Migration Support

This PR adds support for migrating data from MariaDB (and MySQL-compatible databases) to ScyllaDB.

## Overview

The MariaDB migration feature enables:
- **Consistent snapshot migration** using MariaDB's `BACKUP STAGE` commands
- **Parallel data transfer** by splitting the source table into PK ranges
- **Change Data Capture (CDC)** via MariaDB binlog streaming
- **Automatic schema conversion** from MariaDB/SQL to ScyllaDB/CQL
- **Group commit awareness** for safe parallel binlog event application

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ScyllaDB Migrator                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Scala/Spark Layer                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐                    │
│  │  Migrator   │  │ MariaDBMigr- │  │  NativeBridge   │                    │
│  │  (Main)     │──│ ator         │──│  (JNI)          │                    │
│  └─────────────┘  └──────────────┘  └────────┬────────┘                    │
├──────────────────────────────────────────────┼──────────────────────────────┤
│  Native C++ Layer                            │                              │
│  ┌───────────────────────────────────────────┼──────────────────────────┐  │
│  │                    libmariadb-scylla-migrator-jni                    │  │
│  │  ┌─────────────────┐  ┌────────────────┐  ┌────────────────┐        │  │
│  │  │ MariaDBConn-    │  │ ScyllaDBConn-  │  │ BinlogStreamer │        │  │
│  │  │ ection          │  │ ection         │  │                │        │  │
│  │  └────────┬────────┘  └───────┬────────┘  └───────┬────────┘        │  │
│  └───────────┼───────────────────┼───────────────────┼──────────────────┘  │
├──────────────┼───────────────────┼───────────────────┼──────────────────────┤
│              │                   │                   │                      │
│  ┌───────────▼───────────┐  ┌────▼──────────┐  ┌─────▼────────┐            │
│  │ MariaDB Connector/C   │  │ cpp-rs-driver │  │ Binlog API   │            │
│  │ (3.4.8)               │  │ (0.5.1)       │  │              │            │
│  └───────────┬───────────┘  └───────┬───────┘  └──────┬───────┘            │
└──────────────┼──────────────────────┼────────────────┼──────────────────────┘
               │                      │                │
         ┌─────▼─────┐          ┌─────▼─────┐    ┌─────▼─────┐
         │  MariaDB  │          │  ScyllaDB │    │  Binlog   │
         │  Server   │          │  Cluster  │    │  Files    │
         └───────────┘          └───────────┘    └───────────┘
```

## Migration Process

### Phase 1: Initialization (Worker 0)

1. Connect to MariaDB
2. Execute `BACKUP STAGE START` and `BACKUP STAGE BLOCK_COMMIT`
3. Capture current GTID position via `SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'`
4. Retrieve source table schema
5. Create target table in ScyllaDB (auto-convert types)
6. Calculate PK ranges for parallel scanning

### Phase 2: Snapshot Transfer (All Workers)

1. Each worker receives assigned PK range
2. Workers connect to both MariaDB and ScyllaDB
3. Stream data using `mysql_use_result()` for memory efficiency
4. Batch writes to ScyllaDB (default: 100 rows per batch)
5. Progress logging every 10,000 rows

### Phase 3: Lock Release (Worker 0)

1. Wait for all workers to complete snapshot
2. Execute `BACKUP STAGE END` to release lock
3. Source database returns to normal operation

### Phase 4: Binlog Streaming (Worker 0, Optional)

1. Connect to MariaDB with replication flags
2. Set slave GTID position to captured value
3. Stream and parse binlog events (INSERT/UPDATE/DELETE)
4. Apply events to ScyllaDB in order
5. Respect group commit boundaries for safe parallelization
6. Continue until stopped

## Data Type Mapping

| MariaDB Type | CQL Type |
|--------------|----------|
| TINYINT | tinyint |
| SMALLINT | smallint |
| MEDIUMINT, INT | int |
| BIGINT | bigint |
| FLOAT | float |
| DOUBLE, REAL | double |
| DECIMAL, NUMERIC | decimal |
| BOOLEAN, BOOL | boolean |
| DATE | date |
| TIME | time |
| DATETIME, TIMESTAMP | timestamp |
| YEAR | int |
| CHAR, VARCHAR, TEXT | text |
| BINARY, VARBINARY, BLOB | blob |
| JSON | text |
| ENUM, SET | text |

## Primary Key Mapping

- First PK column → Partition Key
- Remaining PK columns → Clustering Columns
- Composite primary keys are supported

## Requirements

### MariaDB Server
- Version 10.4+ (for `BACKUP STAGE` support)
- GTID mode enabled (`gtid_mode=ON`)
- Binary logging enabled (`log_bin=ON`)

### Required Privileges
```sql
CREATE USER 'migrator_user'@'%' IDENTIFIED BY 'password';
GRANT SELECT ON source_db.* TO 'migrator_user'@'%';
GRANT RELOAD ON *.* TO 'migrator_user'@'%';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'migrator_user'@'%';
FLUSH PRIVILEGES;
```

### Build Dependencies
- CMake 3.16+
- C++17 compiler (GCC 9+, Clang 10+)
- MariaDB Connector/C 3.4.8
- ScyllaDB cpp-rs-driver 0.5.1
- JDK 8+ (for JNI headers)
- Scala 2.13 / sbt

## Configuration Example

```yaml
source:
  type: mariadb
  host: mariadb.example.com
  port: 3306
  credentials:
    user: migrator_user
    password: your_password
  database: source_db
  table: source_table
  useBackupStage: true
  streamBinlog: true
  splitCount: 256
  fetchSize: 1000

target:
  type: scylla
  host: scylla1.example.com,scylla2.example.com
  port: 9042
  credentials:
    username: scylla_user
    password: scylla_password
  keyspace: target_keyspace
  table: target_table
  connections: 16
  consistencyLevel: LOCAL_QUORUM
```

## Building

```bash
# Build native library
cd native
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Build Scala assembly
cd ../..
sbt assembly
```

## Running

```bash
spark-submit \
  --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.scylla.config=/path/to/config.yaml \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=/path/to/native/lib" \
  --driver-java-options "-Djava.library.path=/path/to/native/lib" \
  scylla-migrator-assembly.jar
```

## New Files

### Native C++ (`native/`)
- `include/mariadb_scylla_migrator.h` - Core C++ API
- `include/mariadb_scylla_migrator_jni.h` - JNI declarations
- `src/mariadb_connection.cpp` - MariaDB operations
- `src/scylladb_connection.cpp` - ScyllaDB operations
- `src/migrator.cpp` - Orchestration logic
- `src/jni_bridge.cpp` - JNI implementations
- `CMakeLists.txt` - Build configuration

### Scala (`migrator/src/main/scala/com/scylladb/migrator/`)
- `MariaDBMigrator.scala` - Migration coordinator
- `config/MariaDBSourceSettings.scala` - Configuration types
- `config/SourceSettings.scala` - Extended source types
- `config/MigratorConfig.scala` - Config parser
- `mariadb/NativeBridge.scala` - JNI wrapper

### Configuration
- `config.yaml.mariadb.example` - Example configuration

## TODO / Future Work

1. **Full Binlog Parsing** - Complete implementation of binlog event parsing using `mysql_binlog_*` functions
2. **Distributed Coordination** - Replace in-memory coordination with ZooKeeper/etcd for multi-node deployment
3. **Composite PK Optimization** - Better handling of multi-column primary keys for range splitting
4. **Schema Evolution** - Support for ALTER TABLE changes during CDC
5. **Metrics & Monitoring** - Prometheus metrics integration
6. **Savepoint Support** - Resume capability for interrupted migrations
7. **MySQL Compatibility** - Testing and fixes for MySQL 8.x (without `BACKUP STAGE`)

## License

Apache License 2.0 (same as scylla-migrator)

## References

- [MariaDB Connector/C Documentation](https://mariadb.com/kb/en/mariadb-connector-c/)
- [MariaDB BACKUP STAGE](https://mariadb.com/kb/en/backup-stage/)
- [MariaDB GTID](https://mariadb.com/kb/en/gtid/)
- [ScyllaDB cpp-rs-driver](https://github.com/scylladb/cpp-rs-driver)
- [ScyllaDB Migrator](https://github.com/scylladb/scylla-migrator)
