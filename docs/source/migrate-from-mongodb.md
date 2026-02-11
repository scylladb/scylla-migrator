# Migrate from MongoDB to ScyllaDB

This guide explains how to migrate data from a MongoDB collection to a ScyllaDB table using the ScyllaDB Migrator.

## Overview

The MongoDB migration feature provides:

- **Automatic schema inference**: Analyzes MongoDB documents to determine optimal ScyllaDB schema
- **Automatic table creation**: Creates the target ScyllaDB table with appropriate data types
- **Parallel data migration**: Uses Spark's distributed computing for high-throughput migration
- **Real-time synchronization**: Optionally streams changes via MongoDB change streams

## Prerequisites

### MongoDB Requirements

- MongoDB 3.6 or later (for change stream support)
- Replica set configuration (required for change streams)
- Read access to the source collection
- Access to the oplog (for change stream functionality)

### ScyllaDB Requirements

- ScyllaDB cluster with sufficient capacity
- Network connectivity from Spark cluster
- Appropriate user permissions to create keyspaces/tables

### Spark Requirements

- Apache Spark 3.x cluster
- MongoDB Spark Connector
- ScyllaDB Spark Connector

## Configuration

### Basic Configuration

```yaml
source:
  type: mongodb
  host: mongodb-server
  port: 27017
  database: myapp
  collection: users
  splitCount: 256
  batchSize: 1000

target:
  type: scylla
  host: scylla-server
  port: 9042
  keyspace: myapp_migrated
  table: users
  consistencyLevel: LOCAL_QUORUM
  connections: 16

savepoints:
  path: /app/savepoints
  intervalSeconds: 300
```

### With Authentication

```yaml
source:
  type: mongodb
  host: mongodb-server
  port: 27017
  database: myapp
  collection: users
  credentials:
    username: myuser
    password: mypassword
  authSource: admin
  # ...

target:
  type: scylla
  host: scylla-server
  port: 9042
  keyspace: myapp_migrated
  table: users
  credentials:
    username: scylla_user
    password: scylla_pass
  # ...
```

### With Change Streaming

```yaml
source:
  type: mongodb
  host: mongodb-server
  port: 27017
  database: myapp
  collection: users
  replicaSet: rs0  # Required for change streams
  streamChanges: true
  # ...
```

### With Custom Keys

```yaml
source:
  type: mongodb
  host: mongodb-server
  port: 27017
  database: myapp
  collection: events
  partitionKeyField: user_id
  clusteringKeyFields:
    - event_time
    - event_id
  # ...
```

## Running the Migration

1. **Prepare the configuration file**:
   ```bash
   cp config.yaml.mongodb.example config.yaml
   # Edit config.yaml with your settings
   ```

2. **Run the migrator**:
   ```bash
   spark-submit \
     --class com.scylladb.migrator.Migrator \
     --master spark://<spark-master>:7077 \
     --conf spark.scylla.config=/path/to/config.yaml \
     scylla-migrator-assembly.jar
   ```

## Data Type Mapping

MongoDB types are automatically mapped to ScyllaDB/CQL types:

| MongoDB Type | CQL Type | Notes |
|--------------|----------|-------|
| String | text | |
| Int32 | int | |
| Int64/Long | bigint | |
| Double | double | |
| Decimal128 | decimal | |
| Boolean | boolean | |
| Date | timestamp | |
| ObjectId | text | Stored as hex string |
| Binary | blob | |
| Array | list<T> | Element type inferred |
| Document | text | Stored as JSON |

### Nested Documents

Nested MongoDB documents are flattened with underscore separators:

```javascript
// MongoDB document
{
  "_id": ObjectId("..."),
  "name": "John",
  "address": {
    "city": "New York",
    "zip": "10001"
  }
}
```

Becomes ScyllaDB columns:
```sql
_id text PRIMARY KEY,
name text,
address_city text,
address_zip text
```

## Schema Inference

The migrator samples documents to infer the optimal schema:

1. **Field Discovery**: Identifies all fields across sampled documents
2. **Type Analysis**: Determines the most common type for each field
3. **Cardinality Estimation**: Estimates distinct values for key selection
4. **Key Selection**: Chooses optimal partition and clustering keys

### Partition Key Selection Priority

1. User-specified `partitionKeyField`
2. MongoDB `_id` field
3. High-cardinality field with id-like naming
4. Synthetic `_row_id` (last resort)

## Change Stream Processing

When `streamChanges: true`, the migrator:

1. Captures the current oplog position before migration
2. Migrates all existing data
3. Starts tailing the oplog from the captured position
4. Applies all changes (inserts, updates, deletes) to ScyllaDB

### Requirements for Change Streams

- MongoDB 3.6+ with replica set
- `replicaSet` specified in configuration
- Read access to oplog

### Handling Updates

- **Insert/Replace**: Full document upsert
- **Update**: Partial field update when possible
- **Delete**: Row deletion

## Performance Tuning

### Source (MongoDB)

```yaml
source:
  splitCount: 512      # More partitions for larger datasets
  batchSize: 2000      # Larger batches for throughput
  sampleSize: 50000    # More samples for complex schemas
  readPreference: secondary  # Offload from primary
```

### Target (ScyllaDB)

```yaml
target:
  connections: 32      # Match to executor count
  writeBatchSize: 100  # Batch size for writes
```

### Spark Configuration

```bash
spark-submit \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=4 \
  --conf spark.default.parallelism=256 \
  ...
```

## Monitoring

The migrator logs progress and metrics:

```
INFO  - Initial migration complete: 1000000 documents migrated
INFO  - Change stream stats - Events: 5000/min, Inserts: 3000, Updates: 1500, Deletes: 500
```

Monitor ScyllaDB during migration:
- Check Requests Served per Shard metric
- Watch for load balancing across nodes
- Monitor disk and memory usage

## Troubleshooting

### Common Issues

1. **Connection refused to MongoDB**
   - Verify network connectivity
   - Check firewall rules
   - Confirm MongoDB is listening on configured port

2. **Authentication failed**
   - Verify credentials
   - Check `authSource` setting
   - Ensure user has read permissions

3. **Change stream not working**
   - Verify MongoDB is 3.6+
   - Confirm replica set configuration
   - Check oplog access permissions

4. **Type conversion errors**
   - Review document samples for inconsistent types
   - Use explicit `partitionKeyField` for problematic fields
   - Consider data cleanup before migration

### Debug Mode

Enable verbose logging:
```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-Dlog4j.logger.com.scylladb.migrator=DEBUG" \
  ...
```

## Limitations

1. **Schema Evolution**: Cannot handle schema changes during migration
2. **Large Documents**: Documents > 16MB may cause issues
3. **Transactions**: Does not preserve MongoDB transaction boundaries
4. **Geospatial Indexes**: Not automatically created in ScyllaDB
5. **TTL**: MongoDB TTL indexes are not migrated

## Example: Complete Migration Script

```bash
#!/bin/bash

# Build the migrator (if needed)
./build.sh

# Start the migration
spark-submit \
  --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.scylla.config=/app/config.yaml \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.cassandra.connection.host=scylla-server \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  migrator/target/scala-2.13/scylla-migrator-assembly.jar
```
