# Feature Specification: CQL-to-CQL Migration (Cassandra, ScyllaDB, DataStax Astra)

**Feature Branch**: `001-cql-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/migrate-from-cassandra-or-parquet.rst` (sections "Reading from Apache Cassandra or ScyllaDB" and "Configuring the Destination"); `docs/source/configuration.rst` (sections "Overview", "Source" type list, "CQL-Compatible Source (Apache Cassandra, ScyllaDB)", "DataStax Astra (Cloud Secure Connect bundle)" under Source, "Target" type list, "CQL-Compatible Target (Apache Cassandra, ScyllaDB)", "DataStax Astra (Cloud Secure Connect bundle)" under Target); source code at commit 06835d0 (`config/SourceSettings.scala`, `config/TargetSettings.scala`, `config/CloudConfig.scala`, `config/ContactPoints.scala`, `config/HostValidation.scala`, `config/SSLOptions.scala`, `config/Credentials.scala`, `config/MigratorConfig.scala`, `Connectors.scala`, `ConsistencyLevelUtils.scala`, `Migrator.scala`, `readers/Cassandra.scala`, `readers/CollectionBatching.scala`, `writers/Scylla.scala`, `scylla/ScyllaMigrator.scala`, `config.yaml.example`, tests under `tests/src/test/`)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Copy a table from Cassandra/ScyllaDB to ScyllaDB (Priority: P1)

A migration operator has a table in an Apache Cassandra or ScyllaDB cluster and wants a complete
copy of its rows in a ScyllaDB (or Cassandra) cluster. They write a configuration file with a
`source` of `type: cassandra` (or its alias `scylla`) (host, port, keyspace, table,
consistencyLevel, fetchSize, preserveTimestamps, optionally splitCount and connections) and a
`target` of `type: cassandra` (or `scylla`) (host, port, keyspace, table, consistencyLevel,
stripTrailingZerosForDecimals, optionally connections), creates the target keyspace and table,
then submits the Migrator job.

**Why this priority**: This is the core migration path for CQL users; every other CQL feature
(timestamps, TLS, Astra, filters) is a refinement of it.

**Independent Test**: Load a source table with known rows, run the Migrator with a minimal
source/target configuration, and confirm with the Validator (see `009-migration-validation`) that
every source row is present and equal in the target.

**Acceptance Scenarios**:

1. **Given** a source table and a configuration with `source.type: cassandra` and
   `target.type: cassandra`, **When** the Migrator runs to completion, **Then** every row of the
   source table exists in the target table with identical column values.
2. **Given** the target table does not exist, **When** the Migrator runs, **Then** the write fails
   and no data is migrated: the migrator never creates CQL target keyspaces or tables, so the
   operator must create the target table (with the source schema, after any renames, see
   `007-column-renames`) before submitting the job.
3. **Given** the target table exists but lacks a column that the source has (after configured
   renames), **When** the migration runs, **Then** the write fails, because the target table must
   have the same schema as the source.
4. **Given** `host` is given as a YAML list `[node-1, node-2, node-3]` or as the comma-separated
   string `node-1,node-2,node-3`, **When** one of those nodes is down at startup, **Then** the
   Migrator still connects through one of the other contact points.
5. **Given** a configuration whose `target` property is missing, **When** the Migrator starts,
   **Then** it fails at startup with `DecodingFailure at .target: Missing required field` (see
   `010-spark-cluster-and-job-submission` for configuration parse errors in general).
6. **Given** `source.type: scylla`, **When** the configuration is loaded, **Then** it is accepted
   exactly like `source.type: cassandra`.
7. **Given** a source or target without `cloud` that omits `host` or `port`, **When** the Migrator
   starts, **Then** it fails with "'host' and 'port' are required unless 'cloud' is set."
8. **Given** a `host` entry containing URL metacharacters (`/ ? # & @`), an IPv4 address wrapped in
   brackets, or an empty contact-point list, or a `port` outside 1–65535, **When** the Migrator
   starts, **Then** it fails with a `DecodingFailure` describing the invalid host or port (bare or
   bracketed IPv6 addresses are accepted).
9. **Given** the source is `cassandra` and the target is neither a CQL target nor a Parquet target
   (see `011-cql-to-parquet-export`), **When** the Migrator starts, **Then** it stops before reading
   data with "Unsupported combination of source and target".

---

### User Story 2 - Preserve TTLs and WRITETIMEs of cells (Priority: P1)

The operator wants the migrated data to keep each cell's remaining TTL and original WRITETIME, so
that data expires on schedule in the target and later writes (e.g. dual writes) win over migrated
data by normal last-write-wins rules. They set `source.preserveTimestamps: true`.

**Why this priority**: Correct expiry and write-ordering are part of data correctness
(Constitution principle I) for most production CQL migrations.

**Independent Test**: Insert rows with known `USING TTL` / `USING TIMESTAMP` values (including
frozen collections), migrate with `preserveTimestamps: true`, and compare TTL/WRITETIME with the
Validator using `compareTimestamps: true` and `ttlToleranceMillis`.

**Acceptance Scenarios**:

1. **Given** `preserveTimestamps: true` and a table without non-frozen collections, **When** the
   migration completes, **Then** every target cell carries the source cell's WRITETIME and its
   remaining TTL (as of read time).
2. **Given** `preserveTimestamps: true` and a table with frozen collections
   (`frozen<list/map/set>`), **When** the migration runs, **Then** each frozen collection is treated
   as a single cell and its TTL/WRITETIME are preserved.
3. **Given** `preserveTimestamps: true`, `preserveCollectionTimestamps: false` (the default) and a
   table with non-frozen (multi-cell) collection columns, **When** the migration starts, **Then** it
   fails before reading data with an error that suggests freezing the columns, enabling
   `preserveCollectionTimestamps`, or setting `preserveTimestamps: false`.
4. **Given** `preserveTimestamps: true` and a table with a non-frozen user-defined type column,
   **When** the migration starts, **Then** it fails before reading data (freeze the column or
   disable `preserveTimestamps`).
5. **Given** `preserveTimestamps: true` and a source table with a column named `ttl`,
   `writetime`, `<column>_ttl` or `<column>_writetime`, **When** the migration starts, **Then** it
   fails before reading data because those names are reserved for timestamp preservation.
6. **Given** `preserveTimestamps: true` and a table with only primary-key columns, **When** the
   migration starts, **Then** it logs "No regular columns in the table - disabling timestamp
   preservation" and copies the rows without timestamps.

---

### User Story 3 - Preserve per-element timestamps of non-frozen collections (Priority: P2)

The operator's table has non-frozen `set` or `map` columns whose elements each carry their own
TTL/WRITETIME. They opt in with `preserveTimestamps: true` and
`preserveCollectionTimestamps: true`, stop writes to the source, and run the migration on a
Cassandra 5.0+ or ScyllaDB 2026.2+ source.

**Why this priority**: Needed for full fidelity on collection-heavy schemas, but opt-in, costly
and restricted to specific source versions and table shapes.

**Independent Test**: Populate a quiesced Cassandra 5.0+ (or ScyllaDB 2026.2+) table with
non-frozen sets/maps whose elements have different TTL/WRITETIME values, migrate, and confirm with
the Validator that per-element TTL/WRITETIME match within `ttlToleranceMillis`.

**Acceptance Scenarios**:

1. **Given** both options are true and the source is Cassandra 5.0+ or ScyllaDB 2026.2+ (detected
   automatically), **When** the migration completes, **Then** each element of each non-frozen set
   or map in the target carries the original element's WRITETIME and remaining TTL.
2. **Given** `preserveCollectionTimestamps: true` but `preserveTimestamps: false`, **When** the
   migration runs, **Then** `preserveCollectionTimestamps` has no effect.
3. **Given** the table has a static non-frozen collection column, **When** the migration starts,
   **Then** the table is rejected before the migration starts.
4. **Given** a ScyllaDB 2026.2+ source whose primary key includes a frozen tuple, UDT or
   collection, **When** the migration starts, **Then** the table is rejected before the migration
   starts (the Cassandra 5.0+ path has no such limit).
5. **Given** a non-frozen list column, or a set/map whose element/key type is not
   text/ascii/varchar or an integer type, **When** collection timestamp preservation is requested,
   **Then** that column is unsupported.
6. **Given** a target row already holds collection elements that are older than every source
   element (e.g. stale or target-only entries from an earlier run), **When** the row is restored,
   **Then** the oldest-WRITETIME group is written as a whole-collection assignment whose tombstone
   (one microsecond older) removes those older target elements, and the remaining groups are
   appended.
7. **Given** a target row holds collection elements newer than every source element, **When** the
   row is restored, **Then** those newer target elements are kept (last write wins).
8. **Given** a null or empty source collection, **When** the row is restored, **Then** the target
   collection is left untouched.
9. **Given** the migration is interrupted while collection timestamps are being preserved,
   **When** it is re-run with the same configuration, **Then** it resumes from the last completed
   batch (see `008-savepoints-and-resume`).
10. **Given** both options are true but the source accepts neither the Cassandra 5.0+ nor the
    ScyllaDB 2026.2+ per-element metadata form, **When** the migration starts, **Then** it fails
    before reading data with an error naming the column and the required source versions.
11. **Given** a single non-frozen collection cell with more than 100,000 elements, **When** it is
    migrated, **Then** a warning about memory/throughput is logged; above 1,000,000 elements the
    migration aborts unless the executor JVM property `scylla.migrator.maxCollectionElements` is
    raised (a non-positive value disables the cap).
12. **Given** `preserveCollectionTimestamps: true` but `preserveTimestamps: false`, **When** the
    migration starts, **Then** a warning says the option has no effect.

---

### User Story 4 - Migrate from or to DataStax Astra with a secure-connect bundle (Priority: P2)

The operator migrates from (or to) a managed DataStax Astra DB cluster reached through an SNI
proxy. Instead of `host`/`port`/`localDC`/`sslOptions`, they set
`cloud.secureBundlePath` to the Astra secure-connect bundle and supply the client id/secret
through `credentials`.

**Why this priority**: Astra is a supported managed source and target; without it those users
cannot migrate at all.

**Independent Test**: Configure `source.cloud.secureBundlePath` (or `target.cloud...`) with a
valid bundle and credentials, run a migration of a small table, and validate the row count;
separately, add `host` next to `cloud` and confirm the configuration is rejected at startup.

**Acceptance Scenarios**:

1. **Given** `cloud.secureBundlePath` points to a bundle readable from the driver and every
   executor, and `credentials` hold the client id and secret, **When** the Migrator runs, **Then**
   it connects through the bundle's contact points, local datacenter and TLS material.
2. **Given** `cloud` is set together with any of `host`, `port`, `localDC` or `sslOptions` in the
   same source or target, **When** the Migrator starts, **Then** it rejects the configuration
   before reading or writing data.
3. **Given** the bundle is passed with `spark-submit --files s3://my-bucket/secure-connect-prod.zip`
   and `secureBundlePath: secure-connect-prod.zip` (bare filename), **When** the job runs, **Then**
   every executor finds a local copy of the bundle before any task runs.
4. **Given** `secureBundlePath` is an absolute filesystem path, an `https://` URL reachable from
   every node, a bare filename distributed via `--files`, or an `s3://`/`s3a://` URL, **When** the
   Migrator starts, **Then** each form is accepted (an absolute path is converted to a `file://`
   URL at runtime); a `file://` URL is accepted too.
5. **Given** `secureBundlePath` is empty, a plain `http://` URL, a URL with user-info credentials
   or a query string, or a relative path containing a directory (e.g. `dir/bundle.zip`), **When**
   the Migrator starts, **Then** the configuration is rejected with a `DecodingFailure`.

---

### User Story 5 - Secure and authenticated connections (Priority: P2)

The operator's clusters require authentication and TLS. They set `credentials` and `sslOptions`
(trust store, optional key store for client authentication, protocol, enabled algorithms) on the
source and/or target, and optionally `localDC` to pin a datacenter.

**Why this priority**: Most production clusters require authentication and encryption (Constitution
principle VI).

**Independent Test**: Run a migration against TLS-only clusters with `sslOptions.enabled: true`,
a trust store distributed with `--files`, and credentials; confirm the job connects and copies data.

**Acceptance Scenarios**:

1. **Given** `sslOptions.enabled: true` with `trustStorePath` and `trustStorePassword`, **When**
   the job runs with the trust store distributed through `spark-submit --files`, **Then** the
   driver and the worker nodes open TLS connections to the cluster.
2. **Given** `sslOptions.clientAuthEnabled: true` with `keyStorePath`/`keyStorePassword`,
   **When** the job runs, **Then** the migrator presents the client certificate.
3. **Given** `credentials.username`/`credentials.password`, **When** the job connects, **Then**
   it authenticates with them.
4. **Given** `sslOptions` without `trustStoreType`, `keyStoreType`, `protocol` or
   `enabledAlgorithms`, **When** the job connects, **Then** `JKS`, `JKS`, `TLS` and the cipher
   suites `TLS_RSA_WITH_AES_128_CBC_SHA`, `TLS_RSA_WITH_AES_256_CBC_SHA` are used.

---

### User Story 6 - Migrate a subset of rows with a filter (Priority: P3)

The operator only wants rows matching a CQL condition and sets
`source.where: race_start_date = '2015-05-27' AND race_end_date = '2015-05-27'`.

**Why this priority**: Useful for partial or staged migrations, but not needed for a full copy.

**Independent Test**: Migrate a table with a `where` filter and confirm that only matching rows
appear in the target.

**Acceptance Scenarios**:

1. **Given** `source.where` is set, **When** the migration completes, **Then** only rows satisfying
   the condition are written to the target.

---

### User Story 7 - Force a uniform TTL/WRITETIME on written rows (Priority: P3)

When timestamps are not preserved, the operator wants every written row to receive the same TTL
and/or the same WRITETIME — e.g. a WRITETIME just before dual writes started, so that dual writes
always win over migrated data, even for collections. They set `target.writeTTLInS` and/or
`target.writeWritetimestampInuS`.

**Why this priority**: Supports safe migrations alongside dual writes; optional.

**Independent Test**: Migrate with `preserveTimestamps: false`, `writeTTLInS: 7776000` and
`writeWritetimestampInuS: 1640998861000000`, then read TTL/WRITETIME of target cells.

**Acceptance Scenarios**:

1. **Given** `preserveTimestamps: false` and `writeTTLInS: 7776000`, **When** the migration
   completes, **Then** every written row has a TTL of 7,776,000 seconds (90 days) at write time.
2. **Given** `preserveTimestamps: false` and `writeWritetimestampInuS: 1640998861000000`,
   **When** the migration completes, **Then** every written cell has WRITETIME
   1640998861000000 (microseconds since the Unix epoch).
3. **Given** only one of the two properties is set, **When** the migration completes, **Then** only
   that one is enforced.
4. **Given** `preserveTimestamps: true` (and the table has regular columns) together with
   `writeTTLInS` and/or `writeWritetimestampInuS`, **When** the migration runs, **Then** the
   preserved per-cell TTL/WRITETIME are written and the forced values are silently ignored (no
   error, no warning).

---

### User Story 8 - Tune parallelism, read size and decimal formatting (Priority: P3)

The operator adjusts `splitCount`, `connections` and `fetchSize` to fit the Spark cluster and
database capacity, chooses a consistency level, and sets `stripTrailingZerosForDecimals: true` to
avoid padded decimals in the target.

**Why this priority**: Performance and fidelity tuning on top of a working migration.

**Independent Test**: Migrate a table containing `decimal` value `3.5` with
`stripTrailingZerosForDecimals: true` and confirm the target holds `3.5`, not `3.5000000000...`.

**Acceptance Scenarios**:

1. **Given** `stripTrailingZerosForDecimals: false`, **When** a decimal `3.5` is migrated, **Then**
   it may be written padded with zeros appropriate to its scale (e.g. `3.5000000000...`).
2. **Given** `stripTrailingZerosForDecimals: true`, **When** a decimal `3.5` is migrated, **Then**
   trailing zeros are stripped before the value is written.
3. **Given** `splitCount: 256` on a cluster of 32 cores, **When** the migration runs, **Then** the
   source is split into 256 units of work, giving finer-grained resume than a smaller count.
4. **Given** `consistencyLevel: EACH_QUORUM` (or any other level the CQL driver knows), **When** the
   migration runs, **Then** it is used; **given** an unknown or lower-case value such as
   `local_quorum`, **Then** the job fails when it starts reading (or writing) with "Invalid
   consistency level '...'. Valid values are: ..." rather than at configuration load.

### Edge Cases

- `consistencyLevel: ONE` or `LOCAL_ONE` on the source can miss data unless the source is fully
  repaired; `LOCAL_QUORUM` is recommended.
- A DNS name that resolves to several addresses counts as a single contact point; operators must
  list distinct per-node names to get failover.
- `preserveTimestamps: true` on a table with non-frozen lists, maps or sets is incompatible unless
  `preserveCollectionTimestamps` is also enabled and the column shape is supported.
- Collection timestamp preservation requires **source quiescence**. On the ScyllaDB 2026.2+
  per-element path, a concurrent write between the base scan and the per-row point reads can
  (a) delete an element, which is then dropped from the migrated row and logged, or (b) add an
  element, which is silently absent and not counted in the dropped-element warning. The
  Cassandra 5.0+ path reads values and metadata in one scan and is not affected.
- TTLs are read as remaining seconds and re-applied on write, so the clock advances between read
  and write/compare; validation must absorb this with `ttlToleranceMillis`.
- A short-lived element can expire on the source between read and append; it is then still written
  to the target with its remaining-TTL-at-read-time and briefly reappears there. Prefer tables
  whose collection TTLs are comfortably longer than the expected job duration.
- CQL exposes no TTL for a row marker: a row inserted `USING TTL` whose non-key columns are all null
  or empty (e.g. an empty collection) is written to the target without a TTL.
- Very large non-frozen collections expand into up to one update per distinct element
  TTL/WRITETIME, which is memory- and throughput-intensive.
- With collection timestamp preservation, the source is cached in executor memory spilling to
  `spark.local.dir`; executor local disk must hold about one batch of source data plus per-element
  metadata. Raise `splitCount` for large tables to keep batches small. The batch size is
  `spark.default.parallelism` splits by default and can be changed with the driver JVM property
  `-Dscylla.migrator.collectionBatchPartitions=<n>`; a non-positive value writes the whole input as
  one batch.
- Validation of per-element collection metadata against ScyllaDB 2026.2+ issues ceil(n / 100)
  synchronous queries per collection per row on each ScyllaDB side and reads values and metadata at
  slightly different times; stop writes first. Against older servers or a ScyllaDB endpoint whose
  primary key includes a frozen tuple, UDT or collection, non-frozen collections are compared by
  value only. `copyMissingRows` restores element TTL/WRITETIME only when the source can read them
  and is rejected otherwise (see `009-migration-validation`).
- Trust/key store files referenced by `trustStorePath`/`keyStorePath` must be shipped to worker
  nodes with `spark-submit --files`; otherwise executors cannot find them.
- An Astra bundle given as an `s3://`/`s3a://` URL works in standard Spark environments, but
  distributing it with `--files` is recommended for reliability (no dependency on Hadoop URL
  handlers or repeated S3 fetches).
- The Astra bundle does not embed authentication credentials; omitting `credentials` leaves the
  connection unauthenticated.
- `writeTTLInS`/`writeWritetimestampInuS` only take effect when timestamps are not preserved. When
  `preserveTimestamps: true` is in effect, the combination is accepted at startup and the forced
  values are silently ignored; if timestamp preservation is disabled because the table has no
  regular columns, the forced values apply.
- Unknown or lower-case `consistencyLevel` values are not rejected when the configuration is loaded;
  the job fails later, when the source read or target write is set up.
- The `where` filter value is shown as `<redacted>` in the logged configuration (like passwords and
  secrets).
- A DSE `solr_query` column in the source is dropped before writing.
- Failed CQL queries are retried without limit by the connector, so an unreachable or overloaded
  cluster stalls the job rather than failing it quickly.
- Primary-key types on the ScyllaDB 2026.2+ per-element path: text/ascii/varchar, integer types,
  boolean, float/double, timestamp, date, time, blob, uuid/timeuuid, inet, varint and decimal are
  supported; any other key column type (e.g. frozen tuple, UDT, collection, duration) is rejected.
- The per-element point reads use chunks of 100 keys by default; the executor JVM property
  `scylla.migrator.subscriptKeyChunkSize` changes it (1–20000; any other value fails the job).
- A per-element TTL outside 0–2147483647 seconds or a missing per-element WRITETIME aborts the
  migration instead of writing a wrong value.
- Spark pads decimals with zeros appropriate to their scale; there is currently no way to preserve
  the original decimal representation exactly, only to strip trailing zeros.

## Requirements *(mandatory)*

### Functional Requirements

**Source/target compatibility**

- **FR-001**: System MUST accept a source of `type: cassandra` and its alias `type: scylla` for
  Apache Cassandra, ScyllaDB and DataStax Astra source tables.
- **FR-002**: System MUST accept a target of `type: cassandra` and its alias `type: scylla` for
  Apache Cassandra, ScyllaDB and DataStax Astra target tables.
- **FR-003**: System MUST only allow a `cassandra` source to be combined with a `cassandra`/`scylla`
  (CQL-compatible) target or a `parquet` target (`011-cql-to-parquet-export`), and MUST stop before
  reading data with "Unsupported combination of source and target: ..." for any other target.
- **FR-004**: System MUST treat every documented field as mandatory unless its documentation
  starts with "Optional", and MUST fail at startup with a `DecodingFailure` naming the missing or
  malformed property when a required field is absent.

**Connection**

- **FR-005**: System MUST accept `host` as a single host name, a YAML list, or a comma-separated
  string of contact points (DNS names or IPs) on both source and target, and MUST be able to
  connect when at least one listed contact point is reachable.
- **FR-005a**: System MUST reject at startup, with a `DecodingFailure`, a `host` whose contact-point
  list is empty, or any contact point that is not a hostname, IPv4 address or (optionally
  bracketed) IPv6 address — including entries with URL metacharacters (`/ ? # & @`) and IPv4
  addresses wrapped in brackets.
- **FR-005b**: System MUST require both `host` and `port` on a source or target that has no `cloud`
  block, failing with "'host' and 'port' are required unless 'cloud' is set."
- **FR-006**: System MUST connect on the configured CQL `port` (documented example: 9042).
- **FR-006a**: System MUST reject at startup a `port` outside 1–65535.
- **FR-007**: System MUST support optional `localDC`, optional `credentials` (`username`,
  `password`) and optional `sslOptions` on both source and target.
- **FR-008**: System MUST honour `sslOptions` fields `enabled`, `clientAuthEnabled`,
  `trustStorePath`, `trustStorePassword`, `trustStoreType` (e.g. `JKS`), `keyStorePath`,
  `keyStorePassword`, `keyStoreType`, `enabledAlgorithms` and `protocol` (e.g. `TLS`), all of which
  except the enabling flags are optional.
- **FR-008a**: System MUST default `trustStoreType` and `keyStoreType` to `JKS`, `protocol` to
  `TLS`, and `enabledAlgorithms` to `TLS_RSA_WITH_AES_128_CBC_SHA` and
  `TLS_RSA_WITH_AES_256_CBC_SHA` when they are omitted from `sslOptions`.
- **FR-009**: System MUST read the source keyspace/table named by `source.keyspace` and
  `source.table` and write to `target.keyspace`/`target.table`.

**Consistency, parallelism and reads**

- **FR-010**: System MUST require `consistencyLevel` on source and target, accept any upper-case
  consistency level name known to the CQL driver (`ANY`, `ONE`, `TWO`, `THREE`, `QUORUM`, `ALL`,
  `LOCAL_ONE`, `LOCAL_QUORUM`, `EACH_QUORUM`, `SERIAL`, `LOCAL_SERIAL`; documented:
  `LOCAL_ONE`, `ONE`, `LOCAL_QUORUM`, `QUORUM`), and MUST use the configured level for bulk-data
  reads/writes (and for the per-element metadata point reads).
- **FR-010a**: System MUST fail, when the source read or target write is set up (not at
  configuration load), with "Invalid consistency level '<value>'. Valid values are: ..." for an
  unknown or wrongly-cased value.
- **FR-011**: System MUST split the source table into `splitCount` units of work (documented
  example 256; guidance: at least the number of Spark cores, ideally about 8 x Spark cores).
  `splitCount` is optional; when omitted, the connector's default split sizing is used.
- **FR-012**: System MUST set the number of connections per executor to the source and target from
  their respective `connections` settings (documented examples: 8 for source, 16 for target).
  `connections` is optional on both; when omitted the connector default applies.
- **FR-013**: System MUST fetch `fetchSize` rows per source read (required; documented example:
  1000).
- **FR-014**: System MUST, when `source.where` is set, migrate only the rows that satisfy that CQL
  condition.

**Timestamps**

- **FR-015**: System MUST, when `preserveTimestamps: true`, write each target cell with the source
  cell's WRITETIME and its remaining TTL as read from the source.
- **FR-016**: System MUST treat frozen collections as single cells that are supported by
  `preserveTimestamps`.
- **FR-017**: System MUST treat `preserveTimestamps: true` as incompatible with non-frozen
  (multi-cell) collection columns unless `preserveCollectionTimestamps: true` is also set and the
  column is supported, and MUST fail before reading data with an error naming the options to change.
- **FR-017a**: System MUST reject, before reading data, `preserveTimestamps: true` on a table with a
  non-frozen (multi-cell) user-defined type column.
- **FR-017b**: System MUST reject, before reading data, `preserveTimestamps: true` on a table that has
  a column named `ttl` or `writetime`, or named `<regular column>_ttl`/`<regular column>_writetime`
  (reserved for timestamp preservation).
- **FR-017c**: System MUST, when `preserveTimestamps: true` and the table has no regular (non-key)
  columns, log "No regular columns in the table - disabling timestamp preservation" and copy rows
  without timestamp preservation.
- **FR-018**: System MUST default `preserveCollectionTimestamps` to `false` and MUST ignore it
  unless `preserveTimestamps` is also `true`, logging a warning that it has no effect in that case.
- **FR-019**: System MUST, when collection timestamps are preserved, read per-element
  TTL/WRITETIME from Cassandra 5.0+ sources or ScyllaDB 2026.2+ sources (auto-detected), and
  re-apply each element's original TTL/WRITETIME on the target after the base row write.
- **FR-019a**: System MUST fail before reading data when collection timestamp preservation is
  requested for a table with non-frozen collections and the source supports neither per-element
  metadata form, naming the column and the required source versions.
- **FR-020**: System MUST support collection timestamp preservation for non-frozen sets and maps
  whose element/key type is text/ascii/varchar or an integer type, and MUST treat non-frozen lists
  and other element/key types as unsupported.
- **FR-021**: System MUST reject, before the migration starts, tables with static non-frozen
  collections when collection timestamp preservation is requested.
- **FR-022**: System MUST reject, before the distributed read starts, ScyllaDB 2026.2+ source tables
  whose primary key includes a column type other than text/ascii/varchar, integer types, boolean,
  float/double, counter, timestamp, date, time, blob, uuid/timeuuid, inet, varint or decimal (e.g. a
  frozen tuple, UDT or collection) when collection timestamp preservation is requested.
- **FR-023**: System MUST restore collection elements by writing the oldest-WRITETIME group as a
  whole-collection assignment (whose tombstone is one microsecond older) and the remaining groups as
  appends, so that target elements older than every source element are removed, target elements
  newer than every source element are kept, and a null/empty source collection leaves the target
  collection untouched.
- **FR-024**: System MUST, when collection timestamps are preserved, read, cache and write the
  source in batches of splits (default: `spark.default.parallelism` splits; overridable with the
  driver property `scylla.migrator.collectionBatchPartitions`, a non-positive value meaning one
  batch), release each batch's cache and save a savepoint before the next batch, and resume an
  interrupted run from the last completed batch.
- **FR-024a**: System MUST fail with an error when the driver property
  `scylla.migrator.collectionBatchPartitions` is set to a non-integer value.
- **FR-025**: System MUST log collection elements that were deleted on the source during the
  migration and therefore dropped from the migrated row (ScyllaDB 2026.2+ path).
- **FR-025a**: System MUST log a warning for a non-frozen collection cell with more than 100,000
  elements and MUST abort the migration for a cell with more than 1,000,000 elements, unless the
  executor JVM property `scylla.migrator.maxCollectionElements` sets another cap (non-positive
  disables it).
- **FR-025b**: System MUST read per-element metadata on the ScyllaDB 2026.2+ path in chunks of 100
  keys per query by default, configurable with the executor JVM property
  `scylla.migrator.subscriptKeyChunkSize` in the range 1–20000 (other values fail the job).
- **FR-025c**: System MUST abort, rather than write a wrong value, when a per-element TTL is outside
  0–2147483647 seconds, a per-element WRITETIME is missing, or element and metadata counts differ.
- **FR-026**: System MUST write a row whose TTL comes only from a row marker (all non-key columns
  null or empty) without a TTL, because CQL exposes no row-marker TTL.
- **FR-027**: System MUST, when `target.writeTTLInS` is set, apply that TTL in seconds to all
  written rows, and when `target.writeWritetimestampInuS` is set, apply that WRITETIME in
  microseconds since the Unix epoch to all written rows; either, both or neither may be set.
- **FR-027a**: System MUST ignore `writeTTLInS` and `writeWritetimestampInuS`, without error or
  warning, whenever timestamp preservation is in effect (`preserveTimestamps: true` on a table with
  regular columns); per-cell preserved values win.

**Target table and values**

- **FR-028**: System MUST NOT create CQL target keyspaces or tables; the target table MUST exist
  before the migration, and a missing table makes the write fail.
- **FR-029**: System MUST require the target table to have the same schema as the source table,
  modulo configured renames (see `007-column-renames`); every source column (after renames) must
  exist in the target table.
- **FR-030**: System MUST, when `stripTrailingZerosForDecimals: true`, strip trailing zeros from
  decimal values before writing them; when `false`, decimals are written as produced by the
  processing engine (possibly zero-padded to their scale). Stripping also applies to decimals
  inside collections and to the per-element collection writes.
- **FR-030a**: System MUST drop a source column named `solr_query` (DSE search pseudo-column) before
  writing to the target.
- **FR-030b**: System MUST, when the optional target property `dropNullPrimaryKeys` is `true`, skip
  rows whose primary-key value is null and log "Dropped <n> rows with null primary key values";
  for a `cassandra` source it defaults to `false` (no filtering).

**DataStax Astra**

- **FR-031**: System MUST accept a `cloud.secureBundlePath` on a `cassandra` source or target and
  connect through the bundle's contact points, local datacenter and TLS material.
- **FR-032**: System MUST reject at startup any source or target that sets `cloud` together with
  `host`, `port`, `localDC` or `sslOptions`, naming the offending property ("'cloud' is mutually
  exclusive with 'host'/'port'", "'localDC' must not be set when using 'cloud'", "'sslOptions' must
  not be set when using 'cloud'").
- **FR-033**: System MUST take authentication for Astra from `credentials` (client id as
  `username`, client secret as `password`), since the bundle does not carry credentials; omitting
  `credentials` is not a configuration error (the connection is attempted without authentication).
- **FR-034**: System MUST accept `secureBundlePath` as an absolute filesystem path (converted to a
  `file://` URL at runtime), an `https://` URL, a bare filename of a file distributed with
  `spark-submit --files`, or an `s3://`/`s3a://` URL, and the referenced bundle MUST be readable
  from the driver and every executor that opens a CQL session. A `file://` URL is also accepted.
- **FR-034a**: System MUST reject at startup a `secureBundlePath` that is empty, uses plain
  `http://`, carries URL user-info or a query string, or is a relative path other than a bare
  filename (letters, digits, `.`, `_`, `-`).

**Resume**

- **FR-035**: System MUST support savepoints and automatic resume for `cassandra` sources by
  tracking completed token ranges (behaviour specified in `008-savepoints-and-resume`), and MUST
  fail at startup with "Missing required field: savepoints ..." when a `cassandra` source
  configuration has no `savepoints` section.

**Operations**

- **FR-036**: System MUST log the loaded configuration with credentials, passwords, secrets and the
  `where` value replaced by `<redacted>`, and MUST log the consistency level used for the source
  and the target.
- **FR-037**: System MUST retry failed CQL queries to the source and target without a retry limit.

### Key Entities

- **CQL Source configuration** (`source`, `type: cassandra`): host/contact points, port,
  credentials, localDC, keyspace, table, consistencyLevel, preserveTimestamps,
  preserveCollectionTimestamps, splitCount, connections, fetchSize, sslOptions, where, or
  alternatively `cloud`.
- **CQL Target configuration** (`target`, `type: cassandra`/`scylla`): host/contact points, port,
  keyspace, localDC, credentials, table, consistencyLevel, connections,
  stripTrailingZerosForDecimals, writeTTLInS, writeWritetimestampInuS, dropNullPrimaryKeys,
  sslOptions, or alternatively `cloud`.
- **Cloud configuration** (`cloud`): `secureBundlePath` to a DataStax Astra secure-connect bundle
  (zip) carrying contact points, local datacenter and TLS material, mutually exclusive with
  `host`/`port`/`localDC`/`sslOptions`.
- **SSL options** (`sslOptions`): TLS enablement, client authentication, trust store and key store
  locations/passwords/types, enabled cipher suites, protocol.
- **Cell metadata**: per-cell TTL (remaining seconds) and WRITETIME (microseconds); for non-frozen
  collections, per-element TTL and WRITETIME.
- **Split / token range**: unit of parallel work over the source table; also the unit of resume
  progress.
- **Collection batch**: a group of source splits read, cached and written together when collection
  timestamps are preserved; progress is saved after each batch.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a completed migration of a quiesced source, 100% of source rows (or of rows
  matching `where`) are present and equal in the target, as confirmed by the Validator.
- **SC-002**: With `preserveTimestamps: true`, 100% of compared cells have equal WRITETIME and TTL
  differences within the configured `ttlToleranceMillis`, as reported by the Validator with
  `compareTimestamps: true`.
- **SC-003**: With `preserveCollectionTimestamps: true` on a supported, quiesced source, 100% of
  non-frozen set/map elements carry their source TTL/WRITETIME within tolerance.
- **SC-004**: 100% of configurations that combine `cloud` with `host`, `port`, `localDC` or
  `sslOptions`, that have an invalid host/port/bundle path, or that request timestamp or collection
  timestamp preservation on a rejected table shape, fail before any data is read or written.
- **SC-005**: A migration succeeds while any single listed contact point is unavailable at startup,
  provided at least one other listed contact point is reachable.
- **SC-006**: With `stripTrailingZerosForDecimals: true`, 0 migrated decimal values carry trailing
  zeros that were not present in the source value.
- **SC-007**: An interrupted collection-timestamp migration that is re-run with the same
  configuration re-writes at most the batch that was in progress when it was interrupted.

## Assumptions

- Operators stop writes to the source before migrating when using
  `preserveCollectionTimestamps` (required), and before validating against ScyllaDB.
- Operators provide a running Spark cluster and submit the job as described in
  `010-spark-cluster-and-job-submission`; trust/key stores and `--files`-distributed bundles are
  the operator's responsibility to supply.
- Operators who use `consistencyLevel: ONE`/`LOCAL_ONE` on the source ensure it is fully repaired.
- The source `type` is documented only as `cassandra`, but the code also accepts `scylla` for
  sources (and both spellings for targets), as the constitution requires.
- Operators create the target keyspace and table before the migration; the "created on the fly"
  sentence in the docs does not apply to CQL targets.
- The Cassandra page of the docs states more briefly that `preserveTimestamps` is incompatible with
  collections; this spec follows the more detailed configuration reference (frozen collections
  supported, non-frozen collections only with `preserveCollectionTimestamps`).
- Column renames (`007-column-renames`), savepoints (`008-savepoints-and-resume`) and validation
  (`009-migration-validation`) are specified separately and only referenced here.
- Writes are idempotent on the target primary key, so re-running a migration only costs extra load.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Missing CQL target table | `configuration.rst` (CQL target `table`) and `migrate-from-cassandra-or-parquet.rst:154`: "If it does not exist, it will be created on the fly." | The CQL writer only saves rows into an existing table; no code path creates a CQL keyspace or table (only the savepoints table, `SavepointStore.scala:296`). `writers/Scylla.scala:346`; integration tests pre-create the target table (`tests/src/test/scala/com/scylladb/migrator/scylla/MigratorSuite.scala:84-90`). | Code (FR-028, US1-2) | Replace with "The table must already exist with the same schema as the source (after renames)." |
| 2 | `writeTTLInS`/`writeWritetimestampInuS` with `preserveTimestamps: true` | Only "If we do not persist timestamps ... we can enforce ..."; combination unspecified. | Preserved per-cell TTL/WRITETIME are used; forced values silently ignored, no warning (`writers/Scylla.scala:227-246`). | Code (FR-027a, US7-4) | State that the properties are ignored when timestamps are preserved (or add a startup warning). |
| 3 | `scylla` source alias | Source type list names only `cassandra` (`configuration.rst` "Source"). | `cassandra` and `scylla` both accepted for sources (`config/SourceSettings.scala:750`) and targets (`config/TargetSettings.scala:233`); test fixture `scylla-to-scylla-basic.yaml` uses `type: scylla` as source. | Code (FR-001) | Add `scylla` as an alias in the source and target type lists. |
| 4 | Allowed targets for a `cassandra` source | "can be used together with a target of type `cassandra` only" (`configuration.rst` CQL source). | Also `parquet` target (`Migrator.scala:96`); other combinations fail after config load with "Unsupported combination of source and target" (`Migrator.scala:110-114`), not with a `DecodingFailure`. | Code (FR-003, US1-9) | List `parquet` as an allowed target and describe the error. |
| 5 | Consistency levels | "Options are: LOCAL_ONE, ONE, LOCAL_QUORUM, QUORUM." | Any driver level name accepted, case-sensitive; invalid value fails only when the read/write is set up, listing valid values (`ConsistencyLevelUtils.scala:11-19`, `readers/Cassandra.scala:1827`, `writers/Scylla.scala:217`). | Code (FR-010, FR-010a) | Mention that all CQL levels are accepted, upper-case only, and when an invalid value is detected. |
| 6 | Optional `splitCount`/`connections` | Shown without "Optional" (i.e. mandatory per the docs' convention). | Both optional on source; `connections` optional on target (`config/SourceSettings.scala:141-142`, `config/TargetSettings.scala:78`); connector defaults apply. | Code (FR-011, FR-012) | Mark them "Optional" and state the default. |
| 7 | `sslOptions` defaults | Only "all below are optional". | Defaults `JKS`/`JKS`/`TLS` and two CBC cipher suites (`config/SSLOptions.scala:38-48`, `Connectors.scala:96-105`). | Code (FR-008a) | Document the defaults (and that GCM suites need `enabledAlgorithms`). |
| 8 | Host/port validation | Not documented. | Empty contact list, URL metacharacters, bracketed IPv4, invalid IPv6 and ports outside 1–65535 rejected at startup (`config/ContactPoints.scala:20-24`, `config/HostValidation.scala:57-75`); host+port required without `cloud` (`config/SourceSettings.scala:125-131`, `config/TargetSettings.scala:62-67`). | Code (FR-005a, FR-005b, FR-006a) | Add a short "validation" note. |
| 9 | `secureBundlePath` validation | Lists the accepted forms only. | Also rejects empty, `http://`, user-info, query strings and relative paths with directories; accepts `file://` (`config/CloudConfig.scala:43-96`). | Code (FR-034, FR-034a) | List rejected forms and `file://`. |
| 10 | `dropNullPrimaryKeys` target property | Not documented. | Optional; defaults to `false` for CQL/DynamoDB sources and `true` otherwise; drops null-key rows and logs a count (`config/TargetSettings.scala:26,83`, `writers/Scylla.scala:97-107,310-371`). | Code (FR-030b) | Document the property. |
| 11 | Extra `preserveTimestamps` rejections | Only collections are mentioned. | Rejects non-frozen UDT columns and reserved column names (`ttl`, `writetime`, `<col>_ttl`, `<col>_writetime`); disables preservation with a warning on key-only tables (`readers/Cassandra.scala:940-1004`). | Code (FR-017a–c) | Document these cases. |
| 12 | Collection-preservation limits and tuning | Only `collectionBatchPartitions` is documented. | Warn >100,000 / abort >1,000,000 elements per cell (`scylla.migrator.maxCollectionElements`), point-read chunk size `scylla.migrator.subscriptKeyChunkSize` (default 100, 1–20000) (`readers/Cassandra.scala:64-104,140-158`); invalid `collectionBatchPartitions` fails (`readers/CollectionBatching.scala:15-28`). | Code (FR-024a, FR-025a, FR-025b) | Document the properties and limits. |
| 13 | Unsupported metadata source | "Requires a source that can read collection-element metadata". | Fails fast with an explicit error naming the column and versions (`readers/Cassandra.scala:219-248`); `preserveCollectionTimestamps` without `preserveTimestamps` logs a warning (`readers/Cassandra.scala:952-957`). | Code (FR-018, FR-019a) | Mention the error and the warning. |
| 14 | Operational behaviour | Not documented. | Unlimited CQL query retries (`Connectors.scala:65,78`); DSE `solr_query` column dropped (`writers/Scylla.scala:119-130`); `where` redacted in logged config (`config/MigratorConfig.scala:171-174`). | Code (FR-030a, FR-036, FR-037) | Add a short operations note. |

Verified unchanged: contact-point list/comma forms, `localDC`/`credentials`/`sslOptions` fields,
`where` filtering, `fetchSize`, frozen-collection preservation, `preserveCollectionTimestamps`
default `false`, set/map key-type support and list rejection, static-collection rejection,
oldest-group overwrite plus append restore, batching via `collectionBatchPartitions` with
per-batch savepoints, `stripTrailingZerosForDecimals`, Astra `cloud` mutual exclusion and bundle
path forms, and savepoint-based resume all match the documentation.
