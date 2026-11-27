# Feature Specification: Aerospike to CQL Migration

**Feature Branch**: `005-aerospike-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/configuration.rst` — "Source" (source type list) and "Aerospike Source" (all subsections: "Schema and record identity", "Target schema and invalid records", "Metadata and restarts", "Authentication and TLS"); `docs/source/configuration.rst` — "Savepoints" (resumable sources); `docs/source/resume-interrupted-migration.rst` (resumable sources note); source code at commit 06835d0 (`config/SourceSettings.scala` (Aerospike decoder), `config/Aerospike*.scala`, `config/MigratorConfig.scala`, `readers/Aerospike.scala`, `readers/AerospikeHosts.scala`, `readers/AerospikeRDD.scala`, `readers/AerospikeTypes.scala`, `writers/AerospikeSchemaValidation.scala`, `writers/Scylla.scala`, `Migrator.scala`, `Validator.scala`, `build.sbt`).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Migrate an Aerospike set into a pre-created ScyllaDB table (Priority: P1)

A database operator wants to copy every record of one Aerospike namespace/set into an existing
CQL table in ScyllaDB (or Apache Cassandra). They declare which bins to copy and their types,
choose how the record key is represented, and run the migration on a Spark cluster.

**Why this priority**: This is the core value of the feature: without it, no Aerospike data can
be moved at all.

**Independent Test**: Load records with stored string keys (`sendKey=true`) into an Aerospike
set, create the matching CQL table (`aero_key text PRIMARY KEY`, one column per declared bin),
run the migrator with `source.type: aerospike`, `keyMode: string` and a `schema` listing the
bins, then confirm every source record is present in the target with identical values.

**Acceptance Scenarios**:

1. **Given** a source config with `hosts: [aerospike.example.com]`, `namespace: test`,
   `set: users`, `keyMode: string` and `schema: {name: string, age: long, tags: list<string>,
   attrs: map<string,long>}`, and a target table `migration.users` with columns `aero_key text
   PRIMARY KEY, name text, age bigint, tags frozen<list<text>>, attrs frozen<map<text,bigint>>`,
   **When** the migration runs, **Then** each record is written as one row keyed by its
   original string key with the four declared bins.
2. **Given** a seed host listed without an inline port and no `port` setting, **When** the
   migrator connects, **Then** it uses port 3000.
3. **Given** a record that has a bin not declared in `schema`, **When** it is migrated,
   **Then** that bin is omitted; **and given** a record missing a declared bin, **Then** the
   corresponding column is null.
4. **Given** `schema: {}`, **When** the migration runs, **Then** only the generated key column
   (and any enabled metadata columns) are written.
5. **Given** a configuration missing any of `hosts`, `namespace`, `set`, `schema` or `keyMode`,
   or with an empty `namespace` or `set`, **When** the migrator starts, **Then** it fails
   before reading any data.
6. **Given** `source.type: aerospike` with a `parquet`, `dynamodb` or `alternator` target,
   **When** the migrator starts, **Then** it fails with an "Unsupported combination of source
   and target" error before reading any data.
7. **Given** a valid Aerospike-to-ScyllaDB configuration, **When** the Validator is run with it,
   **Then** it fails with an "Unsupported combination of source and target" error (no
   Aerospike validation exists).

---

### User Story 2 - Choose the record-key representation (Priority: P1)

The operator needs the generated key column to match how the source records were written:
stored string, integer or binary keys, or — when keys were not stored or are of mixed types —
the record digest.

**Why this priority**: The key is part of the target primary key; a wrong key mode makes the
migration fail or skip records.

**Independent Test**: Run one migration per `keyMode` against suitable source data and target
key column types, and check the generated key values and the skipped-record behaviour.

**Acceptance Scenarios**:

1. **Given** `keyMode: integer` and target key column `bigint`, **When** records written with
   `sendKey=true` and integer keys are migrated, **Then** the key column holds the original
   integer keys.
2. **Given** `keyMode: binary` and target key column `blob`, **When** records with stored binary
   keys are migrated, **Then** the key column holds the original bytes.
3. **Given** `keyMode: digest` and target key column `text`, **When** records with mixed key
   types or no stored keys are migrated, **Then** every record is written with its lowercase
   40-character hex digest as key.
4. **Given** a stored-key mode (`string`, `integer` or `binary`), **When** a record has no
   stored key or a key of another type, **Then** the record is skipped regardless of
   `invalidRecordHandling`, with no coercion and no digest fallback.
5. **Given** `keyMode: string` and a target key column of type `bigint`, **When** the migration
   starts, **Then** the job fails because the generated key does not have the required exact
   CQL type.

---

### User Story 3 - Control what happens to invalid values and oversized lists (Priority: P2)

The operator wants to decide whether a record with an unconvertible value is dropped entirely
or written with that field nulled, and how rows whose non-frozen lists exceed the per-row
element budget are handled.

**Why this priority**: Real data is rarely perfectly clean; explicit policies keep the
migration lossless-by-default while letting operators trade completeness for coverage.

**Independent Test**: Seed records with an out-of-range number, a null collection member and
a non-frozen list exceeding 131,072 elements; run with each policy value and check the target.

**Acceptance Scenarios**:

1. **Given** `invalidRecordHandling` is omitted (default `skip`), **When** a declared value is
   invalid (e.g. numeric overflow), **Then** the entire record is omitted.
2. **Given** `invalidRecordHandling: "null"`, **When** a top-level field is invalid, **Then**
   the record is written with the valid fields and that field set to null where the target
   permits it; a bad collection member nulls the whole collection field.
3. **Given** an unquoted `invalidRecordHandling: null`, **When** the migrator starts, **Then**
   the configuration is rejected as an invalid policy.
4. **Given** a `double` bin with value 0.1 mapped to a CQL `decimal` column, **When** it is
   converted, **Then** the value is treated as invalid; **given** 0.5, **Then** it is written.
5. **Given** non-frozen list columns whose combined outer elements exceed 131,072 in one row,
   **When** `listOverflowHandling` is `skip` (default) / `nullify` / `empty`, **Then**
   respectively the record is omitted / every contributing list is written as null / every
   contributing list is written as empty, and other surviving fields are preserved.

---

### User Story 4 - Export TTL and generation as columns (Priority: P3)

The operator wants to keep Aerospike's remaining TTL and generation counter as ordinary columns
for auditing or application use.

**Why this priority**: Useful metadata, but not needed for a basic data copy.

**Independent Test**: Enable `preserveTTL` and `preserveGeneration`, add `aero_ttl int` and
`aero_generation int` columns to the target, migrate, and compare with source metadata.

**Acceptance Scenarios**:

1. **Given** `preserveTTL: true`, **When** a record with an expiration is migrated, **Then**
   `aero_ttl` holds its remaining seconds at read time; for a record that never expires it
   holds -1, and for a record that expired before it was converted it holds 0.
2. **Given** `preserveGeneration: true`, **When** a record is migrated, **Then**
   `aero_generation` holds its generation counter.
3. **Given** `preserveTTL: true`, **When** the row is written, **Then** no CQL write TTL is
   applied to the target row (the value is stored as data only).
4. **Given** a bin named `aero_ttl` and `preserveTTL: true`, **When** the operator sets
   `ttlColumn: source_ttl`, **Then** the generated column is named `source_ttl` and the bin
   keeps its name.

---

### User Story 5 - Authenticate and encrypt the Aerospike connection (Priority: P2)

The operator must connect to a secured Aerospike cluster with credentials kept out of the
configuration file and, optionally, with TLS.

**Why this priority**: Production clusters are usually secured; without this, the feature is
unusable there.

**Independent Test**: Run against an Aerospike cluster requiring authentication and TLS, once
with environment credentials and once with `source.credentials`, and verify the connection
and the failure modes below.

**Acceptance Scenarios**:

1. **Given** `AEROSPIKE_USERNAME` and `AEROSPIKE_PASSWORD` set to nonempty values on the driver
   and every executor, **When** `source.credentials` is also configured, **Then** the
   environment credentials are used and configured credentials are not broadcast to executors.
2. **Given** only one of the two variables is set, or one is empty, **When** the migrator
   starts, **Then** it fails before connecting.
3. **Given** neither variable is set on the driver and `source.credentials` is configured,
   **When** the migration runs, **Then** configured credentials are sent to executors via a
   Spark broadcast.
4. **Given** `source.tlsName: aerospike-tls`, **When** the migrator connects, **Then** TLS is
   used for every seed with that verification name, overriding inline seed TLS names.
5. **Given** a seed string with an inline TLS name and no `source.tlsName`, **When** the
   migrator starts, **Then** configuration validation fails before connecting.

---

### Edge Cases

- The driver or any executor runs a JDK older than 17: unsupported; JDK 17 or later is required
  on all of them.
- Target keyspace or table does not exist: the operator must create both before migrating.
- `schemaSampleSize`, `schemaDiscoveryStrategy`, a separate `bins` filter, or relying on
  automatic schema inference: not supported; `schema` must be declared explicitly. Unknown
  properties such as these are silently ignored by the configuration parser (no error), so a
  configuration that sets them still runs with the declared `schema` only.
- Integer-backed boolean flags declared as `boolean`/`bool`: only native Boolean bins are
  valid; such flags must be declared as `long` or `string`.
- A `string` schema for binary, collection or GeoJSON values yields hex, Java-style collection
  punctuation, or JSON text respectively; collection text is not guaranteed parseable or
  round-trippable.
- Generated column name collides with a bin name (compared ignoring case): the configuration
  is rejected at load time; use `keyColumn`, `ttlColumn` or `generationColumn`, disable the
  metadata column, or drop the bin from `schema` — top-level `renames` cannot resolve it;
  disabled metadata columns do not reserve their names.
- Two declared bins whose names differ only by case, a blank bin name, a blank
  `keyColumn`/`ttlColumn`/`generationColumn`, or two enabled generated columns with the same name
  ignoring case: rejected at configuration load.
- An unknown schema type name (e.g. `float`) or a malformed `map<...>` type: rejected at
  configuration load. Type names are case-insensitive.
- An emitted column missing from the target table, or an incompatible source/target type
  mapping: the job fails.
- Target has other primary-key columns not supplied (or null) for a record: the record is
  skipped in either invalid-record mode.
- Writing static columns while the generated key is only a clustering column: invalid; the key
  must be in the partition key so records cannot overwrite each other's static values.
- Native list mapped to a non-list CQL collection (e.g. set): invalid; lists require CQL lists
  to keep order and duplicates. Nested CQL collections that are not frozen: invalid.
- Null collection members, map-key collisions after conversion, or an empty collection bound
  to a non-frozen target collection: invalid field values (use frozen collections to preserve
  empty collections).
- When the target ignores null writes, a nulled field (via `"null"` policy or `nullify`) can
  leave an older destination value in place; `empty` bindings clear the destination column
  even when nulls are ignored.
- Frozen lists and nested frozen contents do not count toward the 131,072-element budget; the
  budget is per row and resets for each row.
- Configuration errors, connection errors, exhausted scan retries and write errors are fatal
  regardless of the invalid-record policy.
- Scan retries can replay records (at-least-once); a failed job can leave partial data, and a
  restart rescans from the beginning without rollback.
- A `savepoints` section is configured: its settings are ignored for this source (no savepoint
  is written or resumed, and a warning is logged), but the section must still be structurally
  valid — e.g. specifying both `path` and `target`, or a file `path` without `intervalSeconds`,
  still fails configuration loading.
- A declared bin is absent from a record: the column is null and a warning is logged once per
  Spark partition, with a per-partition summary (count and up to 5 example names) at
  completion.
- A scan that receives no records for `pollTimeoutSeconds` × `maxPollRetries` consecutive idle
  polls (default 120 s × 5 = 10 minutes) fails the partition with a timeout error; scan retries
  reset this budget.
- Transient scan errors (timeout, server not available, partition unavailable, device overload)
  are retried up to `maxScanRetries` times per partition (default 3); other errors fail
  immediately.
- Custom trust store needed: must be supplied with `-Djavax.net.ssl.trustStore` on both driver
  and executors; per-host TLS activation and distinct per-seed verification names are not
  supported.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept `source.type: aerospike` and allow it only with a CQL-compatible
  target (`scylla`/`cassandra`); any other target type MUST fail at startup with an
  "Unsupported combination of source and target" error before data is read.
- **FR-002**: System MUST require `hosts` (non-empty list of seed hosts; each entry nonblank and
  in Aerospike seed syntax `host[:tlsName][:port]`, possibly comma-separated), `namespace`,
  `set`, `schema` and `keyMode`, and MUST reject an empty `namespace` or `set` at startup before
  reading any data.
- **FR-003**: System MUST use port 3000 for any seed without an inline port when `port` is not
  set, and the configured `port` otherwise; ports outside 1–65535 MUST be rejected.
- **FR-004**: System MUST migrate only bins declared in `schema`, omit undeclared bins, and
  write null for declared bins missing from a record; `schema: {}` MUST emit only the generated
  key and enabled metadata columns.
- **FR-005**: System MUST NOT perform automatic schema inference and MUST NOT support
  `schemaSampleSize`, `schemaDiscoveryStrategy` or a separate `bins` filter; such properties
  are ignored without error.
- **FR-006**: System MUST support schema types `string`/`text`, `long`/`bigint`/`int`/`integer`
  (all producing 64-bit integer values), `double`, `boolean`/`bool` (native Boolean bins only),
  `binary`/`blob`, and nested `list<T>` and `map<K,V>`; type names MUST be matched
  case-insensitively and an unknown type MUST be rejected at configuration load. Bin names MUST
  be nonblank and distinct ignoring case.
- **FR-007**: When a bin is declared as `string`, System MUST render binary values as hex,
  collections with Java-style punctuation, and GeoJSON as JSON text.
- **FR-008**: System MUST support `keyMode` values `string`, `integer`, `binary` and `digest`,
  requiring respective target key types `text`/`varchar`, `bigint`, `blob`, `text`/`varchar`.
- **FR-009**: In stored-key modes, System MUST skip any record whose stored key is missing or of
  a different type, in both invalid-record modes, without coercion or digest fallback.
- **FR-010**: In `digest` mode, System MUST emit the lowercase 40-character hex digest for every
  record, including records with mixed or absent stored keys.
- **FR-011**: System MUST name the generated key column `aero_key` by default and the metadata
  columns `aero_ttl` and `aero_generation`, and MUST allow overriding them with `keyColumn`,
  `ttlColumn` and `generationColumn`; only enabled generated columns reserve their names. A
  declared bin whose name equals (ignoring case) an enabled generated column name, a blank
  generated column name, or duplicate enabled generated column names MUST be rejected at
  configuration load.
- **FR-012**: System MUST allow top-level `renames` to map emitted columns to destination names
  (see `007-column-renames`).
- **FR-013**: System MUST fail the job (before writing data) when any emitted column is absent from the target table,
  when the generated key is not part of the target primary key with the exact required type, or
  when the source/target schema mapping is otherwise incompatible.
- **FR-014**: System MUST require, when static columns are written, that the generated key be
  part of the target partition key.
- **FR-015**: System MUST require native lists to map to CQL lists, maps to compatible CQL maps,
  and nested CQL collections to be frozen.
- **FR-016**: System MUST treat null collection members, converted map-key collisions, and empty
  collections bound to non-frozen target collections as invalid field values.
- **FR-017**: System MUST support `invalidRecordHandling` values `skip` (default; omit the whole
  record) and `"null"` (write valid fields, set each invalid top-level field to null where the
  destination permits it); a bad collection member MUST invalidate the whole field, and an
  unquoted YAML `null` MUST be rejected as a policy.
- **FR-018**: System MUST treat numeric overflow, rounding and truncation, including at the
  target CQL type boundary, as invalid values; double-to-decimal conversion MUST preserve the
  exact mathematical value of the source double or be invalid.
- **FR-019**: System MUST skip the whole record, in either invalid-record mode, when the record
  key is missing/invalid or any target primary-key value is invalid or null.
- **FR-020**: System MUST treat configuration errors, connection errors, exhausted scan retries
  and write errors as fatal.
- **FR-021**: System MUST support `listOverflowHandling` values `skip` (default), `nullify` and
  `empty`, applied after ordinary validation when retained top-level non-frozen lists exceed
  131,072 outer elements combined in one row INSERT; frozen lists and nested frozen contents
  MUST NOT count, and the budget MUST reset per row.
- **FR-022**: Under `nullify`/`empty`, System MUST replace every contributing list with null /
  empty, preserve other surviving fields, and leave originally null or already nulled fields
  null.
- **FR-023**: System MUST default `preserveTTL` and `preserveGeneration` to false; when enabled
  they MUST add the TTL column (remaining seconds at read time, -1 for no expiration, 0 for a
  record already expired when converted) and the
  generation column respectively, as ordinary values that do not set target write TTLs or
  write timestamps.
- **FR-024**: System MUST read with at-least-once semantics and MUST NOT offer durable resume;
  the `savepoints` section MUST be optional, a configured `savepoints` section's settings MUST be
  ignored (no savepoints written, no automatic resume, value checks such as positive
  `intervalSeconds` skipped) while the section itself must still parse, System MUST log the
  warning "Source does not support savepoints; any configured savepoints settings are ignored"
  at startup, and a restart MUST scan from the beginning.
- **FR-025**: System MUST use `AEROSPIKE_USERNAME`/`AEROSPIKE_PASSWORD` when both are present and
  nonempty, taking precedence over `source.credentials`; an empty value or a partial pair MUST
  fail before connecting; configured credentials MUST be used only when both variables are
  absent.
- **FR-026**: When the driver selects environment credentials, System MUST NOT broadcast
  configured credentials (each executor supplies its own pair); when both variables are absent
  on the driver, System MUST send configured credentials to executors via a Spark broadcast.
- **FR-027**: System MUST enable TLS only when `source.tlsName` is set, apply that name to every
  seed overriding inline seed TLS names, and reject at configuration validation a seed with an
  inline TLS name when `source.tlsName` is absent.
- **FR-028**: System MUST use the JVM default trust store for TLS, overridable with
  `-Djavax.net.ssl.trustStore` on driver and executors.
- **FR-029**: System MUST require JDK 17 or later on the driver and every executor.
- **FR-030**: System MUST accept optional read-tuning properties, each rejected at configuration
  load when not positive: `splitCount` (number of Spark partitions the 4,096 Aerospike data
  partitions are divided into; default min(4096, 4 × Spark default parallelism); values above
  4096 rejected before reading), `queueSize` (per-partition record buffer, default 1024),
  `pollTimeoutSeconds` (default 120), `maxPollRetries` (consecutive idle polls before failing a
  partition, default 5), `connectTimeoutMs` (default: Aerospike client default),
  `socketTimeoutMs` (default 10000), `totalTimeoutMs` (default 3 × `socketTimeoutMs` when that
  is set, otherwise 30000), `maxConnsPerNode` and `connPoolsPerNode` (default: Aerospike client
  defaults); `maxScanRetries` (default 3) MUST be non-negative.
- **FR-031**: System MUST retry a partition scan after a timeout, server-not-available,
  partition-unavailable or device-overload error, up to `maxScanRetries` times, discarding
  records of the failed attempt; exhausted retries and other scan errors MUST fail the job.
- **FR-032**: System MUST log, at the end of a successful migration, the number of source records
  read, invalid and skipped (as a warning when any were invalid or skipped), and on failure an
  "INCOMPLETE" best-effort report of the same counts; counts can include replays.
- **FR-033**: System MUST reject at configuration load an unknown `keyMode`,
  `invalidRecordHandling` or `listOverflowHandling` value, listing the valid values.
- **FR-034**: System MUST log a warning when configured credentials are broadcast to executors
  and when environment credentials take precedence over configured ones.
- **FR-035**: The Validator MUST NOT support an Aerospike source: running it with
  `source.type: aerospike` MUST fail with an "Unsupported combination of source and target"
  error. (This gap is not yet documented; see Docs vs Code Review.)
- **FR-036**: System MUST reject (before writing) an emitted column, after `renames`, whose name
  is reserved by the CQL connector, and any two emitted columns whose names collide ignoring
  case.

### Key Entities

- **Aerospike source configuration**: `type: aerospike`, `hosts`, `port` (default 3000),
  `namespace`, `set`, `schema`, `keyMode`, optional `keyColumn`, `ttlColumn`,
  `generationColumn`, `invalidRecordHandling`, `listOverflowHandling`, `preserveTTL`,
  `preserveGeneration`, `credentials`, `tlsName`, plus read tuning `splitCount`, `queueSize`,
  `pollTimeoutSeconds`, `maxPollRetries`, `maxScanRetries`, `connectTimeoutMs`,
  `socketTimeoutMs`, `totalTimeoutMs`, `maxConnsPerNode`, `connPoolsPerNode`.
- **Bin schema**: mapping of bin name to supported type (with aliases and nested list/map).
- **Key mode**: rule producing the generated key column value and its required CQL type.
- **Generated columns**: record key, TTL, generation — reserved names, renamable.
- **Invalid-record policy** and **list-overflow policy**: per-record handling choices.
- **Target CQL table**: pre-created table whose primary key contains the generated key.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For a clean source set with stored keys, 100% of records are present in the target
  with values equal to the source, verified by row count and per-row comparison.
- **SC-002**: 100% of invalid configurations listed in FR-002, FR-025 and FR-027 are rejected
  before any connection or data write.
- **SC-003**: Under default policies, 0 rows are written containing a lossy numeric conversion,
  and every skipped record is attributable to a documented rule.
- **SC-004**: With `keyMode: digest`, 100% of records (including those without stored keys) are
  migrated with a 40-character lowercase hex key.
- **SC-005**: Rerunning a failed migration to completion yields a target identical to a single
  successful run (no duplicate or divergent rows), given an idempotent target table.
- **SC-006**: No Aerospike credential value appears in logs or in configuration broadcast when
  environment credentials are used.

## Assumptions

- The operator creates the destination keyspace and table, with a schema compatible with the
  declared bins, key mode and enabled metadata columns, before starting.
- Records intended for stored-key modes were written with the Aerospike writer setting
  `sendKey=true`; this is not a migration option.
- The target tolerates repeated writes (upsert semantics) because scans are at-least-once and
  restarts rescan from the beginning.
- Target-side settings such as `consistencyLevel`, `connections` and null-write behaviour are
  governed by the CQL target configuration (see `001-cql-to-cql-migration`).
- Spark cluster setup and job submission are covered by `010-spark-cluster-and-job-submission`;
  savepoints for other sources by `008-savepoints-and-resume`.
- The Validator does not support Aerospike-to-CQL (FR-035); per constitution principle I this is
  a gap that must be documented (see `009-migration-validation` and the Docs vs Code Review
  below).
- Preserving Aerospike TTL as a CQL write TTL, write timestamps or conflict resolution is out of
  scope.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Validator support | Nothing (`validate.rst`/"Aerospike Source" silent) | Validator dispatch has no Aerospike case and fails with "Unsupported combination of source and target" (`Validator.scala:144-156`) | Code (FR-035) | State in `validate.rst` and "Aerospike Source" that Aerospike migrations cannot be validated (constitution I gap). |
| 2 | Read-tuning properties | Not documented | `splitCount`, `queueSize`, `pollTimeoutSeconds`, `maxPollRetries`, `maxScanRetries`, `connectTimeoutMs`, `socketTimeoutMs`, `totalTimeoutMs`, `maxConnsPerNode`, `connPoolsPerNode` accepted and validated (`config/SourceSettings.scala:547-572,1013-1045`; defaults `readers/Aerospike.scala:91-93,154-170,266-297`) | Code (FR-030) | Add an "Optional tuning" table with defaults and validation rules. |
| 3 | Configured `savepoints` | "Omit `savepoints`; configured savepoints are ignored" | Settings ignored and a warning logged (`Migrator.scala:67-73`; `config/MigratorConfig.scala:64-71`), but the section is still parsed, so structural errors (both `path` and `target`, missing `intervalSeconds` for file targets) still fail (`config/Savepoints.scala:264-320`) | Code (FR-024) | Mention that an included `savepoints` section must still be syntactically valid, and the startup warning. |
| 4 | Unsupported props (`schemaSampleSize`, `schemaDiscoveryStrategy`, `bins`) | "not supported" | Silently ignored by the non-strict decoder (`config/SourceSettings.scala:1013`) | Code (FR-005) | Say they are ignored (no error), so users don't assume a filter is applied. |
| 5 | TTL value for expired records | -1 = no expiration only | 0 = already expired at conversion (`readers/AerospikeRDD.scala:225-234`; `config/SourceSettings.scala:528-532`) | Code (FR-023) | Document the 0 sentinel. |
| 6 | Bin/generated-name collisions | "use `keyColumn`… to avoid collisions" | Collisions (case-insensitive), duplicate bins ignoring case, blank names, unknown types are rejected at config load; `renames` cannot fix a collision (`readers/Aerospike.scala:182-264`; `readers/AerospikeTypes.scala:249-267`) | Code (FR-006, FR-011) | State the case-insensitive rules and that renames don't help. |
| 7 | Unsupported targets | Not stated beyond "CQL target" | Non-Scylla targets fail at dispatch with "Unsupported combination" (`Migrator.scala:110-114`) | Code (FR-001) | List supported target types explicitly. |
| 8 | Scan retries / poll timeout / metrics logs | "exhausted scan retries are fatal" only | Retryable result codes, idle-poll timeout failure, missing-bin warnings, final read/invalid/skipped report (`readers/Aerospike.scala:28-60,411-416`; `readers/AerospikeRDD.scala:170-190,360-380,465-520`) | Code (FR-031, FR-032, edge cases) | Add a short "Monitoring and retries" note. |
| 9 | Credential warnings | Not mentioned | Warnings logged when broadcasting configured credentials or when env credentials override (`readers/Aerospike.scala:105-121,325-328`) | Code (FR-034) | Optional: mention the warnings. |
| 10 | Empty `namespace`/`set` | "rejected" (implied at config validation) | Passes config decoding; rejected when the reader starts, still before reading (`readers/Aerospike.scala:72-77`) | Code (FR-002 wording "before reading any data") | None needed; optionally move check to config validation. |

Verified unchanged: key modes and required CQL key types, digest format, stored-key skip rules,
default generated column names, `invalidRecordHandling` (default `skip`, quoted `"null"`, unquoted
null rejected), 131,072-element `listOverflowHandling` semantics, double-to-decimal exactness,
static-column/partition-key rule, `preserveTTL`/`preserveGeneration` defaults, environment vs
configured credentials precedence and broadcast behaviour, `tlsName` TLS activation and inline
TLS-name rejection, default port 3000 and the JDK 17 requirement all match the code.
