# Feature Specification: MySQL to CQL Migration

**Feature Branch**: `006-mysql-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/configuration.rst` — "Overview" (savepoints comment), "Source" (source type list), "Savepoints" (MySQL paragraph), "Validation" (MySQL join keys, quiescence, `hashColumns`); `docs/source/resume-interrupted-migration.rst` (resumable sources note); `docs/source/validate.rst` (MySQL quiescence, "MySQL custom join keys"). Source property names only: `config.yaml.example` ("Example for loading from MySQL"); source code at commit 06835d0 (`config/SourceSettings.scala` (MySQL settings, decoder and validation), `config/MigratorConfig.scala`, `config/HostValidation.scala`, `readers/MySQL.scala`, `readers/MySQLSchemaLogger.scala`, `readers/jdbc/*.scala`, `writers/Scylla.scala`, `scylla/MySQLToScyllaValidator.scala`, `Migrator.scala`, `Validator.scala`).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Migrate a MySQL table into ScyllaDB (Priority: P1)

A database operator wants to copy all rows (or a filtered subset) of one MySQL table into an
existing ScyllaDB table, optionally splitting the read across several Spark tasks.

**Why this priority**: This is the core migration path; everything else (validation, restart
guidance) depends on it.

**Independent Test**: Create a MySQL table with a primary key and rows, create a matching
ScyllaDB table, run the migrator with `source.type: mysql`, and confirm every source row is
present in the target.

**Acceptance Scenarios**:

1. **Given** a source configured with `type: mysql`, `host`, `port`, `database`, `table` and
   `credentials` (`username`, `password`), and a matching ScyllaDB target, **When** the migration
   runs, **Then** every row of the MySQL table is written to the target.
2. **Given** a `where` filter, **When** the migration runs, **Then** only rows matching the
   filter are migrated.
3. **Given** `partitionColumn`, `numPartitions`, `lowerBound` and `upperBound`, **When** the
   migration runs, **Then** the read is split across multiple Spark tasks.
4. **Given** `renames` in the configuration, **When** the migration runs, **Then** source
   columns are written under their renamed target names (see `007-column-renames`).
5. **Given** no `fetchSize`, **When** the migration runs, **Then** rows are streamed with a
   server-side cursor fetching 1000 rows per round trip.
6. **Given** no partitioning properties, **When** the migration runs, **Then** the whole table is
   read in a single Spark partition and a warning recommends configuring partitioning for large
   tables.
7. **Given** `partitionColumn: id` (an integer column), `numPartitions: 8`, `lowerBound: 0` and
   `upperBound: 1000000`, **When** the migration runs, **Then** the read is split into 8 ranges
   and a warning states that partitioned reads give no single global snapshot.
8. **Given** a `DATE` partition column with `lowerBound: "2024-01-01"` and
   `upperBound: "2024-02-01"` (or a `TIMESTAMP`/`DATETIME` column with
   `"2024-01-01 00:00:00"`-style bounds), **When** the migration runs, **Then** the bounds are
   accepted; **given** epoch-millisecond numbers for such a column, **Then** the job fails
   before reading.
9. **Given** a `scylla`/`cassandra` target, **When** the migration runs, **Then** it proceeds;
   **given** a `parquet`, `dynamodb` or `alternator` target, **Then** it fails at startup with
   "Unsupported combination of source and target".

---

### User Story 2 - Restart an interrupted MySQL migration safely (Priority: P1)

A MySQL migration is interrupted. The operator needs to know that it cannot be resumed and that
re-running it from scratch is safe.

**Why this priority**: Large tables make interruptions likely; incorrect expectations about
resume would cause either data loss or wasted effort.

**Independent Test**: Interrupt a MySQL migration midway, re-run it with the same configuration
(no `savepoints` section), and confirm the target ends up with exactly the source rows and no
duplicates.

**Acceptance Scenarios**:

1. **Given** a MySQL migration configuration without a `savepoints` section, **When** the
   migrator starts, **Then** the configuration is accepted (the section is optional for MySQL).
1a. **Given** a MySQL configuration that nevertheless contains a parsable `savepoints` section,
   **When** the migrator starts, **Then** the section is ignored (no savepoints written or
   resumed), a warning "Source does not support savepoints; any configured savepoints settings
   are ignored…" is logged, and the migration runs from the beginning.
2. **Given** an interrupted MySQL migration, **When** it is re-run, **Then** it restarts from the
   beginning and re-reads the whole (filtered) table.
3. **Given** a target table whose primary key matches the source key (idempotent upserts),
   **When** the migration is re-run after a partial run, **Then** the final target content equals
   the source without duplicates.

---

### User Story 3 - Validate a MySQL-to-ScyllaDB migration (Priority: P2)

After migrating, the operator runs the Validator to confirm the ScyllaDB table matches the MySQL
table, optionally using a custom join key and hashing large columns to reduce shuffle volume.

**Why this priority**: Confirms correctness, but only after a migration exists.

**Independent Test**: Stop writes on both sides, run the Validator with a `validation` section
and `source.primaryKey`, and check that differences are reported (or none, for an exact copy).

**Acceptance Scenarios**:

1. **Given** `source.primaryKey` matching MySQL's declared primary key (including column order),
   **When** the Validator runs, **Then** it joins source and target on that key.
2. **Given** `source.primaryKey` differing from the declared primary key and
   `skipSourcePrimaryKeyValidation` left `false`, **When** the Validator runs, **Then** it refuses
   because the key does not match the table's primary key.
3. **Given** `skipSourcePrimaryKeyValidation: true` and neither `verifySourceJoinKeyUniqueness`
   nor `acknowledgeUnverifiedJoinKey` set, **When** the Validator runs, **Then** it refuses to use
   the custom join key.
4. **Given** `skipSourcePrimaryKeyValidation: true` and `verifySourceJoinKeyUniqueness: true`,
   **When** the selected source rows contain duplicate key combinations or null key components,
   **Then** validation is rejected before target comparison.
5. **Given** both `verifySourceJoinKeyUniqueness` and `acknowledgeUnverifiedJoinKey` are `true`,
   **When** the Validator runs, **Then** the uniqueness verification is performed.
6. **Given** a custom join key that, after `renames`, does not match the ScyllaDB target primary
   key, **When** the Validator runs, **Then** it refuses regardless of the override.
7. **Given** `validation.hashColumns: [large_text_column, blob_column]` (source-side names),
   **When** the Validator runs, **Then** those columns are compared via a content hash, with
   renames applied automatically.

---

### User Story 4 - Configure the MySQL connection safely (Priority: P2)

The operator needs configuration mistakes and dangerous JDBC settings to be caught before any
data is read, and secrets (password, WHERE filter) never to appear in logs.

**Why this priority**: The connection is built from operator-supplied strings; unsafe values
could read local files, inject SQL, or leak credentials.

**Independent Test**: Load configurations with each invalid value listed in FR-015–FR-022 and
check they are rejected before connecting; run a valid migration and check the logged
configuration redacts the password and WHERE filter.

**Acceptance Scenarios**:

1. **Given** `host: "db/evil?x"`, a `database` or `table` with characters outside
   `[A-Za-z0-9_$-]`, an empty username or password, or `port: 70000`, **When** the configuration
   loads, **Then** it is rejected.
2. **Given** `connectionProperties: {allowLoadLocalInfile: "true"}` (any letter case), **When** the
   configuration loads, **Then** it is rejected as a blocked security-sensitive key.
3. **Given** `connectionProperties: {useSSL: "true", connectTimeout: "5000"}`, **When** the
   migration runs, **Then** both are applied; **given** `connectTimeout: "abc"`, **Then** the
   configuration is rejected.
4. **Given** `connectionProperties: {useCursorFetch: "false"}` or `{fetchsize: "10"}`, **When** the
   migration runs, **Then** the property is ignored with a warning naming it.
5. **Given** `where: "id > 10; DROP TABLE t"`, **When** the migration starts, **Then** it fails
   because the filter contains a forbidden keyword; **given** a filter containing a newline,
   **Then** the configuration is rejected at load.
6. **Given** a configuration whose `password` is the literal `<redacted>` (a savepoint/log copy),
   **When** it loads, **Then** it is rejected with a hint to use the original configuration.

---

### Edge Cases

- The MySQL source type is missing from the configuration reference's list of source types and
  has no source-configuration section in the docs; this spec documents the as-built contract
  from code (FR-001, FR-015–FR-026).
- A `savepoints` section is supplied for a MySQL migration: its settings are ignored with a
  warning (not rejected), but the section must still be structurally valid (e.g. not both `path`
  and `target`, `intervalSeconds` present for file targets) or configuration loading fails.
- Unknown properties in the MySQL source section are silently ignored (no error), so a typo in
  an optional property name falls back to its default.
- Rows whose target primary-key columns are null are dropped before writing by default for a
  MySQL source (target `dropNullPrimaryKeys` defaults to true for non-CQL, non-DynamoDB
  sources); see `001-cql-to-cql-migration`.
- MySQL `TINYINT(1)` columns are read as integers, not booleans; `UNSIGNED BIGINT` columns are read
  as exact decimals (logged at startup) and suit a CQL `varint` target column; zero dates fail
  the read unless `zeroDateTimeBehavior` opts in to `CONVERT_TO_NULL` or `ROUND` (logged as a
  warning).
- Temporal values are read in the Spark session time zone (`spark.sql.session.timeZone`), which
  is also used to interpret `TIMESTAMP` partition bounds; an invalid zone fails the read.
- `fetchSize` above 10,000 is accepted but logs a memory warning; above 100,000 is rejected.
- The partition column must be numeric, `DATE` or `TIMESTAMP`/`DATETIME` and must exist in the
  table (case-insensitive match); otherwise the job fails before reading, listing available
  columns.
- A custom `spark.redaction.regex` that does not cover the JDBC option keys holding secrets
  (`password`, or sensitive `connectionProperties` keys) makes the job refuse to create the
  reader.
- Interruption mid-run: partial data remains in the target; the migration restarts from the
  beginning, so target writes must be idempotent.
- Partitioned reads use multiple independent statements/connections and give no global
  point-in-time snapshot; quiesce the source if exact consistency matters.
- Live writes on either side during validation (including throughout the uniqueness check)
  can produce spurious differences; writes must be stopped on both sides first.
- `acknowledgeUnverifiedJoinKey` with a key that is actually non-unique: conflicting duplicate
  source rows can produce field mismatches against one target row, and with `copyMissingRows`
  duplicates can be written under the same target key (last write wins).
- `verifySourceJoinKeyUniqueness` adds a full scan and shuffle and persists source rows in Spark
  memory and disk until validation succeeds or fails; an existing caller-owned persistence
  setting is preserved.
- A source `where` filter limits uniqueness verification to the selected rows.
- `hashColumns` reduces join/shuffle volume but the Validator still reads the original payload
  columns from both MySQL and ScyllaDB before hashing.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept `source.type: mysql` and migrate a single MySQL table into a
  CQL target (`target.type: scylla` or its `cassandra` alias) only; any other target type MUST
  fail at startup with an "Unsupported combination of source and target" error. The source is
  configured with required `host`, `port` (no default), `database`, `table` and `credentials`
  (`username`, `password`), plus optional `fetchSize` (default 1000), `primaryKey`, `where`,
  `partitionColumn`, `numPartitions`, `lowerBound`, `upperBound`, `zeroDateTimeBehavior`
  (default `EXCEPTION`), `connectionProperties`, and the validation-only
  `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness` and
  `acknowledgeUnverifiedJoinKey` (all default `false`).
- **FR-002**: System MUST migrate only rows matching the optional source `where` filter when one
  is set (applied as `SELECT * FROM <table> WHERE <filter>`), MUST reject an empty/blank filter
  or one containing control characters (including U+2028/U+2029) at configuration load, and MUST
  fail before reading when the filter — outside quoted text and comments — contains any of the
  keywords DROP, DELETE, TRUNCATE, ALTER, CREATE, EXEC, EXECUTE, UNION, INTO, OUTFILE, DUMPFILE,
  LOAD_FILE, BENCHMARK, SLEEP, GRANT, REVOKE, or contains MySQL executable comments (`/*!…*/`),
  unterminated quotes/comments, or backslash escapes inside quoted strings. The filter is
  otherwise trusted SQL.
- **FR-003**: System MUST support splitting the source read across `numPartitions` Spark tasks
  using `partitionColumn`, `numPartitions`, `lowerBound` and `upperBound`; `partitionColumn` and
  `numPartitions` MUST be set together, both bounds MUST be set when they are, bounds MUST NOT be
  set without them, `numPartitions` MUST be positive, `partitionColumn` MUST be a plain
  identifier `[A-Za-z_][A-Za-z0-9_]*` or a backtick-quoted identifier, and bounds MUST be
  nonblank with `lowerBound` < `upperBound`. Bounds MUST be integer literals for numeric
  columns, ISO `DATE` literals for `DATE` columns and timestamp literals for
  `TIMESTAMP`/`DATETIME` columns; other column types MUST be rejected before reading. Rows
  outside the bounds are still read (bounds only shape the split). Without partitioning the
  table MUST be read in one partition with a warning.
- **FR-004**: System MUST apply top-level `renames` to MySQL source columns when writing to the
  target and when validating.
- **FR-005**: System MUST NOT support savepoints or resume for MySQL sources; an interrupted
  MySQL migration MUST be restarted from the beginning, and no automatic resume lookup is
  performed.
- **FR-006**: The documentation MUST state that MySQL is not resumable, giving the reason (no
  durable per-range progress; partitioned reads use multiple independent statements rather than
  one resumable snapshot) and advising idempotent target writes for large tables.
- **FR-007**: The `savepoints` section MUST be optional for a MySQL migration. When present, its
  settings MUST be ignored (value checks such as positive `intervalSeconds` skipped; the section
  is omitted when the configuration is rendered/logged) and System MUST log the warning "Source
  does not support savepoints; any configured savepoints settings are ignored. If this migration
  is interrupted, it must be restarted from scratch. Ensure the target table supports idempotent
  writes."; a structurally invalid `savepoints` section still fails configuration loading.
- **FR-008**: Validator MUST require `source.primaryKey` (and a `validation` section) and, by
  default, require it to match the MySQL table's declared primary key, including column order
  (column names compared case-insensitively); a table exposing no primary key MUST be refused
  unless `skipSourcePrimaryKeyValidation` is set.
- **FR-009**: Validator MUST default `skipSourcePrimaryKeyValidation`,
  `verifySourceJoinKeyUniqueness` and `acknowledgeUnverifiedJoinKey` to `false`; these options
  MUST affect validation only, not migration reads.
- **FR-010**: When `skipSourcePrimaryKeyValidation` is `true`, Validator MUST require the
  configured join-key columns to exist in the source and, after `renames`, to match the ScyllaDB
  target primary key.
- **FR-011**: When `skipSourcePrimaryKeyValidation` is `true`, Validator MUST refuse to run unless
  `verifySourceJoinKeyUniqueness` or `acknowledgeUnverifiedJoinKey` is `true`; when both are
  `true`, verification MUST take precedence; when `skipSourcePrimaryKeyValidation` is `false`,
  both safeguard options MUST have no effect.
- **FR-012**: With `verifySourceJoinKeyUniqueness: true`, Validator MUST scan the selected source
  rows and reject duplicate key combinations or null key components before target validation,
  and MUST release persisted source rows on success or failure.
- **FR-013**: Validator MUST support optional `validation.hashColumns` (source-side names, renames
  applied automatically) compared via a content hash for MySQL-to-ScyllaDB validation;
  `hashColumns` MUST NOT include primary-key columns (source or renamed target names) and MUST
  NOT contain blank names.
- **FR-014**: The documentation MUST instruct operators to stop writes on both the MySQL source and
  the ScyllaDB target before validating, because validation may read with multiple statements
  over time and provides no global point-in-time snapshot; the Validator MUST also log this as
  a warning at start.
- **FR-015**: System MUST reject at configuration load: an empty `host`, or one that is not a
  hostname, IPv4 address or (optionally bracketed) IPv6 address (bracketed IPv4 is rejected with
  a hint); a `port` outside 1–65535; an empty `database` or `table`, or one containing characters
  other than ASCII letters, digits, `_`, `$` or `-`; an empty `username` or `password`; and a
  `password` equal to `<redacted>` (with a hint that the file looks like a savepoint).
- **FR-016**: System MUST require `fetchSize` to be between 1 and 100,000 and MUST log a memory
  warning when it exceeds 10,000; rows MUST be streamed with a server-side cursor using that
  fetch size.
- **FR-017**: System MUST accept `zeroDateTimeBehavior` values `EXCEPTION` (default: reading a
  zero date fails), `CONVERT_TO_NULL` and `ROUND` (case-insensitive), MUST reject other values,
  and MUST log a warning when a non-default value is used.
- **FR-018**: System MUST pass `connectionProperties` to the MySQL driver except: keys in the
  blocked list (`allowLoadLocalInfile`, `allowUrlInLocalInfile`, `allowLoadLocalInfileInPath`,
  `autoDeserialize`, `allowPublicKeyRetrieval`, `serverRSAPublicKeyFile`, `queryInterceptors`,
  `exceptionInterceptors`, `connectionLifecycleInterceptors`, `authenticationPlugins`,
  `propertiesTransform`, `socketFactory`, `autoGenerateTestcaseScript`, `statementInterceptors`,
  `connectionPropertiesLoadBalancer`, `allowMultiQueries`, `profilerEventHandler`,
  `clientInfoProvider`, `logger`, `sessionVariables`; matched case-insensitively) MUST be
  rejected at configuration load; Spark-reserved JDBC option keys (e.g. `url`, `dbtable`,
  `user`, `password`, `fetchsize`, `partitionColumn`, `sessionInitStatement`) and keys the
  migrator sets in the JDBC URL (`useCursorFetch`, `tinyInt1isBit`, `zeroDateTimeBehavior`,
  `connectionTimeZone`, `forceConnectionTimeZoneToSession`) MUST be ignored with a warning
  (for `zeroDateTimeBehavior` the warning points to the source property).
- **FR-019**: System MUST read `maxAllowedPacket` (default 256 MiB, positive integer),
  `connectTimeout` (default 30,000 ms) and `socketTimeout` (default 600,000 ms; both integers
  0–2,147,483,647) from `connectionProperties` when supplied, MUST reject non-numeric,
  out-of-range or duplicate-with-different-casing values at configuration load, and MUST apply
  them to the connection URL. (As built, the reader also logs "Ignoring connectionProperty
  '<key>' – this property is already set in the JDBC URL" for these three keys even though the
  supplied value is used.)
- **FR-020**: System MUST log a security warning when a TLS keystore/certificate URL property in
  `connectionProperties` uses a `file:` or `http:` scheme.
- **FR-021**: System MUST read MySQL temporal values using the Spark session time zone and MUST
  read `TINYINT(1)` as an integer, not a boolean.
- **FR-022**: System MUST never log the cleartext `password` or `where` filter: the logged
  configuration MUST show `<redacted>` for them (and for sensitive `connectionProperties` keys),
  the WHERE filter MUST be logged only by length, and System MUST refuse to build the reader when
  `spark.redaction.regex` does not cover every secret-bearing JDBC option key.
- **FR-023**: System MUST log at startup the connection target (host, port, database, table),
  the fetch size, whether partitioning is used (column, JDBC type, partitions, bounds), the
  number of Spark partitions, a note that MySQL reads are not resumable, and an info message for
  columns that look like `UNSIGNED BIGINT` (kept as exact decimals for CQL `varint`).
- **FR-024**: System MUST fail before reading when the configured `partitionColumn` does not exist
  in the table (listing available columns) or its type is not numeric, `DATE` or
  `TIMESTAMP`/`DATETIME`.
- **FR-025**: By default System MUST drop, before writing, rows whose target primary-key columns
  are null for a MySQL source (overridable with target `dropNullPrimaryKeys`).
- **FR-026**: Validator MUST reject renames that map two primary-key columns to the same target
  name and MUST refuse when a configured `primaryKey` column is not present in the MySQL table.

### Key Entities

- **MySQL source configuration**: `type: mysql`, connection (`host`, `port`, `database`, `table`,
  `credentials`, `connectionProperties`), read tuning (`fetchSize`, `partitionColumn`,
  `numPartitions`, `lowerBound`, `upperBound`), data semantics (`zeroDateTimeBehavior`), row
  filter (`where`), and validation-only join-key options (`primaryKey`,
  `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness`,
  `acknowledgeUnverifiedJoinKey`).
- **Join key**: `source.primaryKey` columns used to match source rows to target rows; either the
  declared MySQL primary key or a custom key guarded by a safeguard option.
- **Hash columns**: list of source columns compared by content hash during validation.
- **Target CQL table**: pre-existing ScyllaDB table whose primary key equals the (renamed) join
  key.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a completed migration with a quiesced source, the Validator reports 0 missing
  or differing rows.
- **SC-002**: Re-running an interrupted migration to completion yields a target with exactly the
  source row count (no duplicates) when the target key equals the source key.
- **SC-003**: 100% of Validator runs with a custom join key and no safeguard are refused before
  any comparison.
- **SC-004**: 100% of duplicate or null custom join-key values present in the selected source rows
  are detected when uniqueness verification is enabled.
- **SC-005**: 100% of the invalid source configurations listed in FR-003 and FR-015–FR-019 are
  rejected before any row is read, and 0 cleartext passwords or WHERE filters appear in logs.

## Assumptions

- The operator creates the target keyspace and table, with a primary key that makes re-running
  the migration idempotent.
- Writes to the MySQL source (and the target, during validation) are stopped when a consistent
  copy or validation is required.
- The `where` filter is trusted operator input.
- Target-side settings follow the CQL target configuration (`001-cql-to-cql-migration`); the
  general Validator behaviour is in `009-migration-validation`; savepoints for other sources are
  in `008-savepoints-and-resume`; job submission in `010-spark-cluster-and-job-submission`.
- Source property names, defaults and validation rules in FR-001 and FR-015–FR-026 come from the
  source code (and `config.yaml.example`), not from the user documentation, which lacks a
  "MySQL Source" section (see Docs vs Code Review).

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | MySQL source reference | `mysql` missing from source type list; no "MySQL Source" section (only `config.yaml.example`) | Full decoder with required/optional properties, defaults and validation (`config/SourceSettings.scala:292-518,864-1009`) | Code (FR-001, FR-015–FR-019) | Add `mysql` to the type list and a "MySQL Source" section with the property table. |
| 2 | Configured `savepoints` | "This section must be omitted for MySQL migrations" | Optional; if present its settings are ignored with a warning, but it must still parse (`config/MigratorConfig.scala:61-87`; `Migrator.scala:67-73`; test `MigratorConfigTest.scala:914-958`) | Code (FR-007) | Say "optional and ignored (warning logged)" instead of "must be omitted". |
| 3 | Supported targets | Not stated | Only `scylla`/`cassandra`; others fail with "Unsupported combination of source and target" (`Migrator.scala:84-87,110-114`) | Code (FR-001) | State the supported target. |
| 4 | WHERE filter safety | `config.yaml.example`: "treated as trusted SQL input, not sanitized" | Blank/control characters rejected at load; forbidden keywords, executable comments, backslash escapes, unterminated quotes rejected before reading (`config/SourceSettings.scala:921-934`; `readers/MySQL.scala:278-373`; `readers/jdbc/JdbcWhereFilter.scala`) | Code (FR-002) | Document the restrictions so legitimate filters (e.g. `name = 'a\'b'`) aren't surprising. |
| 5 | `connectionProperties` | "extra JDBC connection properties (e.g. SSL, timeouts)" | Blocked keys rejected at load; reserved/URL-embedded keys ignored with warnings; timeouts/`maxAllowedPacket` validated with defaults (`readers/MySQL.scala:71-101,417-536`; `readers/jdbc/JdbcSafeProperties.scala`; `readers/jdbc/JdbcNumericProperty.scala`) | Code (FR-018, FR-019, FR-020) | List blocked/ignored keys and the default timeouts. |
| 5a | Timeout properties warning | "e.g. SSL, timeouts" | `connectTimeout`/`socketTimeout`/`maxAllowedPacket` values are applied via the JDBC URL, yet an "Ignoring connectionProperty … already set in the JDBC URL" warning is logged for them (`readers/MySQL.scala:423-428,482-491,521-524`) | Code (FR-019) | Document that the warning is benign for these keys (or fix the misleading log in code). |
| 6 | `fetchSize` limits | "Defaults to 1000" | Must be 1–100,000; warning above 10,000 (`config/SourceSettings.scala:449-452`; `readers/jdbc/JdbcFetchSize.scala`) | Code (FR-016) | Document the bounds. |
| 7 | Partitioned-read validation | Bounds format examples only | Pairing rules, identifier pattern, ordering, column existence and type checks (`config/SourceSettings.scala:380-516`; `readers/jdbc/JdbcPartitionBounds.scala`; `readers/MySQL.scala:662-708`) | Code (FR-003, FR-024) | Document the rules and that bounds do not filter rows. |
| 8 | Type/time-zone semantics | Not documented | `tinyInt1isBit=false`, session time zone forced from `spark.sql.session.timeZone`, unsigned BIGINT as decimal (`readers/MySQL.scala:417-441`; `readers/MySQLSchemaLogger.scala`) | Code (FR-021, edge cases) | Add a type-mapping note. |
| 9 | Secrets in logs | Not documented for MySQL | Password/WHERE redacted in logged config; WHERE logged by length; reader refused if `spark.redaction.regex` misses secret keys (`config/MigratorConfig.scala:171-190`; `readers/MySQL.scala:586-660`) | Code (FR-022) | Mention redaction and the `spark.redaction.regex` requirement. |
| 10 | Null primary keys | Not documented for MySQL | Rows with null target PK values dropped by default (`writers/Scylla.scala:97-108,314-317`) | Code (FR-025) | Cross-reference `dropNullPrimaryKeys` in the MySQL section. |
| 11 | Unknown source properties | Not documented | Silently ignored (non-strict decoder, `config/SourceSettings.scala:333-334`) | Code (edge case) | Optional warning in docs. |

Verified unchanged: no MySQL resume/savepoints and restart-from-scratch semantics, the non-resumable
rationale, partitioned-read no-snapshot warning, `renames` application, and all Validator
join-key rules (declared-PK match, `skipSourcePrimaryKeyValidation` safeguards, precedence of
`verifySourceJoinKeyUniqueness`, uniqueness/null checks, target-PK match after renames,
`hashColumns`, quiescence warning) match the code.
