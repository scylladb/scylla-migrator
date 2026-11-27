# Feature Specification: MySQL to CQL Migration

**Feature Branch**: `006-mysql-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/configuration.rst` — "Overview" (savepoints comment), "Source" (source type list), "Savepoints" (MySQL paragraph), "Validation" (MySQL join keys, quiescence, `hashColumns`); `docs/source/resume-interrupted-migration.rst` (resumable sources note); `docs/source/validate.rst` (MySQL quiescence, "MySQL custom join keys"). Source property names only: `config.yaml.example` ("Example for loading from MySQL").

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

1. **Given** a MySQL migration configuration, **When** the operator prepares it, **Then** the
   documentation instructs them to omit the `savepoints` section.
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

### Edge Cases

- The MySQL source type is missing from the configuration reference's list of source types and
  has no source-configuration section in the docs (see FR-001 marker).
- A `savepoints` section is supplied for a MySQL migration: the docs say it must be omitted, but
  not whether it is rejected or ignored (see FR-007 marker).
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
  CQL-compatible (ScyllaDB) target, configured with `host`, `port`, `database`, `table` and
  `credentials` (`username`, `password`), plus optional `fetchSize`, `primaryKey`, `where`,
  `partitionColumn`, `numPartitions`, `lowerBound`, `upperBound`, `zeroDateTimeBehavior` and
  `connectionProperties`. [NEEDS CLARIFICATION: the MySQL source configuration (properties,
  defaults, allowed values, compatible target types) is undocumented in `docs/source`, and the
  configuration reference's list of valid source `type` values omits `mysql` even though the
  Overview, Savepoints and Validation sections and `validate.rst` refer to MySQL migrations —
  should `mysql` be added to that list with a full "MySQL Source" section, and which target
  types (only `scylla`/`cassandra`?) are supported?]
- **FR-002**: System MUST migrate only rows matching the optional source `where` filter when one
  is set.
- **FR-003**: System MUST support splitting the source read across multiple Spark tasks using
  `partitionColumn`, `numPartitions`, `lowerBound` and `upperBound`.
- **FR-004**: System MUST apply top-level `renames` to MySQL source columns when writing to the
  target and when validating.
- **FR-005**: System MUST NOT support savepoints or resume for MySQL sources; an interrupted
  MySQL migration MUST be restarted from the beginning.
- **FR-006**: The documentation MUST state that MySQL is not resumable, giving the reason (no
  durable per-range progress; partitioned reads use multiple independent statements rather than
  one resumable snapshot) and advising idempotent target writes for large tables.
- **FR-007**: The configuration for a MySQL migration MUST omit the `savepoints` section.
  [NEEDS CLARIFICATION: the docs say the `savepoints` section "must be omitted" for MySQL but do
  not say whether a configured `savepoints` section is rejected at startup or ignored (the
  Aerospike source documents "ignored") — which behaviour is the contract?]
- **FR-008**: Validator MUST, by default, require `source.primaryKey` to match the MySQL table's
  declared primary key, including column order.
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
  applied automatically) compared via a content hash for MySQL-to-ScyllaDB validation.
- **FR-014**: The documentation MUST instruct operators to stop writes on both the MySQL source and
  the ScyllaDB target before validating, because validation may read with multiple statements
  over time and provides no global point-in-time snapshot.

### Key Entities

- **MySQL source configuration**: `type: mysql`, connection (`host`, `port`, `database`, `table`,
  `credentials`), read tuning (`fetchSize`, partitioning bounds), row filter (`where`), and
  validation-only join-key options.
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

## Assumptions

- The operator creates the target keyspace and table, with a primary key that makes re-running
  the migration idempotent.
- Writes to the MySQL source (and the target, during validation) are stopped when a consistent
  copy or validation is required.
- The `where` filter is trusted operator input.
- Target-side settings follow the CQL target configuration (`001-cql-to-cql-migration`); the
  general Validator behaviour is in `009-migration-validation`; savepoints for other sources are
  in `008-savepoints-and-resume`; job submission in `010-spark-cluster-and-job-submission`.
- Source property names listed in FR-001 come from `config.yaml.example`, not from the user
  documentation; their defaults and semantics are not part of this baseline until documented.
