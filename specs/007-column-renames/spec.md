# Feature Specification: Column Renames

**Feature Branch**: `007-column-renames`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/rename-columns.rst` (whole page); `docs/source/configuration.rst` (sections "Overview", "Renames", "Aerospike Source" paragraph on generated columns and top-level `renames`, "Savepoints" `target-table` `jobId` identity, "Validation" `hashColumns`); `docs/source/resume-interrupted-migration.rst` ("Target-Database Savepoints" job identity); `docs/source/validate.rst` ("MySQL custom join keys"); `docs/source/index.rst` (feature list)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Rename columns during a migration (Priority: P1)

A migration operator's target table uses different column names from the source (for example the
source column `foo` must become `bar`, and `xxx` must become `yyy`). Because the migration requires
an existing target table to have the same schema as the source, they add a top-level `renames`
list to the configuration so that the data lands in the target-named columns.

**Why this priority**: This is the entire feature; without it, schemas with different column names
cannot be migrated.

**Independent Test**: Create a source table with columns `id, foo, xxx`, a target table with
columns `id, bar, yyy`, configure `renames: [{from: foo, to: bar}, {from: xxx, to: yyy}]`, migrate,
and check that the target `bar`/`yyy` columns hold the source `foo`/`xxx` values.

**Acceptance Scenarios**:

1. **Given** `renames` contains `- from: foo` / `to: bar`, **When** the migration runs, **Then**
   values of source column `foo` are written to target column `bar`.
2. **Given** several entries in `renames`, **When** the migration runs, **Then** every listed column
   is renamed and unlisted columns keep their names.
3. **Given** the `renames` property is omitted, **When** the migration runs, **Then** no column is
   renamed.
4. **Given** the target table does not exist and renames are configured, **When** the target table
   is created on the fly, **Then** it uses the renamed column names.

---

### User Story 2 - Map Aerospike generated columns to target names (Priority: P2)

When migrating from Aerospike (see `005-aerospike-to-cql-migration`), the migrator emits generated
columns (record key, default `aero_key`, and optionally `aero_ttl` and `aero_generation`) in
addition to bins. The operator uses top-level `renames` to map these emitted columns to the column
names of their target table.

**Why this priority**: Lets Aerospike users target existing schemas without renaming their table
columns to the migrator's generated names.

**Independent Test**: Migrate an Aerospike set with `preserveTTL: true`, configure
`renames: [{from: aero_key, to: id}, {from: aero_ttl, to: ttl_left}]`, and check the target columns.

**Acceptance Scenarios**:

1. **Given** an Aerospike source and `renames` mapping `aero_key` to `id`, **When** the migration
   runs, **Then** the generated record key is written to target column `id`.
2. **Given** renamed emitted columns, **When** the target schema is checked, **Then** every emitted
   column (after renaming) must exist in the target table.

---

### User Story 3 - Validate MySQL migrations whose key columns were renamed (Priority: P3)

The operator migrated a MySQL table with renames and validates it with a custom join key. The
configured `source.primaryKey` columns use source names; after `renames` are applied, they must
match the ScyllaDB target primary key. `validation.hashColumns` also uses source-side names, and
renames are applied automatically.

**Why this priority**: Only matters for MySQL validation, but must be correct to avoid false
mismatches (see `006-mysql-to-cql-migration`, `009-migration-validation`).

**Independent Test**: Validate a MySQL-to-ScyllaDB migration where a key column `ext_id` was renamed
to `external_id`; confirm validation accepts `primaryKey: [tenant_id, ext_id]` and rejects a key
that does not match the target key after renaming.

**Acceptance Scenarios**:

1. **Given** `source.primaryKey` lists source column names and `renames` maps them to the target
   key column names, **When** the Validator runs, **Then** the key is accepted as matching the
   target primary key.
2. **Given** the renamed custom key does not match the target primary key, **When** the Validator
   runs, **Then** it rejects the key (the source-key override does not bypass the target check).
3. **Given** `validation.hashColumns` lists source-side column names, **When** the Validator runs,
   **Then** the renamed target columns are compared.

---

### User Story 4 - Renames are part of the resume identity (Priority: P3)

The operator stores savepoints in the target database (`savepoints.target.type: target-table`).
Renames are part of the configuration identity from which the job id is derived and against which
savepoint rows are matched, so changing renames between runs starts a different job instead of
resuming the old one.

**Why this priority**: Prevents resuming progress that was recorded for a different column mapping
(see `008-savepoints-and-resume`).

**Independent Test**: Run a migration with target-table savepoints, interrupt it, change `renames`,
re-run, and confirm the old savepoint rows are not resumed.

**Acceptance Scenarios**:

1. **Given** target-table savepoints with no explicit `jobId`, **When** the job is re-run with the
   same `renames`, **Then** the derived job id is the same and the run resumes.
2. **Given** the job is re-run with different `renames`, **When** resume lookup happens, **Then**
   the earlier rows are not resumed, even if an explicit `savepoints.target.jobId` selects them.

### Edge Cases

- If an existing target table does not have the same schema as the source after renames, the
  migration is not expected to succeed; renames are the only documented schema adaptation.
- Aerospike generated column names can collide with bin names; `keyColumn`, `ttlColumn` and
  `generationColumn` change the generated names, while `renames` maps emitted columns to destination
  names. Only enabled generated columns reserve their names.
- For MySQL custom join keys, renames apply to the target check only; the configured columns must
  still exist in the source under their source names.
- Changing `renames` between runs changes the target-table savepoint identity and therefore
  discards resumability of the earlier progress (completed work may be repeated).
- The docs do not specify behaviour for a `from` column that does not exist in the source, a `to`
  name that collides with another source column, or two entries with the same `from`; this spec does
  not define those cases.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept an optional top-level `renames` property holding a list of objects,
  each with a `from` (source column name) and a `to` (target column name).
- **FR-002**: System MUST write the values of each `from` column into the corresponding `to` column
  of the target.
- **FR-003**: System MUST leave columns that are not listed in `renames` under their original names.
- **FR-004**: System MUST perform no renaming when `renames` is omitted.
- **FR-005**: System MUST use the renamed column names when it creates a missing CQL target table on
  the fly (see `001-cql-to-cql-migration`).
- **FR-006**: System MUST apply renames to columns emitted by the Aerospike source, including the
  generated key, TTL and generation columns (see `005-aerospike-to-cql-migration`).
- **FR-007**: System MUST, for MySQL validation with a custom join key, require the configured
  `source.primaryKey` columns, after applying `renames`, to match the ScyllaDB target primary key.
- **FR-008**: System MUST interpret `validation.hashColumns` as source-side column names and apply
  `renames` automatically when comparing.
- **FR-009**: System MUST include `renames` in the configuration identity used to derive the
  target-table savepoint job id and to accept savepoint rows for resume, so that a change in
  `renames` prevents resuming earlier rows, with or without an explicit `jobId`.

### Key Entities

- **Rename**: a pair (`from`, `to`) mapping a source column name to a target column name.
- **Renames list** (`renames`): optional top-level list of Rename entries applied to every row/item.
- **Configuration identity**: the redacted source, target and renames settings that identify a
  migration job for target-table savepoints (see `008-savepoints-and-resume`).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For every configured rename, 100% of non-null source values of the `from` column
  appear in the `to` column of the corresponding target row, as confirmed by validation.
- **SC-002**: A migration with no `renames` property produces target column names identical to the
  source column names in 100% of cases.
- **SC-003**: 0 target-table savepoint rows recorded under one `renames` configuration are resumed
  by a run with a different `renames` configuration.
- **SC-004**: MySQL validations with renamed key columns report 0 false key mismatches caused by
  the renaming.

## Assumptions

- The operator is responsible for creating target tables whose column names match the renamed
  names (or for letting the migrator create the table on the fly for CQL targets).
- Renames change only column names, not types or values.
- The docs describe renames generically as "item columns"; they apply to all migrations that use
  the top-level configuration, with source-specific notes only for Aerospike and MySQL.
- File-backed savepoint identity rules, if any, are owned by `008-savepoints-and-resume`; the docs
  only state the renames-in-identity rule for the `target-table` backend.
