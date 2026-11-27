# Feature Specification: Column Renames

**Feature Branch**: `007-column-renames`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/rename-columns.rst` (whole page); `docs/source/configuration.rst` (sections "Overview", "Renames", "Aerospike Source" paragraph on generated columns and top-level `renames`, "Savepoints" `target-table` `jobId` identity, "Validation" `hashColumns`); `docs/source/resume-interrupted-migration.rst` ("Target-Database Savepoints" job identity); `docs/source/validate.rst` ("MySQL custom join keys"); `docs/source/index.rst` (feature list); source code at commit 06835d0 (`config/Rename.scala`, `config/MigratorConfig.scala`, `writers/Scylla.scala`, `writers/DynamoDB.scala`, `writers/DynamoStreamReplication.scala`, `schema/SchemaResolver.scala`, `scylla/ScyllaValidator.scala`, `scylla/MySQLToScyllaValidator.scala`, `alternator/AlternatorValidator.scala`, `alternator/AlternatorMigrator.scala`, `DynamoUtils.scala`, `SavepointStore.scala`, `SavepointsResume.scala`, tests `RenameTest`, `RenamedItemsTest`)

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
4. **Given** a CQL target and renames, **When** the migration runs, **Then** the target table must
   already exist with the renamed column names (the migrator never creates CQL tables).
5. **Given** a `from` that matches no source column, **When** the migration runs, **Then** the entry
   is ignored silently.
6. **Given** renames that make two written columns share a name (compared case-insensitively),
   **When** a CQL migration starts writing, **Then** it fails with "Column name collision detected
   ..." before any row is written.
7. **Given** chained entries `a → b` then `b → c` on a CQL target, **When** the migration runs,
   **Then** source column `a` is written to `c` (entries are applied in order).
8. **Given** a DynamoDB or Alternator migration with renames, **When** items are written (snapshot
   and streamed changes), **Then** top-level attribute names are renamed with a single lookup per
   attribute (no chaining).

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
- A `from` column that does not exist in the source is ignored without warning.
- A `to` name that collides with another written column (case-insensitively) makes a CQL migration
  fail before writing. MySQL validation additionally rejects conflicting case-insensitive mappings
  of the same `from`, and renames mapping two key columns to the same target name.
- Two entries with the same `from` behave inconsistently: the CQL writer applies only the first
  (the column no longer exists under its old name afterwards), while DynamoDB writes and the CQL
  and DynamoDB validators use the last one. Avoid duplicate `from` entries.
- Matching of `from` against source column names is case-insensitive on the plain CQL/Parquet/MySQL
  write path and in MySQL validation, but exact-case on the timestamp-preservation write path (CQL
  source with `preserveTimestamps`, Parquet with migrator metadata), in the CQL and DynamoDB
  validators and on DynamoDB/Alternator writes. Use the exact source spelling.
- For DynamoDB/Alternator targets that the migrator creates, the table is created with the
  source's key schema, without applying renames; renaming a key attribute therefore requires a
  pre-created target table whose key uses the renamed name.
- File-backed savepoints (`savepoints.path`) do not check renames: a re-run with different
  `renames` still merges the earlier skip-state. Only target-table savepoints include renames in
  the resume identity.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept an optional top-level `renames` property holding a list of objects,
  each with a `from` (source column name) and a `to` (target column name).
- **FR-002**: System MUST write the values of each `from` column into the corresponding `to` column
  of the target.
- **FR-003**: System MUST leave columns that are not listed in `renames` under their original names.
- **FR-004**: System MUST perform no renaming when `renames` is omitted.
- **FR-005**: System MUST write to the renamed column names of an existing CQL target table; it does
  not create missing CQL target tables (see `001-cql-to-cql-migration` FR-028).
- **FR-006**: System MUST apply renames to columns emitted by the Aerospike source, including the
  generated key, TTL and generation columns (see `005-aerospike-to-cql-migration`).
- **FR-007**: System MUST, for MySQL validation with a custom join key, require the configured
  `source.primaryKey` columns, after applying `renames`, to match the ScyllaDB target primary key.
- **FR-008**: System MUST interpret `validation.hashColumns` as source-side column names and apply
  `renames` automatically when comparing.
- **FR-009**: System MUST include `renames` in the configuration identity used to derive the
  target-table savepoint job id and to accept savepoint rows for resume, so that a change in
  `renames` prevents resuming earlier rows, with or without an explicit `jobId`.
- **FR-010**: System MUST require both `from` and `to` in every `renames` entry, failing at startup
  with a `DecodingFailure` otherwise.
- **FR-011**: System MUST apply CQL-target renames in list order, so chained entries compose
  (`a → b`, `b → c` writes `a` to `c`), and MUST ignore entries whose `from` matches no column.
- **FR-012**: System MUST fail a CQL write, before writing rows, when column names collide
  case-insensitively after renames ("Column name collision detected ...").
- **FR-013**: System MUST apply renames on every CQL write path: CQL, Parquet, MySQL and Aerospike
  sources, the timestamp-preservation write, and the per-element collection-append writes.
- **FR-014**: System MUST rename top-level attribute names of DynamoDB/Alternator items in the
  snapshot transfer and in streamed changes (puts and delete keys), using a single lookup per
  attribute.
- **FR-015**: System MUST map source columns through `renames` when the CQL and DynamoDB validators
  compare source and target rows (see `009-migration-validation`).
- **FR-016**: System MUST, when filtering rows with null primary keys, map target key columns back
  to source columns through `renames`, and MUST fail with "Cannot resolve all primary key columns
  from the source DataFrame ... configure renames accordingly" when a target key column cannot be
  found.
- **FR-017**: System MUST, for MySQL validation, match renames case-insensitively, reject
  conflicting case-insensitive mappings and renames that map two key columns to the same target
  name, and warn about key columns without an explicit rename.

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

- The operator is responsible for creating CQL target tables whose column names match the renamed
  names; the migrator does not create them.
- Renames change only column names, not types or values.
- The docs describe renames generically as "item columns"; they apply to all migrations that use
  the top-level configuration, with source-specific notes only for Aerospike and MySQL.
- File-backed savepoints have no identity check (code: resume merges the newest readable savepoint
  regardless of `renames`); details are owned by `008-savepoints-and-resume`.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Target table creation with renames | Renames are used when the table "will be created on the fly" (`configuration.rst` CQL target `table`); `rename-columns.rst:5` "if it already exists". | CQL tables are never created (`writers/Scylla.scala:346`); DynamoDB tables are created with the source key schema, renames not applied (`DynamoUtils.scala:249-254`). | Code (FR-005, Edge Cases) | Require a pre-created CQL table; warn about renaming DynamoDB key attributes. |
| 2 | Chaining and order | Not documented. | CQL writer folds renames in order (`writers/Scylla.scala:132-140,553-556`); DynamoDB uses a single map lookup (`writers/DynamoDB.scala:121-129`, `config/MigratorConfig.scala:31-32`). | Code (FR-011, FR-014) | Document order semantics and recommend non-chained renames. |
| 3 | Case sensitivity | Not documented. | Case-insensitive on the plain DataFrame write (Spark resolver) and MySQL validation (`config/Rename.scala:16-33`); exact-case on the exploded timestamp write (`writers/Scylla.scala:135`), DynamoDB writes and the CQL/DynamoDB validators (`config/MigratorConfig.scala:31-32`). | Code (Edge Cases) | Tell users to spell `from` exactly as in the source. |
| 4 | Collisions and unknown columns | "this spec does not define those cases" (docs silent). | Case-insensitive collisions after renames fail the CQL write (`schema/SchemaResolver.scala:46-61`, `writers/Scylla.scala:249-253`); unknown `from` silently ignored; duplicate `from`: first wins on CQL writes, last wins in maps (`config/MigratorConfig.scala:31-32`); MySQL validation rejects conflicting mappings (`config/Rename.scala:16-28`, `scylla/MySQLToScyllaValidator.scala:720-740`). | Code (FR-011, FR-012, FR-017) | Document the error and recommend unique `from` entries. |
| 5 | Renames for DynamoDB/streams | Renames described generically as "item columns". | Applied to snapshot items and streamed put/delete keys (`writers/DynamoDB.scala:121-129`, `writers/DynamoStreamReplication.scala:133-150`). | Code (FR-014) | Mention streamed changes. |
| 6 | Resume identity for file savepoints | Renames-in-identity documented for `target-table` only. | File-backed resume merges the newest readable savepoint without any identity check (`SavepointsResume.scala:51-108`); target-table identity hashes source, target and renames (`SavepointStore.scala:127-142`). | Code (Edge Cases) | State that changing renames with file savepoints still resumes old progress. |
| 7 | Null-key filter with renames | Not documented. | Target key columns are mapped back through renames; unresolvable keys fail with an explicit error (`writers/Scylla.scala:32-73`). | Code (FR-016) | Optional note. |

Verified unchanged: top-level optional `renames` list of `from`/`to` pairs, unlisted columns keep
their names, Aerospike generated columns are renamable, MySQL custom join key and `hashColumns`
use source-side names with renames applied, and target-table savepoint identity includes renames.
