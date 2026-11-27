# Feature Specification: CQL-to-Parquet Export (Cassandra / ScyllaDB → Parquet files)

**Feature Branch**: `011-cql-to-parquet-export`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from source code at commit 06835d0 — undocumented in docs/source)

**Input**: Source code at commit 06835d0: `migrator/src/main/scala/com/scylladb/migrator/Migrator.scala`
(source/target dispatch), `config/TargetSettings.scala` (`Parquet` target and its decoder),
`config/MigratorConfig.scala` (savepoints requirement, target-table savepoint restriction),
`config/Savepoints.scala`, `writers/Parquet.scala`, `scylla/ScyllaMigrator.scala`
(`migrateToParquet`), `scylla/CqlParquetSavepointsManager.scala`, `readers/TimestampColumns.scala`,
`readers/Cassandra.scala` (`readDataframe`), `readers/Parquet.scala` (round-trip import),
`SavepointsResume.scala`, `Validator.scala`; tests
`tests/src/test/scala/com/scylladb/migrator/config/ParquetTargetValidationTest.scala`,
`config/ParquetConfigSerializationTest.scala`, `writers/ParquetWriterTest.scala`,
`scylla/CqlToParquetToScyllaTimestampRoundTripTest.scala`,
`scylla/Cassandra5CollectionTimestampParquetRoundTripTest.scala`,
`scylla/CassandraToParquetE2EBenchmark.scala`, `scylla/ScyllaToParquetE2EBenchmark.scala`; fixtures
`tests/src/test/configurations/cassandra-to-parquet-timestamps.yaml`,
`cassandra5-to-parquet-collection-timestamps.yaml`, `bench-e2e-cassandra-to-parquet.yaml`,
`bench-e2e-scylla-to-parquet.yaml`. No `docs/source/` page describes this feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Export a CQL table to a Parquet dataset (Priority: P1)

A migration operator wants a file-based copy of an Apache Cassandra or ScyllaDB table, for example
as an offline backup, to move data between environments that cannot reach each other, or to load
it later with the Parquet source (see `002-parquet-to-cql-migration`). They write a configuration
whose `source` is the usual CQL source (`type: cassandra` or `scylla`, see `001-cql-to-cql-migration`)
and whose `target` is:

```yaml
target:
  type: parquet
  path: s3a://my-bucket/exports/my_table   # or a local / HDFS / other Hadoop path
  compression: snappy                       # optional, default snappy
  mode: error                               # optional, default error
savepoints:
  path: /app/savepoints
  intervalSeconds: 300
```

and submit the Migrator job (see `010-spark-cluster-and-job-submission`).

**Why this priority**: It is the whole feature; every other story refines this export.

**Independent Test**: Load a source table with known rows, run the Migrator with a `parquet`
target pointing to an empty location, then read the output directory with any Parquet reader
(e.g. Spark) and check the row count and column values.

**Acceptance Scenarios**:

1. **Given** a CQL source table with N rows and `target.type: parquet` with a `path` that does not
   exist, **When** the Migrator runs to completion, **Then** `path` is a directory of Parquet part
   files containing N rows whose columns are the source table's columns (same names), and the job
   exits successfully.
2. **Given** `compression` and `mode` are omitted, **When** the configuration is loaded, **Then**
   `compression` is `snappy` and `mode` is `error`.
3. **Given** `compression: gzip`, **When** the export completes, **Then** the Parquet files are
   gzip-compressed.
4. **Given** `compression: Snappy` and `mode: Overwrite` (mixed case), **When** the configuration
   is loaded, **Then** they are accepted and normalised to lower case.
5. **Given** `source.where` is set, **When** the export completes, **Then** only rows matching the
   condition are written.
6. **Given** a `cassandra` source whose `cloud.secureBundlePath` points to a DataStax Astra bundle,
   **When** the export runs, **Then** the source connection behaves as in `001-cql-to-cql-migration`
   (the target side is unaffected).

---

### User Story 2 - Choose what happens when the output path already exists (Priority: P1)

The operator decides whether an existing output location is an error, should be replaced, added
to, or left alone, by setting `target.mode` to `error`, `overwrite`, `append` or `ignore`.

**Why this priority**: Protecting or replacing existing exports is a data-safety decision every
operator must make.

**Independent Test**: Write an export to a path, then run again with each `mode` and inspect the
row count and exit status.

**Acceptance Scenarios**:

1. **Given** `mode: error` (the default) and `path` already exists, **When** the export runs,
   **Then** the write fails with an error naming the path (`Failed to write Parquet to <path>: ...`)
   and the process exits with a failure status.
2. **Given** `mode: overwrite` and `path` already contains an earlier export, **When** the export
   completes, **Then** `path` contains only the rows of this run.
3. **Given** `mode: append` and `path` already contains Parquet files, **When** the export
   completes, **Then** the new part files are added next to the existing ones.
4. **Given** `mode: ignore` and `path` already exists, **When** the export runs, **Then** nothing
   is written and the existing data is left unchanged.

---

### User Story 3 - Preserve TTL and WRITETIME in the export for a later restore (Priority: P2)

The operator wants a later Parquet → CQL import (`002-parquet-to-cql-migration`) to recreate the
original cells' TTLs and WRITETIMEs. They set `source.preserveTimestamps: true` (and optionally
`source.preserveCollectionTimestamps: true` on a Cassandra 5.0+ or ScyllaDB 2026.2+ source for
per-element timestamps of non-frozen sets/maps, see `001-cql-to-cql-migration`).

**Why this priority**: Required for a lossless backup/restore of TTL'd or time-ordered data;
optional otherwise.

**Independent Test**: Insert rows `USING TTL ... AND TIMESTAMP ...`, export with
`preserveTimestamps: true`, import the directory with a `parquet` source into a CQL target, and
compare WRITETIME/TTL of the restored cells (as done by the round-trip integration tests).

**Acceptance Scenarios**:

1. **Given** `preserveTimestamps: true`, **When** the export completes, **Then** for each regular
   column `<col>` the files also contain `__migrator_meta_<col>_ttl` (remaining TTL in seconds at
   read time) and `__migrator_meta_<col>_writetime` (WRITETIME in microseconds) columns.
2. **Given** `preserveTimestamps: true` and `preserveCollectionTimestamps: true` on a supported
   source with non-frozen set/map columns, **When** the export completes, **Then** each such column
   is accompanied by per-element TTL and WRITETIME array columns under the same
   `__migrator_meta_<col>_ttl` / `__migrator_meta_<col>_writetime` names.
3. **Given** such an export, **When** it is used as `source.path` of a `parquet` source migrating
   to a CQL target, **Then** the metadata columns are detected automatically, are not written as
   ordinary columns, and the restored cells (and collection elements) carry the exported WRITETIME
   and TTL.
4. **Given** `preserveTimestamps: false`, **When** the export completes, **Then** no
   `__migrator_meta_` columns are written.

---

### User Story 4 - Resume an interrupted export (Priority: P3)

The export is a long Spark job. As with CQL → CQL migrations, the Migrator records which source
token ranges have been processed in savepoints and, on the next run with the same configuration,
skips them (see `008-savepoints-and-resume`).

**Why this priority**: Useful for large tables, but the guarantees are weaker than for CQL targets
(see Edge Cases), so operators should treat a failed export as needing a clean re-run.

**Independent Test**: Run an export with a short `savepoints.intervalSeconds`, interrupt it, then
inspect the savepoint files for `skipTokenRanges`; re-run and inspect the logged
"Auto-resume: merged skip-state" message.

**Acceptance Scenarios**:

1. **Given** a configuration without a `savepoints` section, **When** the Migrator starts,
   **Then** it fails with `Missing required field: savepoints`.
2. **Given** `savepoints.target.type: target-table`, **When** the Migrator starts, **Then** it
   fails with `savepoints.target.type: target-table is supported only when target.type is 'scylla'.`
3. **Given** the export is running, **When** `savepoints.intervalSeconds` elapse or the driver
   receives SIGINT/SIGTERM/USR2, **Then** a savepoint is written listing the token ranges whose
   Spark write tasks have finished successfully.
4. **Given** the Parquet write fails, **When** the Migrator handles the error, **Then** it writes a
   final savepoint, logs `Caught error while writing Parquet. Will create a savepoint before
   exiting`, and exits with a failure status.
5. **Given** a previous run left a savepoint and `savepoints.autoResume` is `true` (default),
   **When** the same configuration is run again, **Then** the token ranges recorded in the latest
   savepoint are not read again.

---

### Edge Cases

- **Resume can silently drop rows.** Token ranges are recorded as soon as their Spark write task
  succeeds, but Parquet output is only committed when the whole write job commits. If the job then
  fails, the uncommitted files of those tasks are discarded while the savepoint still lists their
  ranges; a resumed run skips them and the export is incomplete.
- **Re-running after a successful export exports nothing.** The final savepoint of a successful run
  lists every token range. Re-running the same configuration with `autoResume: true` (the default)
  skips all ranges: with `mode: overwrite` the previous export is replaced by an empty dataset;
  with `append` nothing is added; with `error` the run fails because the path exists. Operators
  must clear the savepoints directory or set `savepoints.autoResume: false` for a fresh export.
- With `mode: error` (default), resuming after a failed run fails if the failed run left the output
  directory behind; `mode: append` is needed to add the remaining ranges.
- `renames` in the configuration are ignored for this target: exported columns keep their source
  names (renames are applied later, by the Parquet → CQL import, see `007-column-renames`).
- With `preserveTimestamps: true`, a source table that has both a column `foo` and a column
  `foo_ttl` (or `foo_writetime`) gets ambiguous metadata naming, because metadata columns are
  derived by suffixing `_ttl`/`_writetime` to the column name.
- The exported TTL is the remaining TTL at read time; a later import re-applies it from the import
  time, so restored data expires later than it would have on the source by the time elapsed
  between export and import.
- `compression: lzo` and `brotli` pass startup validation but need codec libraries that are not
  necessarily on the Spark classpath; the write then fails at runtime.
- `none` and `uncompressed` both produce uncompressed files.
- The target has no credentials or endpoint properties; access to `s3a://`, `gs://`, HDFS, etc.
  relies entirely on the Spark/Hadoop configuration (e.g. `spark.hadoop.fs.s3a.*` or an instance
  profile).
- The Validator does not support a `parquet` target; running it with this configuration fails with
  `Unsupported combination of source and target`.
- Only a CQL source can be exported to Parquet; any other source with `target.type: parquet`
  (e.g. `mysql`, `parquet`, `dynamodb`) passes configuration loading but fails when the job starts
  with `Unsupported combination of source and target: <Source> -> Parquet`.
- A path consisting only of whitespace is rejected; leading/trailing whitespace is trimmed.

## Requirements *(mandatory)*

### Functional Requirements

**Configuration and startup validation**

- **FR-001**: System MUST accept `target.type: parquet` and MUST support it only together with a
  CQL source (`source.type: cassandra` or its alias `scylla`); other sources combined with a
  `parquet` target MUST fail when the job starts with an "Unsupported combination of source and
  target" error.
- **FR-002**: System MUST require `target.path` and MUST reject an empty or whitespace-only value
  at startup with `Parquet target 'path' must not be empty`; surrounding whitespace MUST be trimmed.
- **FR-003**: System MUST accept an optional `target.compression`, defaulting to `snappy`, with
  allowed values (case-insensitive) `none`, `uncompressed`, `snappy`, `gzip`, `lzo`, `brotli`,
  `lz4`, `zstd`; any other value MUST fail at startup with `Invalid Parquet compression codec
  '<value>'. Valid values: ...`.
- **FR-004**: System MUST accept an optional `target.mode`, defaulting to `error`, with allowed
  values (case-insensitive) `error`, `overwrite`, `append`, `ignore`; any other value MUST fail at
  startup with `Invalid Parquet write mode '<value>'. Valid values: ...`.
- **FR-005**: System MUST normalise `compression` and `mode` to lower case (this normalised form is
  what is echoed in the redacted configuration log and stored in savepoints).
- **FR-006**: System MUST require a `savepoints` section (because the CQL source supports
  savepoints) and MUST reject `savepoints.target.type: target-table` for a `parquet` target.

**Export behaviour**

- **FR-007**: System MUST read the source table using the CQL source settings of
  `001-cql-to-cql-migration` (connection, consistency level, `splitCount`, `fetchSize`, `where`,
  `preserveTimestamps`, `preserveCollectionTimestamps`, Astra `cloud`).
- **FR-008**: System MUST write the source rows as a Parquet dataset (a directory of part files,
  not a single file) at `target.path`, with one column per source column under its source name.
- **FR-009**: System MUST apply the configured `mode` to an existing `path`: `error` fails, 
  `overwrite` replaces, `append` adds files, `ignore` skips the write.
- **FR-010**: System MUST write the files with the configured compression codec.
- **FR-011**: System MUST accept any Hadoop-compatible filesystem URI for `path` (local, `s3a://`,
  HDFS, ...) and MUST rely on the Spark/Hadoop configuration for its credentials.
- **FR-012**: System MUST log the output path, partition count, compression and mode before writing
  and a success message after writing.
- **FR-013**: System MUST NOT apply `renames` to the exported columns.

**Timestamps**

- **FR-014**: System MUST, when `source.preserveTimestamps: true`, add for each regular column
  `<col>` the columns `__migrator_meta_<col>_ttl` (remaining TTL in seconds) and
  `__migrator_meta_<col>_writetime` (WRITETIME in microseconds), one row per source row (rows are
  not split by timestamp group in the export).
- **FR-015**: System MUST, when `preserveCollectionTimestamps: true` is also set and supported by
  the source, export per-element TTL/WRITETIME of non-frozen set/map columns as array-valued
  `__migrator_meta_<col>_ttl` / `__migrator_meta_<col>_writetime` columns, subject to the same
  support rules and startup rejections as `001-cql-to-cql-migration`.
- **FR-016**: System MUST produce output that the Parquet source (`002-parquet-to-cql-migration`)
  can import, with `__migrator_meta_` columns detected automatically and turned back into
  per-cell (and per-element) TTL/WRITETIME on the CQL target.

**Failure handling and savepoints**

- **FR-017**: System MUST track progress as the set of source token ranges whose Spark write task
  finished successfully, and MUST write savepoints periodically (`savepoints.intervalSeconds`), on
  SIGINT/SIGTERM/USR2, and once at the end of the run (success or failure).
- **FR-018**: System MUST, on a write failure, log `Caught error while writing Parquet. Will create
  a savepoint before exiting`, write the final savepoint, and then exit with a failure status
  (wrapping the cause as `Failed to write Parquet to <path>: <message>`).
- **FR-019**: System MUST, when `savepoints.autoResume` is `true` (default) and a savepoint exists,
  skip the token ranges recorded in the latest savepoint on the next run.

### Key Entities

- **Parquet target configuration** (`target`, `type: parquet`): `path` (required), `compression`
  (default `snappy`), `mode` (default `error`).
- **Parquet export dataset**: directory at `path` holding Parquet part files with the source
  columns and, optionally, `__migrator_meta_<col>_ttl` / `__migrator_meta_<col>_writetime`
  metadata columns.
- **Savepoint**: configuration snapshot whose `skipTokenRanges` lists exported token ranges (see
  `008-savepoints-and-resume`).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A completed export of a quiesced source to an empty path contains 100% of the source
  rows (or of the rows matching `where`), verified by row count and value comparison of the Parquet
  dataset against the source.
- **SC-002**: Importing a `preserveTimestamps: true` export with the Parquet source into a CQL
  target yields cells whose WRITETIME equals the source WRITETIME and whose TTL is positive and no
  larger than the exported TTL, for 100% of compared cells.
- **SC-003**: 100% of configurations with an empty `path`, an unknown `compression` or an unknown
  `mode` fail before any data is read.
- **SC-004**: 100% of failed writes end with a non-zero exit status and a final savepoint.
- **SC-005**: With `mode: error`, 0 existing files at `path` are modified by an export.

## Assumptions

- The source table is quiesced or the operator accepts a non-point-in-time export (the export is
  a scan, not a snapshot).
- Operators provide filesystem access (Hadoop configuration, credentials) for the output location
  to the driver and all executors.
- The Parquet column types are those produced by the Spark Cassandra connector for each CQL type;
  this spec does not define a CQL → Parquet type mapping beyond "readable by the Parquet source".
- Because of the resume caveats in Edge Cases, operators should re-run a failed export from scratch
  (clear the savepoints directory or set `autoResume: false`, and use `mode: overwrite`).
- CQL source behaviour (`001`), savepoints (`008`), Parquet import (`002`) and job submission
  (`010`) are specified elsewhere and only referenced here.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Feature existence | Nothing. `docs/source/configuration.rst:572-576` lists target types `cassandra`, `dynamodb`, `alternator` only; no page mentions a Parquet target; `config.yaml.example` has no Parquet target example | `target.type: parquet` is decoded at `config/TargetSettings.scala:347-375` and dispatched for CQL sources at `Migrator.scala:96-97` | Code | Add a "Parquet Target" subsection to the Target section of `configuration.rst` (properties `path`, `compression`, `mode`, defaults and allowed values) and add `parquet` to the target type list; add a commented target example to `config.yaml.example` |
| 2 | Source/target compatibility | `configuration.rst:67`: "A source of type `cassandra` can be used together with a target of type `cassandra` only." | CQL source also pairs with `parquet` (`Migrator.scala:96-97`) | Code | Change to "... with a target of type `cassandra` or `parquet`" |
| 3 | User guide | `migrate-from-cassandra-or-parquet.rst` covers Cassandra → CQL and Parquet → CQL only | Export to Parquet and round-trip import exist (`scylla/ScyllaMigrator.scala:302-352`, `readers/Parquet.scala:118-124`) | Code | Add an "Exporting a table to Parquet" section (or a new `export-to-parquet.rst` page linked from `index.rst`) describing the backup/restore round trip with `preserveTimestamps` |
| 4 | TTL/WRITETIME metadata columns | Nothing; `configuration.rst` only mentions collection batching for "a Parquet restore", without saying how the metadata is stored | Export adds `__migrator_meta_<col>_ttl/_writetime` columns (`readers/TimestampColumns.scala:14-33`); import detects and strips them (`readers/Parquet.scala:118-124`) | Code | Document the `__migrator_meta_` column convention in the Parquet source and Parquet target sections |
| 5 | Savepoints / resume for Parquet target | `resume-interrupted-migration.rst:5` lists CQL, DynamoDB/Alternator and Parquet *source* only | Token ranges are tracked by a task-end listener (`scylla/CqlParquetSavepointsManager.scala:58-76`) before the Parquet job commits; auto-resume (`SavepointsResume.scala:33`, default `autoResume: true` at `config/Savepoints.scala:300`) then skips them, even after a fully successful run | Code (caveats in Edge Cases) | Document that Parquet exports are resumable only at token-range granularity, that a failed job may lose ranges, and that a re-run needs a clean savepoints directory or `autoResume: false`; consider fixing the code to record ranges only after job commit |
| 6 | Renames | `rename-columns.rst` does not mention targets other than CQL/DynamoDB | `renames` are not applied on export (`scylla/ScyllaMigrator.scala:310-332` passes the source frame unchanged) | Code | State in `rename-columns.rst` that renames are ignored for Parquet exports and applied on import |
| 7 | Validator | `validate.rst` does not list supported pairs for Parquet | No `parquet` target case (`Validator.scala:26-37`) → "Unsupported combination" | Code | Note in `validate.rst` that Parquet exports cannot be validated by the Validator |
| 8 | Compression codecs | — | `lzo`/`brotli` accepted at startup (`config/TargetSettings.scala:205-206`) but may fail at write time without codec libraries | Code | List the codecs and note that `lzo`/`brotli` need extra libraries |

Verified unchanged: nothing to verify against docs — the feature is entirely undocumented; every
requirement above is derived from code and tests (`ParquetTargetValidationTest`,
`ParquetWriterTest`, `CqlToParquetToScyllaTimestampRoundTripTest`,
`Cassandra5CollectionTimestampParquetRoundTripTest`).
