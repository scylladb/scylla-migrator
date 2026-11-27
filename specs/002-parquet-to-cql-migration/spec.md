# Feature Specification: Parquet-to-CQL Migration

**Feature Branch**: `002-parquet-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/migrate-from-cassandra-or-parquet.rst` (sections "Reading from a Parquet File" and "Configuring the Destination"); `docs/source/configuration.rst` (sections "Source" type list, "Parquet Source", "AWS Authentication", and the Parquet mention in "CQL-Compatible Source" `preserveCollectionTimestamps` batching); `docs/source/resume-interrupted-migration.rst` (Parquet file-level tracking note, automatic resume); source code at commit 06835d0 (`config/SourceSettings.scala`, `config/AWSCredentials.scala`, `config/Savepoints.scala`, `config/MigratorConfig.scala`, `AwsUtils.scala`, `Migrator.scala`, `Validator.scala`, `readers/Parquet.scala`, `readers/ParquetWithoutSavepoints.scala`, `readers/ParquetSavepointsManager.scala`, `readers/TimestampColumns.scala`, `readers/Cassandra.scala` (Parquet restore), `readers/CollectionBatching.scala`, `writers/Scylla.scala`, `scylla/ScyllaMigrator.scala`, `config.yaml.example`, test fixtures `parquet-to-scylla-*.yaml`)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Load a local Parquet dataset into ScyllaDB (Priority: P1)

A migration operator has data exported in Parquet format on the filesystem of the Spark master
node and wants it loaded into a ScyllaDB (or Cassandra) table. They configure
`source: {type: parquet, path: /my-directory/my-file.parquet}` (a single file or a directory of
Parquet files) and a CQL target (see `001-cql-to-cql-migration` for target properties), create the
target table, then submit the Migrator job.

**Why this priority**: The minimal Parquet path; it delivers value without any cloud setup.

**Independent Test**: Write a Parquet file with known rows to a local path, migrate it to a CQL
table, and check that the target contains every row.

**Acceptance Scenarios**:

1. **Given** a Parquet file at a local path and a `cassandra` target, **When** the Migrator runs to
   completion, **Then** every row of the Parquet data is written to the target table.
2. **Given** the target table does not exist, **When** the Migrator runs, **Then** the write fails:
   the migrator never creates CQL target tables (see `001-cql-to-cql-migration` FR-028).
3. **Given** `source.type: parquet` and a target type other than `cassandra`/`scylla`, **When** the
   Migrator starts, **Then** it stops before reading data with "Unsupported combination of source
   and target".
4. **Given** `path` names a directory, **When** the Migrator runs, **Then** every Parquet file under
   it is migrated (with file tracking enabled, files are discovered recursively, including
   sub-directories).
5. **Given** `path` matches no Parquet file, **When** the Migrator runs with file tracking, **Then**
   it fails with "No Parquet files found in <path>" (credentials and query strings in the path are
   redacted in logs and errors).
6. **Given** Parquet rows whose primary-key column is null, **When** they are migrated, **Then** they
   are skipped and "Dropped <n> rows with null primary key values" is logged, unless the target sets
   `dropNullPrimaryKeys: false`.
7. **Given** a Parquet column that does not exist in the target table (after renames), **When** the
   migration runs, **Then** the write fails.

---

### User Story 2 - Load a Parquet dataset from S3 (Priority: P1)

The operator's Parquet data lives in an S3 bucket. They set `path: s3a://my-bucket/my-key.parquet`
and, when the object is not public, supply AWS credentials (`credentials.accessKey`,
`credentials.secretKey`), optionally with `endpoint` and `region`.

**Why this priority**: S3 is the common storage for exported datasets in cloud migrations.

**Independent Test**: Upload a Parquet file to a private bucket, configure `s3a://` path and
credentials, migrate, and verify the row count in the target.

**Acceptance Scenarios**:

1. **Given** a public object at `s3a://bucket/key.parquet` and no credentials, **When** the
   Migrator runs, **Then** it reads the object and writes its rows to the target.
2. **Given** a private object and valid `credentials.accessKey`/`credentials.secretKey`, **When**
   the Migrator runs, **Then** it authenticates with those keys and migrates the data.
3. **Given** a custom S3-compatible `endpoint` (host and port), **When** the Migrator runs, **Then**
   `endpoint` is **not** used for reading the Parquet data (it only addresses the STS service when
   `assumeRole` is set); an S3-compatible store must be configured through Spark/Hadoop settings
   such as `--conf spark.hadoop.fs.s3a.endpoint=...`.
4. **Given** `credentials` and `region`, **When** the Migrator runs, **Then** the region is applied to
   the S3A client; **given** `region` without `credentials`, **Then** it is ignored and the
   Spark/Hadoop default credential chain and region apply.

---

### User Story 3 - Access S3 through an assumed role (Priority: P2)

The operator's access to the bucket is granted to a role in another account. They add
`credentials.assumeRole.arn` (and optionally `sessionName`) together with the user's access/secret
keys and the mandatory `region`.

**Why this priority**: Required for cross-account setups, but not for the basic S3 case.

**Independent Test**: Configure AssumeRole credentials for a bucket only readable via the role and
confirm the migration reads it.

**Acceptance Scenarios**:

1. **Given** `credentials.accessKey`, `credentials.secretKey`, `credentials.assumeRole.arn` and
   `region`, **When** the Migrator runs, **Then** it assumes the role and reads the bucket with the
   role's permissions.
2. **Given** `assumeRole` without `sessionName`, **When** the role is assumed, **Then** the session
   name `scylla-migrator` is used.
3. **Given** `assumeRole` without `region`, **When** the configuration is loaded, **Then** it is
   not rejected; the AWS SDK default region lookup is used for the role assumption, which fails at
   run time if no region can be found (the docs call `region` mandatory with `assumeRole`).

---

### User Story 4 - Resume an interrupted Parquet load (Priority: P2)

A long Parquet load is interrupted. The operator re-runs the job with the same configuration and
the migrator skips Parquet files that were already fully migrated.

**Why this priority**: Large loads get interrupted; restarting from scratch is costly
(Constitution principle II).

**Independent Test**: Load a multi-file Parquet dataset, interrupt after some files complete,
re-run with the same configuration and confirm completed files are not re-read.

**Acceptance Scenarios**:

1. **Given** a `savepoints` section and an interrupted Parquet migration, **When** the job is
   re-run with the same configuration, **Then** files recorded as completed in the latest savepoint
   are skipped and the remaining files are migrated (file-level tracking; savepoint mechanics are
   specified in `008-savepoints-and-resume`).
2. **Given** `savepoints.enableParquetFileTracking: false`, **When** an interrupted job is re-run,
   **Then** every file is migrated again (no progress is saved), although the `savepoints` section
   is still required.
3. **Given** a successful run, **When** it completes, **Then** a final savepoint lists every
   processed file, and a re-run with the same configuration logs "No Parquet files to process.
   Migration is complete." and writes nothing.

---

### User Story 5 - Restore a CQL export with its TTLs and WRITETIMEs (Priority: P2)

The operator exported a CQL table to Parquet with `preserveTimestamps: true`
(`011-cql-to-parquet-export`), so the files carry `__migrator_meta_<column>_ttl` and
`__migrator_meta_<column>_writetime` columns. Loading those files into a CQL table restores each
cell's TTL and WRITETIME, including per-element metadata of non-frozen sets and maps.

**Why this priority**: Makes CQL → Parquet → CQL a lossless round trip for timestamps.

**Independent Test**: Export a table with known TTL/WRITETIME values to Parquet, load it into an
empty target table, and validate timestamps.

**Acceptance Scenarios**:

1. **Given** Parquet files with migrator metadata columns, **When** they are migrated, **Then** the
   metadata columns are not written as data and each target cell carries the exported TTL/WRITETIME
   ("Detected per-column CQL timestamp metadata in Parquet schema" is logged).
2. **Given** per-element collection metadata, **When** the files are restored, **Then** files are
   written in batches (about `collectionBatchPartitions` x `spark.sql.files.maxPartitionBytes` of data
   each), each batch's files are recorded as processed only after all of its write passes succeed,
   and incremental per-file tracking is disabled.
3. **Given** malformed per-element metadata (missing collection-kind marker, non-array or wrongly
   typed sidecars, unsupported key type, mismatched element counts, out-of-range TTL), **When** the
   restore runs, **Then** it aborts with a "Parquet restore: ..." error instead of writing data
   with wrong timestamps.

### Edge Cases

- A local `path` must be on the Spark master node's filesystem.
- An S3 object that is not public requires `credentials`; the docs only describe credentials as
  needed "in case the object is not public".
- The docs say `region` is required with a custom `endpoint` and with `assumeRole`; the code does
  not enforce either at startup (see FR-006, FR-009).
- Resume granularity is a whole Parquet file: a file that was partially written when the job was
  interrupted is migrated again from its start; writes are idempotent on the target primary key, so
  this costs extra load only.
- When the CQL target uses `stripTrailingZerosForDecimals`, decimal values read from Parquet follow
  the same padding/stripping rules as CQL sources (see `001-cql-to-cql-migration`).
- Per-element collection metadata is supplied by migrator metadata columns produced by a CQL →
  Parquet export (User Story 5); Parquet data from other producers is migrated as plain values.
- `source.path` may be a single file or a directory/prefix. With file tracking, the file list is
  built recursively (sub-directories included) and recorded by full file URI. Data is then read with
  Spark's Parquet directory rules: on a first run the whole `path` is read (so `key=value`
  sub-directories may become partition columns), whereas a resumed run reads the remaining files
  individually; keep datasets flat (no partition directories) to avoid schema differences between
  runs.
- When writes run as several passes (per-element collection restore), resume granularity is a batch
  of files rather than a single file.
- `region` is applied to S3 reads only when `credentials` are given; `endpoint` is never applied to
  S3 reads.
- With timestamps restored from metadata columns, `writeTTLInS`/`writeWritetimestampInuS` on the
  target are ignored (as for CQL sources).
- A Parquet → CQL configuration cannot be validated: the Validator stops with "Unsupported
  combination of source and target".

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept a source of `type: parquet` with a mandatory `path`.
- **FR-001a**: System MUST accept as `path` either a single Parquet file or a directory/prefix of
  Parquet files, and with file tracking MUST discover files recursively, failing with "No Parquet
  files found in <path>" when none exist and "Failed to list Parquet files from <path>" when the
  path cannot be listed.
- **FR-002**: System MUST accept a `parquet` source only together with a `cassandra`/`scylla` (CQL)
  target, stopping before reading data with "Unsupported combination of source and target"
  otherwise.
- **FR-003**: System MUST read Parquet data from a path on the Spark master node's filesystem.
- **FR-004**: System MUST read Parquet data from S3 using `s3a://` URLs (and from any other Hadoop
  filesystem URI the Spark runtime supports).
- **FR-005**: System MUST accept optional `credentials` with `accessKey` and `secretKey` for S3 and
  use them to authenticate; without `credentials` the Spark/Hadoop default credential chain applies.
- **FR-006**: System MUST accept optional `endpoint` (`host`, `port`) and `region`. `region` MUST be
  applied to S3 reads when `credentials` are set; `endpoint` is used only to reach STS for
  `assumeRole` and is not applied to S3 reads. Neither is validated against the other at startup.
- **FR-007**: System MUST support AssumeRole via `credentials.assumeRole.arn`, using the provided
  access/secret keys as the identity that assumes the role.
- **FR-008**: System MUST use `scylla-migrator` as the role session name when
  `credentials.assumeRole.sessionName` is not set.
- **FR-009**: System MUST use `region` for the role assumption when `assumeRole` is used; a missing
  `region` is not rejected at startup and falls back to the AWS SDK default region lookup.
- **FR-010**: System MUST write the Parquet rows to the configured CQL target using the target
  behaviour of `001-cql-to-cql-migration` (existing target table required, consistency level,
  connections, `stripTrailingZerosForDecimals`, optional forced `writeTTLInS`/`writeWritetimestampInuS`,
  TLS, Astra `cloud` target).
- **FR-010a**: System MUST, by default, skip Parquet rows with a null primary-key value and log
  how many were dropped; the target property `dropNullPrimaryKeys: false` disables this.
- **FR-011**: System MUST apply configured column renames to Parquet columns before writing (see
  `007-column-renames`).
- **FR-012**: System MUST support savepoints for Parquet sources with file-level tracking, recording
  completed Parquet files so that automatic resume skips them (see `008-savepoints-and-resume`).
  Tracking is on by default and controlled by `savepoints.enableParquetFileTracking`; the
  `savepoints` section is required for Parquet sources even when tracking is disabled.
- **FR-012a**: System MUST, after a successful run, record every processed file in a final
  savepoint, and MUST finish without writing when all files are already recorded ("No Parquet files
  to process. Migration is complete.").
- **FR-013**: System MUST never write AWS secrets to savepoints in clear text (savepoints store
  `<redacted>`), and resume MUST take credentials from the launch configuration. Credentials, query
  strings and fragments in `path` MUST be redacted in log messages.
- **FR-014**: System MUST detect migrator metadata columns (`__migrator_meta_<column>_ttl`,
  `__migrator_meta_<column>_writetime`) in the Parquet schema and restore each cell's TTL and
  WRITETIME instead of writing those columns as data; per-element metadata of non-frozen sets and
  maps MUST be restored as well.
- **FR-015**: System MUST write per-element collection restores in batches of files (about
  `scylla.migrator.collectionBatchPartitions` x `spark.sql.files.maxPartitionBytes` each, at least one
  file), marking a batch's files processed only after all its write passes succeed.
- **FR-016**: System MUST abort a restore whose per-element metadata is malformed (missing
  collection-kind marker, inconsistent sidecar types, unsupported key type, length mismatch,
  out-of-range TTL) with a "Parquet restore: ..." error.

### Key Entities

- **Parquet Source configuration** (`source`, `type: parquet`): `path` (local or `s3a://`),
  optional `credentials`, `endpoint`, `region`.
- **AWS credentials** (`credentials`): `accessKey`, `secretKey`, optional `assumeRole`
  (`arn`, optional `sessionName`).
- **Parquet file**: the unit of resume progress; completed files are recorded in savepoints by full
  URI (a batch of files when per-element collection metadata is restored).
- **Migrator metadata columns**: `__migrator_meta_<column>_ttl` / `__migrator_meta_<column>_writetime`
  written by a CQL → Parquet export and consumed to restore TTL/WRITETIME.
- **CQL Target configuration**: as defined in `001-cql-to-cql-migration`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a completed migration, 100% of rows in the Parquet dataset are present in the
  target table with equal values (decimals subject to the documented scale padding unless
  `stripTrailingZerosForDecimals` is enabled).
- **SC-002**: A resumed run with the same configuration re-reads 0 Parquet files that a previous
  run had recorded as completed in its savepoint.
- **SC-003**: Migrations from local paths, public S3 objects, private S3 objects with keys, and S3
  objects reached via AssumeRole all succeed with only configuration changes.
- **SC-004**: 0 savepoint files contain the configured AWS secret key in clear text.

## Assumptions

- The operator produces the Parquet data with a schema compatible with the target table (same
  column names after renames and compatible types).
- The Spark cluster and job submission are set up as in `010-spark-cluster-and-job-submission`;
  for `s3a://` paths the Spark runtime can reach S3.
- Parquet is an offline dataset: nothing writes to it during the migration.
- Target-side properties and behaviours are not repeated here and are owned by
  `001-cql-to-cql-migration`; savepoint storage, naming and resume rules are owned by
  `008-savepoints-and-resume`.
- The docs do not describe validating a Parquet source, and the Validator rejects Parquet → CQL;
  validation is out of scope for this spec (see `009-migration-validation`).
- Operators create the target table before the migration.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Directory paths | "Path of the Parquet file" (`configuration.rst` Parquet Source); only single-file examples. | Directories/prefixes accepted; file tracking lists files recursively (`readers/Parquet.scala:274-297`); `config.yaml.example:223` already shows a directory. | Code (FR-001a, US1-4) | Say "file or directory"; recommend flat layouts (no `key=value` dirs). |
| 2 | Missing target table | "If it does not exist, it will be created on the fly" (`migrate-from-cassandra-or-parquet.rst:154`). | CQL tables are never created (`writers/Scylla.scala:346`). | Code (US1-2, FR-010) | Require the table to exist. |
| 3 | S3 `endpoint` | "AWS endpoint configuration" for S3 reads (`configuration.rst:999-1011`). | `endpoint` is only passed to the STS client for `assumeRole` (`AwsUtils.scala:43-81`); S3A gets keys, session token and region only (`readers/Parquet.scala:311-347`). | Code (FR-006, US2-3) | State that S3-compatible endpoints need `spark.hadoop.fs.s3a.endpoint`, or wire `endpoint` into S3A. |
| 4 | `region` handling | "required if you use a custom endpoint"; "mandatory when you use `assumeRole`". | Not validated at startup (`config/AWSCredentials.scala`, derived decoder at `config/SourceSettings.scala:765-766`); applied to S3A only when `credentials` are set (`readers/Parquet.scala:330-332`). | Code (FR-006, FR-009, US3-3) | Clarify that missing region is not detected until run time and is ignored without credentials. |
| 5 | Timestamp/collection metadata restore | Only an indirect mention of "a Parquet restore" batch in `configuration.rst`. | Metadata columns `__migrator_meta_*_ttl/_writetime` restore TTL/WRITETIME and per-element collection metadata, batched by files, with strict validation (`readers/TimestampColumns.scala:12-57`, `readers/Parquet.scala:117-145,220-272`, `readers/Cassandra.scala:1648-1800`). | Code (US5, FR-014–016) | Document the CQL → Parquet → CQL round trip. |
| 6 | Null primary keys | Not documented. | Rows with null key columns are dropped by default for Parquet sources and counted in a warning; `dropNullPrimaryKeys` overrides (`writers/Scylla.scala:97-107,310-371`). | Code (FR-010a, US1-6) | Document the behaviour and property. |
| 7 | Tracking toggle and completion | Docs mention file-level tracking only. | `savepoints.enableParquetFileTracking` (default `true`) selects tracking; disabled means no resume (`config/Savepoints.scala:31,298`, `readers/Parquet.scala:53-68`, `readers/ParquetWithoutSavepoints.scala`); final savepoint lists all files and an all-done re-run is a no-op (`readers/Parquet.scala:84-90,172-175`). | Code (FR-012, FR-012a, US4-2/3) | Document the toggle in the Parquet section. |
| 8 | Allowed target / validation | "can be used together with a target of type `cassandra` only". | `scylla` alias also accepted; other targets fail with "Unsupported combination" after config load (`Migrator.scala:94,110-114`); the Validator rejects Parquet sources (`Validator.scala:26-37`). | Code (FR-002) | Mention `scylla` and that Parquet sources cannot be validated. |

Verified unchanged: `type: parquet` with mandatory `path`, local and `s3a://` paths, optional
`credentials` (`accessKey`/`secretKey`), AssumeRole with default session name `scylla-migrator`,
renames applied before writing, CQL target behaviour, file-level savepoints with automatic resume
and redacted secrets match the documentation.
