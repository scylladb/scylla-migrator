# Feature Specification: Parquet-to-CQL Migration

**Feature Branch**: `002-parquet-to-cql-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/migrate-from-cassandra-or-parquet.rst` (sections "Reading from a Parquet File" and "Configuring the Destination"); `docs/source/configuration.rst` (sections "Source" type list, "Parquet Source", "AWS Authentication", and the Parquet mention in "CQL-Compatible Source" `preserveCollectionTimestamps` batching); `docs/source/resume-interrupted-migration.rst` (Parquet file-level tracking note, automatic resume)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Load a local Parquet dataset into ScyllaDB (Priority: P1)

A migration operator has data exported in Parquet format on the filesystem of the Spark master
node and wants it loaded into a ScyllaDB (or Cassandra) table. They configure
`source: {type: parquet, path: /my-directory/my-file.parquet}` and a CQL target (see
`001-cql-to-cql-migration` for target properties), then submit the Migrator job.

**Why this priority**: The minimal Parquet path; it delivers value without any cloud setup.

**Independent Test**: Write a Parquet file with known rows to a local path, migrate it to a CQL
table, and check that the target contains every row.

**Acceptance Scenarios**:

1. **Given** a Parquet file at a local path and a `cassandra` target, **When** the Migrator runs to
   completion, **Then** every row of the Parquet data is written to the target table.
2. **Given** the target table does not exist, **When** the Migrator runs, **Then** the target table
   is created on the fly and the data is written into it.
3. **Given** `source.type: parquet` and a target type other than `cassandra`, **When** the Migrator
   starts, **Then** the combination is not supported (a `parquet` source can only be used with a
   `cassandra` target).

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
3. **Given** a custom S3-compatible `endpoint` (host and port), **When** `region` is also set,
   **Then** the Migrator reads from that endpoint.

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
3. **Given** `assumeRole` without `region`, **When** the configuration is validated, **Then** it is
   invalid because `region` is mandatory with `assumeRole`.

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

### Edge Cases

- A local `path` must be on the Spark master node's filesystem.
- An S3 object that is not public requires `credentials`; the docs only describe credentials as
  needed "in case the object is not public".
- `region` is required when using a custom `endpoint` and when using `assumeRole`.
- Resume granularity is a whole Parquet file: a file that was partially written when the job was
  interrupted is migrated again from its start; writes are idempotent on the target primary key, so
  this costs extra load only.
- When the CQL target uses `stripTrailingZerosForDecimals`, decimal values read from Parquet follow
  the same padding/stripping rules as CQL sources (see `001-cql-to-cql-migration`).
- `configuration.rst` mentions that collection-timestamp batching applies "a Parquet restore: a
  batch of files of about that many splits' size"; how Parquet data supplies per-element collection
  metadata is not documented, so this spec does not define it.
- The configuration reference describes `path` as "Path of the Parquet file", while savepoints track
  progress per Parquet file. [NEEDS CLARIFICATION: Can `source.path` point to a directory (or
  prefix) containing several Parquet files, and if so are sub-directories included? The docs only
  show single-file paths, yet resume is described as file-level tracking across "Parquet files".]

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept a source of `type: parquet` with a mandatory `path`.
- **FR-002**: System MUST accept a `parquet` source only together with a `cassandra` (CQL) target.
- **FR-003**: System MUST read Parquet data from a path on the Spark master node's filesystem.
- **FR-004**: System MUST read Parquet data from S3 using `s3a://` URLs.
- **FR-005**: System MUST accept optional `credentials` with `accessKey` and `secretKey` for S3 and
  use them to authenticate.
- **FR-006**: System MUST accept optional `endpoint` (`host`, `port`) and `region` for S3 access,
  and MUST require `region` when a custom endpoint is used.
- **FR-007**: System MUST support AssumeRole via `credentials.assumeRole.arn`, using the provided
  access/secret keys as the identity that assumes the role.
- **FR-008**: System MUST use `scylla-migrator` as the role session name when
  `credentials.assumeRole.sessionName` is not set.
- **FR-009**: System MUST require `region` when `assumeRole` is used.
- **FR-010**: System MUST write the Parquet rows to the configured CQL target using the target
  behaviour of `001-cql-to-cql-migration` (table auto-creation, consistency level, connections,
  `stripTrailingZerosForDecimals`, optional forced `writeTTLInS`/`writeWritetimestampInuS`, TLS,
  Astra `cloud` target).
- **FR-011**: System MUST apply configured column renames to Parquet columns before writing (see
  `007-column-renames`).
- **FR-012**: System MUST support savepoints for Parquet sources with file-level tracking, recording
  completed Parquet files so that automatic resume skips them (see `008-savepoints-and-resume`).
- **FR-013**: System MUST never write AWS secrets to savepoints in clear text (savepoints store
  `<redacted>`), and resume MUST take credentials from the launch configuration.

### Key Entities

- **Parquet Source configuration** (`source`, `type: parquet`): `path` (local or `s3a://`),
  optional `credentials`, `endpoint`, `region`.
- **AWS credentials** (`credentials`): `accessKey`, `secretKey`, optional `assumeRole`
  (`arn`, optional `sessionName`).
- **Parquet file**: the unit of resume progress; completed files are recorded in savepoints.
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
- The docs do not describe validating a Parquet source; Parquet-to-CQL validation is out of scope for
  this spec (see `009-migration-validation`).
