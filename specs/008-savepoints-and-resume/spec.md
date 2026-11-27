# Feature Specification: Savepoints and Resume

**Feature Branch**: `008-savepoints-and-resume`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/resume-interrupted-migration.rst` (all sections: "Automatic resume", "Savepoint file names", "Target-Database Savepoints", "Metadata consistency and availability", "Operating limits"); `docs/source/configuration.rst` — "Overview" (top-level `savepoints`, `skipTokenRanges`, `skipSegments`) and "Savepoints"; `docs/source/configuration.rst` — "Aerospike Source" ("Metadata and restarts").

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Re-run an interrupted migration and continue where it stopped (Priority: P1)

An operator's long-running migration (CQL, DynamoDB/Alternator or Parquet source) is
interrupted. They re-submit the job with the exact same configuration file and expect it to
skip already-migrated work without any manual editing.

**Why this priority**: This is the reason savepoints exist; it saves hours of re-migration and
suits orchestrators (Kubernetes, Cloud Run) that restart jobs with a fixed configuration.

**Independent Test**: Start a migration with `savepoints.path` and `intervalSeconds`, stop it
after at least one savepoint is written, re-run with the same file, and verify that completed
token ranges / scan segments / Parquet files are not migrated again.

**Acceptance Scenarios**:

1. **Given** a CQL migration with `savepoints: {path: /app/savepoints, intervalSeconds: 300}`
   that was interrupted after savepoints were written, **When** the same configuration is
   re-submitted, **Then** the migrator loads the most recent savepoint and skips the token
   ranges it records.
2. **Given** a DynamoDB or Alternator source, **When** resumed, **Then** completed scan segments
   are skipped; **given** a Parquet source, **Then** already-processed files are skipped.
3. **Given** a savepoint with redacted credentials, **When** resuming, **Then** source and target
   credentials and every other non-progress setting come from the launch configuration.
4. **Given** `savepoints.autoResume: false`, **When** the job starts, **Then** no savepoint lookup
   is performed and the migration runs in full while still writing new savepoints to the same
   location.
5. **Given** no savepoint exists or the lookup fails, **When** the job starts, **Then** a warning
   is logged and the migration continues from the launch configuration.

---

### User Story 2 - Store savepoints in shared or cloud storage (Priority: P2)

An operator runs the driver on ephemeral infrastructure and wants savepoints in a durable
location: a local/mounted directory, Google Cloud Storage or AWS S3.

**Why this priority**: Without durable storage, resume does not survive a driver restart on
managed platforms.

**Independent Test**: Configure each backend form, run until a savepoint is written, and confirm
that a correctly named YAML file appears at the resolved location and that resume reads it.

**Acceptance Scenarios**:

1. **Given** `savepoints.path: /app/savepoints`, **When** savepoints are written, **Then** they
   go to that local directory (equivalent to `target: {type: filesystem, path: /app/savepoints}`).
2. **Given** `savepoints.path: gs://my-bucket/scylla-migrator/savepoints` or
   `s3a://my-bucket/scylla-migrator/savepoints`, **When** savepoints are written, **Then** they
   go to that Hadoop filesystem URI.
3. **Given** `target: {type: gcs, bucket: my-bucket, prefix: scylla-migrator/savepoints,
   projectId: my-gcp-project, credentials: {serviceAccountJsonKeyfile: /path/key.json}}`,
   **When** savepoints are written, **Then** they go to
   `gs://my-bucket/scylla-migrator/savepoints` using service-account keyfile authentication.
4. **Given** `target: {type: s3, bucket: my-bucket, prefix: scylla-migrator/savepoints,
   region: us-east-1}`, **When** savepoints are written, **Then** they go to
   `s3a://my-bucket/scylla-migrator/savepoints` using the given region and optional credentials.
5. **Given** a file or cloud destination without `intervalSeconds`, **When** the job starts,
   **Then** the configuration is rejected.
6. **Given** both `savepoints.path` and `savepoints.target`, **When** the job starts, **Then** the
   configuration is rejected.
7. **Given** a structured `filesystem` target with a `gs://` path, **When** the job starts,
   **Then** it is rejected because the structured filesystem target accepts only local
   (including `file://`) paths.

---

### User Story 3 - Store savepoints in the target ScyllaDB database (Priority: P2)

An operator migrating into ScyllaDB/Cassandra wants savepoints stored in a table in the target
cluster, so no extra storage is needed.

**Why this priority**: Removes the need for a shared filesystem or bucket for CQL targets.

**Independent Test**: Configure `savepoints.target.type: target-table` with an existing keyspace,
run until savepoints are written, check the metadata table rows, interrupt, re-run with the same
configuration and confirm resume.

**Acceptance Scenarios**:

1. **Given** a CQL target and `savepoints: {target: {type: target-table}}` without
   `intervalSeconds`, **When** the job runs, **Then** savepoints are written every 300 seconds to
   table `scylla_migrator_savepoints` in `target.keyspace`, created automatically if absent.
2. **Given** `type: target-database`, **When** the job runs, **Then** it behaves the same as
   `target-table`.
3. **Given** an explicit metadata `keyspace` that does not exist, **When** the job starts,
   **Then** it fails because the migrator does not create the keyspace.
4. **Given** `keyspace: MyKs`, **Then** the keyspace `myks` is used; **given**
   `keyspace: '"MyKs"'`, **Then** case is preserved; **given** a name with characters other than
   ASCII letters, digits or underscores (even quoted), **Then** it is rejected.
5. **Given** no `jobId`, **When** the same original configuration is re-run, **Then** the same
   derived job id is used and the newest usable matching savepoint is resumed.
6. **Given** an explicit `jobId` but a changed source, target or `renames`, **When** the job
   starts, **Then** the rows for that job are not resumed because the configuration identity
   no longer matches.
7. **Given** a non-CQL migration target (e.g. DynamoDB), **When** `target-table` is configured,
   **Then** it is not supported.

---

### User Story 4 - Inspect savepoints manually (Priority: P3)

An operator wants to find the latest savepoint by listing the directory, and to keep savepoints
in shared storage without leaking secrets.

**Why this priority**: Operational convenience and safety; resume works without it.

**Independent Test**: Produce several savepoints, list the directory, check ordering and
redaction.

**Acceptance Scenarios**:

1. **Given** several savepoints, **When** the operator runs `ls <dir> | tail -n 1`, **Then** the
   result is the most recent savepoint, i.e. the one automatic resume selects.
2. **Given** a savepoint file, **When** it is opened, **Then** every secret is `<redacted>`.
3. **Given** a directory containing a legacy `savepoint_1234567890.yaml` file, **When** resuming,
   **Then** the file is read and new savepoints use the two-component name format.

---

### Edge Cases

- MySQL, DynamoDB S3 export and Aerospike sources are not resumable; MySQL requires omitting the
  `savepoints` section and Aerospike ignores it (see sibling specs
  `006-mysql-to-cql-migration`, `005-aerospike-to-cql-migration`).
- A redacted savepoint file can no longer be used directly as a standalone configuration file.
- Missing or failed resume lookups only warn: completed work can be repeated, so target writes
  must be idempotent.
- Target-table `LOCAL_QUORUM` requires a majority of local metadata replicas: failed coordinate
  seeding at startup stops the job; a failed final savepoint write can fail the migration after
  data was written; there is no automatic consistency downgrade.
- `LOCAL_QUORUM` does not strengthen older weaker writes, does not guarantee recovery in another
  datacenter, and does not apply to schema creation or bulk-data writes.
- Two concurrent runs of the same target-table job have no coordination and can overwrite
  progress; operators must enforce one active run per job id.
- Append-only target-table history grows without limit; large `config_yaml` payloads (e.g. long
  processed-file lists) can fail database writes; there is no automatic retention.
- The unsalted SHA-256 digest of a source WHERE filter stored in table payloads lets a reader
  confirm guessed low-entropy filters; restrict access to the metadata table.
- On object stores, the temp-file rename may be a copy/delete provided by the connector, not an
  atomic move.
- When the configuration file itself is loaded from GCS or S3, Spark/Hadoop runtime credentials
  are still needed to read it before savepoint settings are parsed.
- Raw `gs://` paths require the Hadoop GCS connector in the Spark runtime and, unless configured
  in YAML, GCS authentication via Spark/Hadoop runtime configuration on the driver (for both
  writing and resuming); other GCS auth modes (e.g. Dataproc service accounts) are configured
  the same way, not as `savepoints` fields.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support savepoints and resume for CQL (Cassandra/ScyllaDB), DynamoDB
  and Alternator, and Parquet (file-level tracking) sources, and MUST NOT offer resume for MySQL,
  DynamoDB S3 export or Aerospike sources.
  [NEEDS CLARIFICATION: `resume-interrupted-migration.rst` lists only MySQL and DynamoDB S3
  export as non-resumable, while `configuration.rst` ("Aerospike Source") states Aerospike has
  no durable resume and ignores `savepoints` — should the resume page's note also list
  Aerospike?]
- **FR-002**: System MUST periodically write savepoints recording already-migrated progress
  (token ranges, scan segments, or Parquet files) at the configured `intervalSeconds`.
- **FR-003**: System MUST enable automatic resume by default (`savepoints.autoResume: true`): on
  startup it MUST locate the most recent usable savepoint at the configured location and merge
  only its progress into the launch configuration.
- **FR-004**: System MUST take credentials and all non-progress settings from the launch
  configuration, never from a savepoint.
- **FR-005**: System MUST skip savepoint lookup when `savepoints.autoResume: false`.
- **FR-006**: System MUST log a warning and continue from the launch configuration when a resume
  lookup finds nothing or fails.
- **FR-007**: System MUST write savepoints with every secret replaced by `<redacted>`, for all
  backends.
- **FR-008**: System MUST name file-backed savepoints `savepoint_<epochMillis>_<counter>.yaml`,
  with a 13-digit zero-padded millisecond timestamp (monotonic per instance) and a 10-digit
  zero-padded per-instance counter, so lexicographic order equals chronological order.
- **FR-009**: System MUST still read legacy `savepoint_<epochSeconds>.yaml` files and MUST write
  new savepoints in the two-component format.
- **FR-010**: System MUST write each savepoint file to a temporary sibling path and then rename it
  to its final name, never writing partial YAML directly to the final name.
- **FR-011**: System MUST accept exactly one of `savepoints.path` or `savepoints.target` and reject
  configurations specifying both.
- **FR-012**: System MUST treat `savepoints.path` with a local path as shorthand for
  `target: {type: filesystem, path: ...}`, and MUST also accept Hadoop filesystem URIs such as
  `gs://` and `s3a://` in `savepoints.path`.
- **FR-013**: System MUST require a nonempty `target.path` for a structured `filesystem` target
  and accept only local filesystem paths (including `file://`) there.
- **FR-014**: System MUST support a structured `gcs` target (`bucket`, `prefix`, optional
  `projectId`, optional `credentials.serviceAccountJsonKeyfile`) resolving to
  `gs://<bucket>/<prefix>`, using service-account keyfile authentication when the keyfile is set.
- **FR-015**: System MUST support a structured `s3` target (`bucket`, `prefix`, optional `region`
  and `credentials` as in other AWS settings) resolving to `s3a://<bucket>/<prefix>` and applying
  region/credentials to savepoint writes.
- **FR-016**: System MUST require an explicit `intervalSeconds` for file and cloud backends and
  MUST default it to 300 seconds for `target-table`.
- **FR-017**: System MUST support a `target-table` backend (alias `target-database`) only when the
  migration target type is `scylla` or its alias `cassandra`.
- **FR-018**: For `target-table`, System MUST default the metadata keyspace to `target.keyspace`
  (inheriting its exact case) and the table to `scylla_migrator_savepoints`; it MUST create the
  table if absent and MUST NOT create the keyspace.
- **FR-019**: System MUST accept explicit metadata keyspace/table names containing only ASCII
  letters, digits and underscores (even inside quotes), lower-casing unquoted names and preserving
  case for names wrapped in CQL double quotes.
- **FR-020**: System MUST store each target-table savepoint as an append-only row keyed by job id
  with columns `job_id`, `epoch_millis`, `sequence`, `schema_version`, `reason`,
  `migrator_version`, `config_sha256` and `config_yaml` (redacted YAML), newest first.
- **FR-021**: When `jobId` is omitted, System MUST derive a stable job id from the redacted
  configuration identity, including source, target and `renames` and excluding
  `skipTokenRanges`, `skipSegments`, `skipParquetFiles`, `savepoints` and `validation`.
- **FR-022**: System MUST resume target-table rows only when their configuration identity matches
  the launch configuration, even when an explicit `jobId` is set.
- **FR-023**: System MUST read a job's target-table rows newest to oldest in small pages, skip
  unusable rows with warnings, and continue across pages until a usable matching savepoint is
  found or history is exhausted.
- **FR-024**: System MUST use `LOCAL_QUORUM` for target-table metadata writes, resume reads and
  coordinate seed reads, independently of bulk-data consistency, with no automatic downgrade;
  coordinate seeding failure MUST stop startup.
- **FR-025**: System MUST store, in target-table payloads, a SHA-256 digest of any source WHERE
  filter instead of the cleartext filter, and the documentation MUST disclose that the digest is
  unsalted.
- **FR-026**: The documentation MUST state the target-table operating limits: one active run per
  job id enforced by the operator, unbounded append-only history, possible write failures for
  large payloads, and no automatic retention.
- **FR-027**: The documentation MUST state that GCS/S3 access needed to load the configuration
  file and, for raw `gs://` paths, connector availability and authentication must be provided
  through Spark/Hadoop runtime configuration.

### Key Entities

- **Savepoint**: a redacted snapshot of the configuration plus progress (`skipTokenRanges`,
  `skipSegments`, or processed Parquet files) at a point in time.
- **Savepoints configuration**: `path` or `target`, `intervalSeconds`, `autoResume`.
- **Savepoint backend**: `filesystem`, `gcs`, `s3`, raw Hadoop URI via `path`, or `target-table`
  (`target-database`) with `keyspace`, `table`, `jobId`.
- **Savepoint job**: the set of target-table rows sharing a job id, explicit or derived from the
  configuration identity.
- **Configuration identity**: redacted source, target and renames used to derive job ids and
  check that a savepoint belongs to the launched migration.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A resumed run with an unchanged configuration re-migrates no token range, scan
  segment or Parquet file recorded as completed in the selected savepoint.
- **SC-002**: 0 cleartext secrets or WHERE filters appear in any savepoint file or metadata row.
- **SC-003**: For any set of savepoint files, the alphabetically last name is the chronologically
  newest in 100% of cases.
- **SC-004**: 100% of invalid savepoint configurations (both `path` and `target`; missing
  `intervalSeconds` for file/cloud; non-local structured filesystem path; invalid metadata names)
  are rejected at startup.
- **SC-005**: Re-running with the same configuration requires 0 manual edits or file selections
  to resume.
- **SC-006**: No savepoint file is ever observed at its final name with partial content.

## Assumptions

- Target writes are idempotent so that repeated work (failed lookups, at-least-once replays)
  costs only extra load.
- The operator runs one active migration per target-table job and monitors metadata table and
  payload growth.
- The Spark runtime includes the needed Hadoop GCS/S3A connectors and the driver can access the
  configured location for both writing and resuming.
- `skipTokenRanges` and `skipSegments` top-level properties are internal and populated by
  savepoints, not by operators.
- Per-source progress semantics are described in `001-cql-to-cql-migration`,
  `002-parquet-to-cql-migration` and `003-dynamodb-family-migration`; job submission in
  `010-spark-cluster-and-job-submission`.
