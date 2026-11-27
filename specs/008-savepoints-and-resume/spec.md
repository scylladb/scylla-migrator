# Feature Specification: Savepoints and Resume

**Feature Branch**: `008-savepoints-and-resume`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/resume-interrupted-migration.rst` (all sections: "Automatic resume", "Savepoint file names", "Target-Database Savepoints", "Metadata consistency and availability", "Operating limits"); `docs/source/configuration.rst` — "Overview" (top-level `savepoints`, `skipTokenRanges`, `skipSegments`) and "Savepoints"; `docs/source/configuration.rst` — "Aerospike Source" ("Metadata and restarts"); source code at commit 06835d0 (`config/Savepoints.scala`, `config/MigratorConfig.scala`, `config/SourceSettings.scala` (`supportsSavepoints`), `config/SensitiveKeys.scala`, `config/SparkSecretRedaction.scala`, `SavepointsManager.scala`, `SavepointsResume.scala`, `SavepointStorage.scala`, `SavepointStore.scala`, `PathIO.scala`, `scylla/CqlSavepointsManager.scala`, `scylla/CqlParquetSavepointsManager.scala`, `scylla/CqlTokenRangeAccumulator.scala`, `scylla/ScyllaMigrator.scala`, `alternator/DynamoDbSavepointsManager.scala`, `readers/ParquetSavepointsManager.scala`, `Migrator.scala`).

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
6. **Given** a file-backed savepoint directory whose newest savepoint is corrupt or partially
   written, **When** the job starts, **Then** that file is skipped with a warning and the newest
   readable older savepoint is used.
7. **Given** a running migration, **When** the driver receives SIGINT, SIGTERM or SIGUSR2,
   **Then** a savepoint is written (waiting at most 5 s for an in-progress write) and the process
   exits; a second signal during that write exits immediately without writing.
8. **Given** a migration that completes or fails while writing, **When** it stops, **Then** a
   final savepoint is written before exit.

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
8. **Given** `target: {type: hadoop, ...}` or an unknown target type, **When** the job starts,
   **Then** it is rejected, listing the canonical types `filesystem`, `s3`, `gcs`,
   `target-table`.
9. **Given** a raw `gs://` path and no GCS connector in the Spark runtime, **When** savepoints are
   accessed, **Then** the job fails with an error explaining that the Hadoop GCS connector must
   be included and configured.

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
   **Then** the configuration is rejected at load with "savepoints.target.type: target-table is
   supported only when target.type is 'scylla'".

---

### User Story 4 - Inspect savepoints manually (Priority: P3)

An operator wants to find the latest savepoint by listing the directory, and to keep savepoints
in shared storage without leaking secrets.

**Why this priority**: Operational convenience and safety; resume works without it.

**Independent Test**: Produce several savepoints, list the directory, check ordering and
redaction.

**Acceptance Scenarios**:

1. **Given** several savepoints in the two-component name format, **When** the operator runs
   `ls <dir> | tail -n 1`, **Then** the result is the most recent savepoint, i.e. the one
   automatic resume selects (unless that file is unreadable, in which case resume falls back to
   an older one).
2. **Given** a savepoint file, **When** it is opened, **Then** every secret is `<redacted>`.
3. **Given** a directory containing a legacy `savepoint_1234567890.yaml` file, **When** resuming,
   **Then** the file is read and new savepoints use the two-component name format.

---

### Edge Cases

- MySQL, DynamoDB S3 export and Aerospike sources are not resumable: for all three the
  `savepoints` section is optional and, when present, its settings are ignored with a startup
  warning (no savepoints written, no auto-resume, value checks skipped); the section must still
  be structurally valid to parse (see sibling specs `006-mysql-to-cql-migration`,
  `005-aerospike-to-cql-migration`, `003-dynamodb-family-migration`).
- For resumable sources a missing `savepoints` section is rejected ("Missing required field:
  savepoints"), and a present section must name an explicit nonempty `path` or a `target`.
- File-backed resume does not check that a savepoint belongs to the launched migration: any
  readable savepoint in the configured directory is merged. Use a separate directory per
  migration (target-table savepoints do check configuration identity).
- A directory mixing legacy `savepoint_<epochSeconds>.yaml` files with new-format files does not
  sort chronologically by name (e.g. `savepoint_1700000000.yaml` sorts after
  `savepoint_1600000000000_…`); automatic resume still orders by parsed timestamp.
- Savepoint files whose names have out-of-range coordinates are ignored with a warning.
- A savepoint is written into a `.tmp` sibling first; an in-flight `.tmp` file can appear in a
  directory listing.
- Usernames are not redacted in savepoints or logged configuration; only values of
  secret-looking keys and the source `where` filter are.
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
  atomic move; on a local filesystem that does not support atomic moves the migrator falls back
  to a non-atomic replace and logs a warning.
- When the configuration file itself is loaded from GCS or S3, Spark/Hadoop runtime credentials
  are still needed to read it before savepoint settings are parsed.
- Raw `gs://` paths require the Hadoop GCS connector in the Spark runtime and, unless configured
  in YAML, GCS authentication via Spark/Hadoop runtime configuration on the driver (for both
  writing and resuming); other GCS auth modes (e.g. Dataproc service accounts) are configured
  the same way, not as `savepoints` fields.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support savepoints and resume for CQL (Cassandra/ScyllaDB), DynamoDB
  and Alternator, and Parquet (file-level tracking) sources — including the CQL-to-Parquet
  export and DynamoDB-to-S3-export paths, which track the same source progress — and MUST NOT
  offer resume for MySQL, DynamoDB S3 export or Aerospike sources; for those three it MUST log
  "Source does not support savepoints; any configured savepoints settings are ignored…" at
  startup. (The resume page's note omits Aerospike; see Docs vs Code Review.)
- **FR-002**: System MUST periodically write savepoints recording already-migrated progress
  (token ranges, scan segments, or Parquet files) at the configured `intervalSeconds` (first one
  after one interval), MUST also write a final savepoint when the migration ends (successfully or
  after a write failure) and after each completed write batch where the source is written in
  batches; a failed periodic write MUST be logged and retried at the next interval.
- **FR-003**: System MUST enable automatic resume by default (`savepoints.autoResume: true`): on
  startup it MUST locate the most recent usable savepoint at the configured location (for
  file-backed stores: newest by parsed timestamp/counter, falling back to older ones when a file
  cannot be read or parsed) and merge only its progress into the launch configuration, as a
  union with any progress already present in the launch configuration, logging the number of
  merged token ranges, segments and files.
- **FR-004**: System MUST take credentials and all non-progress settings from the launch
  configuration, never from a savepoint.
- **FR-005**: System MUST skip savepoint lookup when `savepoints.autoResume: false`.
- **FR-006**: System MUST log a warning and continue from the launch configuration when a resume
  lookup finds nothing or fails.
- **FR-007**: System MUST write savepoints with every secret replaced by `<redacted>`, for all
  backends: the string value of every key whose name — ignoring case and `.`, `_`, `-` —
  contains `password`, `secret`, `token`, `credential`, `accesskey`, `apikey`, `privatekey` or
  `keyfile`, and the source `where` filter, MUST be redacted (the same rule applies to the
  configuration logged at startup).
- **FR-008**: System MUST name file-backed savepoints `savepoint_<epochMillis>_<counter>.yaml`,
  with a 13-digit zero-padded millisecond timestamp (monotonic per instance) and a 10-digit
  zero-padded per-instance counter, so lexicographic order equals chronological order.
- **FR-009**: System MUST still read legacy `savepoint_<epochSeconds>.yaml` files and MUST write
  new savepoints in the two-component format.
- **FR-010**: System MUST write each savepoint file to a temporary sibling path (`<name>.tmp`) and
  then rename it to its final name, never writing partial YAML directly to the final name, and
  MUST create the savepoint directory if it does not exist.
- **FR-011**: System MUST accept exactly one of `savepoints.path` or `savepoints.target` and reject
  configurations specifying both.
- **FR-012**: System MUST treat `savepoints.path` with a local path as shorthand for
  `target: {type: filesystem, path: ...}`, and MUST also accept Hadoop filesystem URIs such as
  `gs://` and `s3a://` in `savepoints.path`; a URI whose filesystem connector is missing MUST
  fail with guidance to add and configure the Hadoop connector.
- **FR-013**: System MUST require a nonempty `target.path` for a structured `filesystem` target
  (type alias `path`) and accept only local filesystem paths (including `file://`) there.
- **FR-014**: System MUST support a structured `gcs` target (`bucket`, `prefix`, optional
  `projectId`, optional `credentials.serviceAccountJsonKeyfile`) resolving to
  `gs://<bucket>/<prefix>`, using service-account keyfile authentication when the keyfile is set.
  Type aliases `gcp`, `google-cloud-storage` and `google-gcs`, and `credentials.keyfile` as an
  alias of `serviceAccountJsonKeyfile`, MUST be accepted; `bucket` and the keyfile MUST be
  nonempty; leading/trailing `/` in `prefix` MUST be stripped and an omitted prefix MUST resolve
  to the bucket root.
- **FR-015**: System MUST support a structured `s3` target (`bucket`, `prefix`, optional `region`
  and `credentials` as in other AWS settings) resolving to `s3a://<bucket>/<prefix>` and applying
  region/credentials to savepoint writes and resume reads. Type aliases `aws-s3` and
  `aws-bucket` MUST be accepted; `bucket` MUST be nonempty; prefix normalisation MUST match the
  GCS target.
- **FR-016**: System MUST require an explicit `intervalSeconds` for file and cloud backends and
  MUST default it to 300 seconds for `target-table`; for resumable sources `intervalSeconds`
  MUST be positive.
- **FR-017**: System MUST support a `target-table` backend (alias `target-database`) only when the
  migration target type is `scylla` or its alias `cassandra`, and MUST reject it at configuration
  load otherwise.
- **FR-018**: For `target-table`, System MUST default the metadata keyspace to `target.keyspace`
  (inheriting its exact case) and the table to `scylla_migrator_savepoints`; it MUST create the
  table if absent and MUST NOT create the keyspace.
- **FR-019**: System MUST accept explicit metadata keyspace/table names containing only ASCII
  letters, digits and underscores (even inside quotes), lower-casing unquoted names and preserving
  case for names wrapped in CQL double quotes; empty `keyspace`, `table` or `jobId` values MUST be
  rejected.
- **FR-020**: System MUST store each target-table savepoint as an append-only row keyed by job id
  with columns `job_id`, `epoch_millis`, `sequence`, `schema_version`, `reason`,
  `migrator_version`, `config_sha256` and `config_yaml` (redacted YAML), newest first.
- **FR-021**: When `jobId` is omitted, System MUST derive a stable job id of the form
  `auto-<first 32 hex characters of the SHA-256 identity digest>` from the redacted
  configuration identity, including source, target and `renames` and excluding
  `skipTokenRanges`, `skipSegments`, `skipParquetFiles`, `savepoints` and `validation`; because
  the identity is computed after redaction, changing only a secret (e.g. rotating a password)
  MUST NOT change it.
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
- **FR-028**: System MUST reject `savepoints.target.type: hadoop` (directing users to
  `savepoints.path` or `s3`/`gcs`) and any unknown target type, listing the canonical types
  `filesystem`, `s3`, `gcs`, `target-table`.
- **FR-029**: While a resumable migration runs, System MUST handle SIGINT, SIGTERM and SIGUSR2
  on the driver by writing a savepoint and exiting; it MUST wait at most 5 seconds for an
  in-progress savepoint write before exiting without writing, and a second signal received
  during the signal-triggered write MUST exit immediately.
- **FR-030**: File-backed automatic resume MUST merge progress from the newest readable savepoint
  in the configured location without checking that it was produced by the same configuration;
  operators MUST use a distinct savepoint location per migration.
- **FR-031**: System MUST accept `savepoints.enableParquetFileTracking` (default `true`); when
  `false`, Parquet sources do not record or skip processed files (see
  `002-parquet-to-cql-migration`).
- **FR-032**: On startup System MUST seed the savepoint timestamp/counter from the newest
  existing savepoint in the store (ignoring out-of-range values with a warning) so that new
  savepoints always sort after existing ones even if the driver clock moved backwards.
- **FR-033**: When an `s3` or `gcs` savepoint target carries credentials, System MUST refuse to
  start unless `spark.redaction.regex` covers the resulting Hadoop secret keys; if the operator
  has not set `spark.redaction.regex`, System MUST install a default that redacts
  password/secret/token/credential/access-key/api-key/private-key/key-file keys.
- **FR-034**: The Validator MUST NOT perform automatic resume or write savepoints.

### Key Entities

- **Savepoint**: a redacted snapshot of the configuration plus progress (`skipTokenRanges`,
  `skipSegments`, or processed Parquet files) at a point in time.
- **Savepoints configuration**: `path` or `target`, `intervalSeconds`, `autoResume`,
  `enableParquetFileTracking`.
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
- **SC-003**: For any set of savepoint files in the two-component name format, the alphabetically
  last name is the chronologically newest in 100% of cases.
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

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Non-resumable sources | Resume page: "MySQL and DynamoDB S3-export sources are not resumable" (omits Aerospike); config ref: MySQL "must omit" `savepoints` | Aerospike, MySQL and DynamoDB S3 export all report no savepoint support; the section is optional, ignored with one generic warning, but still parsed (`config/SourceSettings.scala:324,586,624`; `config/MigratorConfig.scala:61-87`; `Migrator.scala:67-73`; `SavepointsResume.scala:33`) | Code (FR-001, edge cases) | Add Aerospike to the resume-page note; say the section is "optional and ignored (warning logged)" for all three. |
| 2 | Signal-triggered savepoints | Not documented | SIGINT/SIGTERM/SIGUSR2 write a savepoint then exit; 5 s lock wait; second signal force-exits (`SavepointsManager.scala:154-206,352`) | Code (FR-029) | Document signals (useful for Kubernetes graceful shutdown). |
| 3 | Final/batch savepoints | Not documented | A "final" savepoint is written on completion or write failure, and after each batch (`scylla/ScyllaMigrator.scala:152-188`) | Code (FR-002) | Mention the final savepoint. |
| 4 | File resume identity | "finds the most recent savepoint" | No configuration-identity check for file/S3/GCS savepoints; the newest readable file in the directory wins, falling back past unreadable ones (`SavepointsResume.scala:56-115`) | Code (FR-003, FR-030) | Warn to use one directory per migration; mention fallback to older savepoints. |
| 5 | `ls \| tail -n 1` | "always yields the most recent savepoint" | True only for two-component names; legacy `savepoint_<seconds>.yaml` names sort incorrectly against new ones, while resume orders by parsed coordinates (`SavepointsManager.scala:374-439`) | Code (SC-003, edge case) | Qualify the claim when legacy files are present. |
| 6 | Target type aliases | Only `filesystem`, `gcs`, `s3`, `target-table`/`target-database` | Also `path`, `gcp`, `google-cloud-storage`, `google-gcs`, `aws-s3`, `aws-bucket`; `credentials.keyfile`; `hadoop` explicitly rejected (`config/Savepoints.scala:89-105,165-211`) | Code (FR-013–FR-015, FR-028) | Document the aliases or mark them deprecated. |
| 7 | `enableParquetFileTracking` | Not documented | Savepoints property, default `true` (`config/Savepoints.scala:15-31,298-299`; `readers/Parquet.scala:59-66`) | Code (FR-031) | Document it in "Savepoints". |
| 8 | Redaction rule | "every secret is `<redacted>`" | Key-name heuristic (password/secret/token/credential/access key/api key/private key/key file) plus `where`; usernames kept (`config/SensitiveKeys.scala`; `config/MigratorConfig.scala:171-190`) | Code (FR-007) | State the rule, including that file savepoints also redact `where`. |
| 9 | `spark.redaction.regex` | Not documented | Migrator installs a default regex when unset and refuses to configure S3/GCS savepoint credentials if the regex misses the secret keys (`config/SparkSecretRedaction.scala`; `SavepointStorage.scala:42,56`; `Migrator.scala:17`) | Code (FR-033) | Document the requirement for custom regexes. |
| 10 | Derived job id format / secret rotation | "stable job id from the redacted configuration identity" | `auto-` + 32 hex chars; identity computed after redaction, so secret changes keep the same job (`SavepointStore.scala:119-143`) | Code (FR-021) | Optional: give the format so operators can query the table. |
| 11 | `intervalSeconds` positivity, empty names | Not stated | Non-positive interval and empty keyspace/table/jobId rejected, only for resumable sources (`config/Savepoints.scala:349-370`; `config/MigratorConfig.scala:70-71`) | Code (FR-016, FR-019) | Document. |
| 12 | Startup seeding / clock skew | "monotonic within a migrator instance" | Counters are seeded from the newest existing savepoint so ordering survives restarts with clock skew (`SavepointsManager.scala:101-152`; `SavepointStore.scala:229-258,310-334`) | Code (FR-032) | Optional: say monotonic across restarts sharing a store. |

Verified unchanged: automatic resume default and `autoResume: false`, credentials taken from the
launch configuration, warning-and-continue on failed lookups, file name format and legacy-name
reading, temp-file-then-rename, exactly one of `path`/`target`, raw Hadoop URIs in `path`,
structured filesystem local-only rule, GCS/S3 path resolution and credential application,
`intervalSeconds` default of 300 s for `target-table`, metadata keyspace/table defaults and
naming rules, table schema and append-only storage, identity-matched paged (16-row pages) resume
of table rows, `LOCAL_QUORUM` for metadata reads/writes and seeding, and the unsalted WHERE
digest in table payloads all match the code.
