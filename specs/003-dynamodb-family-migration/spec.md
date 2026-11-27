# Feature Specification: DynamoDB-Family Migration (DynamoDB, Alternator, DynamoDB S3 Export)

**Feature Branch**: `003-dynamodb-family-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/migrate-from-dynamodb.rst` (whole page); `docs/source/configuration.rst` sections "Source" (type list), "DynamoDB Source", "Alternator Source", "DynamoDB S3 Export Source", "Target" (type list), "DynamoDB Target", "Alternator Target", "AWS Authentication".

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Copy a DynamoDB table to ScyllaDB Alternator (Priority: P1)

An operator moving off AWS DynamoDB configures a `dynamodb` source (table + region, optionally
credentials) and an `alternator` target (table + `http(s)://` endpoint) and runs the migrator
once to copy a snapshot of every item into Alternator. The target table is created on the fly if
it does not exist.

**Why this priority**: This is the primary reason the DynamoDB path exists: migrating DynamoDB
workloads to ScyllaDB Alternator.

**Independent Test**: Create a DynamoDB table with known items, run the migrator with a
`dynamodb` source and an `alternator` target whose table does not exist, then confirm the target
table exists and contains exactly the same items (e.g. with the Validator, see
`009-migration-validation`).

**Acceptance Scenarios**:

1. **Given** a source `type: dynamodb` with `table` and `region`, and a target `type: alternator`
   with `table` and `endpoint.host: http://<host>` / `endpoint.port`, **When** the migration
   runs, **Then** every source item is written to the Alternator table and the job terminates
   after the snapshot (`streamChanges: false`).
2. **Given** the target table does not exist, **When** the migration starts, **Then** the target
   table is created on the fly before items are written.
3. **Given** source `credentials.accessKey` / `credentials.secretKey`, **When** the migration
   runs, **Then** those credentials are used to authenticate to DynamoDB.
4. **Given** an Alternator target with `endpoint.host` lacking an `http://` or `https://`
   prefix, **When** the configuration is used, **Then** it is not a valid Alternator endpoint
   (the prefix is mandatory).

---

### User Story 2 - Copy between DynamoDB-compatible tables (DynamoDB/Alternator to DynamoDB/Alternator) (Priority: P2)

An operator copies a table from Alternator to DynamoDB, from Alternator to Alternator, or from
DynamoDB to DynamoDB (e.g. across accounts or regions), including against custom
DynamoDB-compatible endpoints such as DynamoDB Local or LocalStack.

**Why this priority**: Covers the remaining documented source/target combinations and the
test/dev workflow with local endpoints.

**Independent Test**: For each pair (alternator→dynamodb, alternator→alternator,
dynamodb→dynamodb), seed a source table, run the migrator, and confirm item equality.

**Acceptance Scenarios**:

1. **Given** a source `type: alternator` with `endpoint.host: http://<host>` and `port`,
   **When** the migration runs to a `dynamodb` or `alternator` target, **Then** all items are
   copied.
2. **Given** a source `type: dynamodb` with an `endpoint` pointing at DynamoDB Local or
   LocalStack, **When** the migration runs, **Then** the migrator reads from that custom endpoint
   instead of AWS.
3. **Given** a `dynamodb` target with `endpoint`, `region` and `credentials`, **When** the
   migration runs, **Then** items are written to that endpoint/region using those credentials.

---

### User Story 3 - Load a DynamoDB S3 export into DynamoDB or Alternator (Priority: P2)

An operator who has already exported a DynamoDB table to S3 points the migrator at the export's
`manifest-summary.json` and supplies the table's key schema and attribute definitions, so the
data is loaded without putting read load on the live table.

**Why this priority**: Avoids consuming read capacity on the production table and supports
exports produced independently of the migrator.

**Independent Test**: Produce (or fixture) an S3 export with a known manifest, configure
`type: dynamodb-s3-export` with `bucket`, `manifestKey` and `tableDescription`, run the migrator
to an Alternator target, and confirm every exported item is present.

**Acceptance Scenarios**:

1. **Given** a source `type: dynamodb-s3-export` with `bucket: my-dynamodb-exports`,
   `manifestKey: my-path/AWSDynamoDB/01234567890123-1234abcd/manifest-summary.json` and a
   `tableDescription` declaring attribute `id` of type `S` as `HASH` key, **When** the migration
   runs, **Then** every item in the export is written to the target.
2. **Given** an S3-compatible store reachable at a custom endpoint, **When** `endpoint.host` is
   given **without** a protocol and `usePathStyleAccess: true` is set, **Then** the export is
   read from that endpoint using path-style S3 access.
3. **Given** `usePathStyleAccess` is omitted, **When** the export is read, **Then**
   virtual-hosted-style access is used (default `false`).

---

### User Story 4 - Control read and write throughput (Priority: P3)

An operator tunes how hard the migrator hits the source and target so that the migration does
not saturate production capacity: `scanSegments` and `maxMapTasks` for read parallelism,
`readThroughput` / `throughputReadPercent` on the source, `writeThroughput` /
`throughputWritePercent` on the target.

**Why this priority**: Required for safe production migrations (constitution principle V) but
the defaults already work for most cases.

**Independent Test**: Run the same migration with `throughputReadPercent: 0.1` and `1.5` and
observe a lower vs higher consumed read rate; set `throughputWritePercent` outside 0.1–1.5 and
observe it is not accepted.

**Acceptance Scenarios**:

1. **Given** `throughputReadPercent` is omitted, **When** the migration runs, **Then** the job
   attempts to consume half (0.5) of the table's read capacity.
2. **Given** `throughputReadPercent: 1.0`, **When** the migration runs, **Then** the request rate
   is increased relative to the 0.5 default; a value below 0.5 decreases it.
3. **Given** a provisioned DynamoDB target and no `writeThroughput`, **When** the migration runs,
   **Then** the table's configured write capacity units are used as the throughput basis; for an
   on-demand table the value 40000 is used.
4. **Given** `scanSegments` is omitted, **When** the migration runs, **Then** the source data is
   split into chunks of about 128 MB processed in parallel, and `maxMapTasks` defaults to the
   value of `scanSegments`.

---

### User Story 5 - Authenticate with AssumeRole (Priority: P3)

An operator whose user has been granted access to the DynamoDB table or S3 bucket through an
IAM role configures `credentials.assumeRole.arn` (and optionally `sessionName`) together with a
`region`.

**Why this priority**: Needed for cross-account migrations; not needed for single-account ones.

**Independent Test**: Configure a source with `accessKey`/`secretKey` of a user and
`assumeRole.arn` of a role that grants table access, plus `region`; confirm the migration reads
the table, and that the session name is `scylla-migrator` when `sessionName` is omitted.

**Acceptance Scenarios**:

1. **Given** `credentials.assumeRole.arn` and `region`, **When** the migrator connects, **Then**
   it assumes the role using the given access/secret key and accesses the resource as the role.
2. **Given** `assumeRole` without `sessionName`, **When** the role is assumed, **Then** the
   session name `scylla-migrator` is used.
3. **Given** `assumeRole` without `region`, **When** the configuration is used, **Then** it is
   invalid because `region` is mandatory with `assumeRole`.

---

### User Story 6 - Tune the Alternator client (Priority: P4)

An operator targeting (or reading from) Alternator tunes routing and HTTP client behaviour:
`datacenter`, `rack`, refresh intervals, compression, header optimisation, connection-pool
limits and timeouts, `maxItemsPerBatch`, and whether consumed-capacity headers are removed.

**Why this priority**: Performance/topology optimisation; defaults are sufficient for a basic
migration.

**Independent Test**: Run an Alternator migration with and without `datacenter`/`rack` set and
confirm requests are routed to the chosen datacenter/rack; confirm consumed-capacity headers are
removed by default.

**Acceptance Scenarios**:

1. **Given** an `alternator` target or source without `removeConsumedCapacity`, **When** requests
   are sent, **Then** consumed-capacity headers are removed (default `true` for Alternator).
2. **Given** `datacenter: dc1` and `rack: rack1`, **When** requests are routed, **Then** the
   client targets nodes in that datacenter/rack.

### Edge Cases

- Alternator `endpoint.host` MUST carry `http://` or `https://`; DynamoDB S3 export
  `endpoint.host` MUST NOT carry a protocol. Mixing these conventions produces an invalid
  endpoint.
- The `dynamodb` source table "must already exist"; a missing source table is an error, whereas a
  missing target table is created on the fly.
- `throughputReadPercent` / `throughputWritePercent` outside the inclusive range 0.1–1.5 are not
  valid.
- The actual read/write rate varies with key distribution; a non-uniform key distribution may not
  reach the configured percentage.
- On-demand DynamoDB targets have no configured WCU; the migrator assumes 40000 as the throughput
  basis, which may be higher or lower than the account's real on-demand limits.
- `region` is mandatory when `assumeRole` is used, and per the AWS Authentication section
  required when a custom endpoint is used (see FR-007 clarification).
- The S3 export carries no table schema the migrator relies on: `tableDescription` (key schema and
  attribute definitions) MUST be supplied by the operator and must match the export.
- Only one `source` and one `target` property may appear in the configuration file.
- `skipInitialSnapshotTransfer` is ignored when `streamChanges` is false; change streaming itself
  is specified in `004-dynamodb-change-streaming`.
- Very old or non-standard S3 export layouts are out of scope beyond what `manifest-summary.json`
  describes.

## Requirements *(mandatory)*

### Functional Requirements

**Source/target compatibility**

- **FR-001**: System MUST accept source types `dynamodb`, `alternator` and `dynamodb-s3-export`
  and target types `dynamodb` and `alternator` for DynamoDB-family migrations.
- **FR-002**: System MUST support a `dynamodb` source with a `dynamodb` or `alternator` target.
  [NEEDS CLARIFICATION: configuration.rst ("DynamoDB Source") states a `dynamodb` source can also
  be used with a `dynamodb-s3-export` target, but `dynamodb-s3-export` is not in the documented
  list of target types and has no target documentation; is exporting to S3 a supported target or
  is this a documentation error?]
- **FR-003**: System MUST support an `alternator` source with a `dynamodb` or `alternator` target.
- **FR-004**: System MUST support a `dynamodb-s3-export` source with a `dynamodb` or `alternator`
  target.

**DynamoDB source**

- **FR-005**: A `dynamodb` source MUST require `table`, naming an already existing table.
- **FR-006**: A `dynamodb` source MUST accept optional `region`, `credentials` and `endpoint`
  (`host`, `port`), where `endpoint` lets the migrator read from DynamoDB-compatible services such
  as DynamoDB Local or LocalStack.
- **FR-007**: When a custom `endpoint` is configured for AWS communication, System MUST require a
  `region` [NEEDS CLARIFICATION: the "AWS Authentication" section says region is "required if you
  use a custom endpoint", but the documented minimal `dynamodb` source example with a custom
  endpoint omits `region`; is `region` actually mandatory with a custom endpoint, or defaulted?].

**Alternator source**

- **FR-008**: An `alternator` source MUST require `table` and `endpoint` (`host`, `port`), and
  `endpoint.host` MUST include the protocol prefix `http://` or `https://`.
- **FR-009**: An `alternator` source MUST accept optional `region` and `credentials`.

**Common DynamoDB/Alternator read tuning**

- **FR-010**: `scanSegments` MUST be optional; when omitted, System MUST split the source data into
  segments of about 128 MB processed in parallel by Spark executors.
- **FR-011**: `maxMapTasks` MUST be optional and cap the number of tasks per Spark executor;
  when omitted it MUST default to the value of `scanSegments`.
- **FR-012**: `readThroughput` MUST be optional and set the read-capacity basis used for throttling.
- **FR-013**: `throughputReadPercent` MUST be optional, MUST accept values between 0.1 and 1.5
  inclusive, and MUST default to 0.5 (attempt to consume half the table's read capacity); values
  above 0.5 increase and values below 0.5 decrease the read request rate.

**DynamoDB S3 export source**

- **FR-014**: A `dynamodb-s3-export` source MUST require `bucket`, `manifestKey` (key of the
  `manifest-summary.json` object in the bucket) and `tableDescription` with `attributeDefinitions`
  (each `name`, `type`) and `keySchema` (each `name`, `type`).
- **FR-015**: A `dynamodb-s3-export` source MUST accept optional `endpoint` whose `host` is given
  without a protocol, plus `port`, to read from a custom S3-compatible endpoint.
- **FR-016**: A `dynamodb-s3-export` source MUST accept optional `region` and `credentials`.
- **FR-017**: `usePathStyleAccess` MUST be optional and default to `false`; when `true`, S3
  path-style access MUST be used.
- **FR-018**: Reading a DynamoDB S3 export MUST NOT read from the live DynamoDB table.

**DynamoDB target**

- **FR-019**: A `dynamodb` target MUST require `table`; if the table does not exist, System MUST
  create it on the fly.
- **FR-020**: A `dynamodb` target MUST accept optional `endpoint`, `region` and `credentials`.
- **FR-021**: `writeThroughput` MUST be optional; when omitted, System MUST use the table's
  configured write capacity units for provisioned tables and the value 40000 for on-demand tables.
- **FR-022**: `throughputWritePercent` MUST be optional, MUST accept values between 0.1 and 1.5
  inclusive, and MUST default to 0.5 (attempt to consume half the table's write capacity); values
  above 0.5 increase and values below 0.5 decrease the write request rate.
- **FR-023**: A DynamoDB-family target MUST accept `streamChanges` (snapshot-only when `false`)
  and optional `skipInitialSnapshotTransfer` (default `false`, ignored when `streamChanges` is
  `false`); streaming semantics are specified in `004-dynamodb-change-streaming`.

**Alternator target**

- **FR-024**: An `alternator` target MUST require `table` and `endpoint`, `endpoint.host` MUST
  include `http://` or `https://`, and a missing table MUST be created on the fly.
- **FR-025**: An `alternator` target MUST accept the same optional `writeThroughput`,
  `throughputWritePercent` (0.1–1.5), `streamChanges` and `skipInitialSnapshotTransfer`
  properties as a `dynamodb` target.

**Alternator client tuning (source and target)**

- **FR-026**: For `alternator` sources and targets, `removeConsumedCapacity` MUST be optional and
  default to `true`, removing consumed-capacity headers from requests.
- **FR-027**: For `alternator` sources and targets, System MUST accept optional routing/client
  tuning properties `datacenter`, `rack`, `activeRefreshIntervalMs`, `idleRefreshIntervalMs`,
  `compression`, `optimizeHeaders`, `maxConnections`, `connectionMaxIdleTimeMs`,
  `connectionTimeToLiveMs`, `connectionAcquisitionTimeoutMs`, `connectionTimeoutMs` and
  `maxItemsPerBatch` (documented example values: dc1, rack1, 1000, 60000, false, false, 400,
  600000, 0, 10000, 15000, 100).

**AWS authentication**

- **FR-028**: For DynamoDB, Alternator and S3 communication, System MUST accept
  `credentials.accessKey` and `credentials.secretKey`.
- **FR-029**: System MUST accept `credentials.assumeRole.arn` to access the resource through an
  IAM role assumed with the given access/secret key.
- **FR-030**: When `assumeRole` is set, `region` MUST be mandatory.
- **FR-031**: When `assumeRole.sessionName` is omitted, System MUST use the session name
  `scylla-migrator`.
- **FR-032**: Credentials MUST NOT be written in clear text to logs or savepoints
  (constitution principle VI; savepoint behaviour in `008-savepoints-and-resume`).

**Configuration validity**

- **FR-033**: A configuration file MUST contain exactly one `source` and one `target`; a malformed
  or incomplete file MUST fail at startup with a `DecodingFailure` / `ParsingFailure` message
  naming the problem.

### Key Entities

- **DynamoDB source config**: `type: dynamodb`, `table`, optional `endpoint`, `region`,
  `credentials`, `scanSegments`, `readThroughput`, `throughputReadPercent`, `maxMapTasks`.
- **Alternator source config**: as DynamoDB source plus mandatory `endpoint` with protocol prefix,
  `removeConsumedCapacity` and Alternator client-tuning properties.
- **DynamoDB S3 export source config**: `type: dynamodb-s3-export`, `bucket`, `manifestKey`,
  `tableDescription`, optional protocol-less `endpoint`, `region`, `credentials`,
  `usePathStyleAccess`; may also carry `streamSource` (see `004-dynamodb-change-streaming`).
- **Table description**: operator-supplied key schema (`keySchema`: name + HASH/RANGE) and
  attribute definitions (`attributeDefinitions`: name + type such as `S`).
- **DynamoDB / Alternator target config**: `type`, `table`, optional `endpoint`, `region`,
  `credentials`, `writeThroughput`, `throughputWritePercent`, `streamChanges`,
  `skipInitialSnapshotTransfer`; Alternator adds client tuning and `removeConsumedCapacity`.
- **AWS credentials**: `accessKey`, `secretKey`, optional `assumeRole` (`arn`, `sessionName`).
- **Endpoint**: `host` (with protocol for Alternator, without for S3 export) and `port`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For each supported pair (dynamodb, alternator, dynamodb-s3-export → dynamodb,
  alternator), 100% of source items are present and equal in the target as confirmed by the
  Validator (where the Validator supports the pair; see `009-migration-validation`).
- **SC-002**: With default throughput settings, the migration consumes on average no more than
  about half of the source table's read capacity and the target table's write capacity (subject
  to key-distribution variance).
- **SC-003**: A migration into a non-existent target table succeeds without any manual table
  creation step.
- **SC-004**: Loading from a DynamoDB S3 export consumes zero read capacity on the live source
  table.
- **SC-005**: 100% of invalid configurations covered by this spec (missing required field,
  `assumeRole` without `region`, out-of-range throughput percent) are rejected before any item is
  read or written.
- **SC-006**: No AWS secret key appears in clear text in migrator logs or savepoints.

## Assumptions

- The operator provides network access and IAM permissions to the source table / S3 bucket and
  the target table; permissions for streaming are covered in `004-dynamodb-change-streaming`.
- The DynamoDB S3 export already exists; the migrator does not trigger exports.
- `tableDescription` supplied by the operator matches the exported table.
- Resume of interrupted DynamoDB/Alternator scans via savepoints is specified in
  `008-savepoints-and-resume`; S3 export sources are not resumable (constitution principle II).
- Column renames for DynamoDB-family migrations are specified in `007-column-renames`.
- Cluster setup and `spark-submit` invocation are specified in
  `010-spark-cluster-and-job-submission`.
- Exact numeric defaults for the Alternator client tuning properties and `readThroughput` are not
  stated in the docs beyond the example values; the example values are recorded, not asserted as
  defaults.
- Migration of DynamoDB data into CQL tables is out of scope (not documented).
