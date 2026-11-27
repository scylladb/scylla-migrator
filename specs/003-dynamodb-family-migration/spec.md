# Feature Specification: DynamoDB-Family Migration (DynamoDB, Alternator, DynamoDB S3 Export)

**Feature Branch**: `003-dynamodb-family-migration`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/migrate-from-dynamodb.rst` (whole page); `docs/source/configuration.rst` sections "Source" (type list), "DynamoDB Source", "Alternator Source", "DynamoDB S3 Export Source", "Target" (type list), "DynamoDB Target", "Alternator Target", "AWS Authentication"; source code at commit 06835d0 (`config/SourceSettings.scala`, `config/TargetSettings.scala`, `config/AlternatorSettings.scala`, `config/AWSCredentials.scala`, `config/HostValidation.scala`, `config/MigratorConfig.scala`, `AwsUtils.scala`, `DynamoUtils.scala`, `Migrator.scala`, `alternator/AlternatorMigrator.scala`, `readers/DynamoDB.scala`, `readers/DynamoDBS3Export.scala`, `writers/DynamoDB.scala`, `emr-dynamodb-hadoop` connector sources, tests `DynamoDBSourceSettingParserTest`, `DynamoDBTargetSettingParserTest`, fixtures in `tests/src/test/configurations/`).

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
   with `table`, `endpoint.host: http://<host>` / `endpoint.port` and `streamChanges: false`,
   **When** the migration runs, **Then** every source item is written to the Alternator table and
   the job terminates after the snapshot.
2. **Given** the target table does not exist, **When** the migration starts, **Then** the target
   table is created on the fly with the source key schema, attribute definitions and secondary
   indexes before items are written.
3. **Given** source `credentials.accessKey` / `credentials.secretKey`, **When** the migration
   runs, **Then** those credentials are used to authenticate to DynamoDB.
4. **Given** an Alternator target with `endpoint.host` lacking an `http://` or `https://`
   prefix, **When** the configuration is loaded, **Then** it is rejected at startup with an error
   saying the endpoint host must include a protocol prefix.
5. **Given** a DynamoDB-family target without a `streamChanges` property, **When** the
   configuration is loaded, **Then** it is rejected at startup with an error naming
   `streamChanges` (the property is required; use `false` for a snapshot-only migration).

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
   LocalStack (host given either as a bare hostname or with `http://`/`https://`), **When** the
   migration runs, **Then** the migrator reads from that custom endpoint instead of AWS; a bare
   host is contacted over `http://`.
3. **Given** a `dynamodb` target with `endpoint`, `region` and `credentials`, **When** the
   migration runs, **Then** items are written to that endpoint/region using those credentials.
4. **Given** a `type: dynamodb` source or target that also carries Alternator-only properties
   (e.g. `removeConsumedCapacity`, `datacenter`, or a nested `alternator:` block), **When** the
   configuration is loaded, **Then** it is rejected with a message suggesting `type: alternator`.

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
   given (as a bare host, which is contacted over `http://`, or with an explicit `http://` /
   `https://` prefix) and `usePathStyleAccess: true` is set, **Then** the export is read from that
   endpoint using path-style S3 access.
3. **Given** `usePathStyleAccess` is omitted, **When** the export is read, **Then** the S3
   client's default (virtual-hosted-style) access is used.
4. **Given** a manifest whose `exportType` is not `FULL_EXPORT` or whose `outputFormat` is not
   `DYNAMODB_JSON`, **When** the migration starts, **Then** it fails with "Unsupported export
   type" / "Unsupported export format".
5. **Given** a `tableDescription` attribute type other than `S`, `N`, `B` or a key type other
   than `HASH`, `RANGE`, **When** the configuration is loaded, **Then** it is rejected (e.g.
   "Unknown attribute type number").

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
observe it is rejected at startup.

**Acceptance Scenarios**:

1. **Given** `throughputReadPercent` is omitted, **When** the migration runs, **Then** the job
   attempts to consume half (0.5) of the read-throughput basis.
2. **Given** `throughputReadPercent: 1.0`, **When** the migration runs, **Then** the request rate
   is increased relative to the 0.5 default; a value below 0.5 decreases it.
3. **Given** no `writeThroughput`, **When** the migration runs, **Then** the target table's
   provisioned write capacity units are used as the throughput basis; for an on-demand table the
   value 40000 is used. The same rule (provisioned RCU, or 40000 on-demand) applies to the source
   when `readThroughput` is omitted.
4. **Given** `scanSegments` is omitted, **When** the migration runs, **Then** the number of scan
   segments is computed from the table size (about one segment per 100 MB) and the read
   throughput budget, with a minimum of one segment.
5. **Given** `scanSegments: 0`, `maxMapTasks: -1`, `readThroughput: 0`, `writeThroughput: 0`,
   `throughputReadPercent: 0.09` or `throughputWritePercent: 1.51`, **When** the configuration is
   loaded, **Then** it is rejected at startup with a message naming the property.

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

1. **Given** `credentials.assumeRole.arn` and `region` on a `dynamodb` or `dynamodb-s3-export`
   source or a `dynamodb` target, **When** the migrator connects, **Then** it assumes the role
   using the given access/secret key and accesses the resource with the temporary credentials.
2. **Given** `assumeRole` without `sessionName`, **When** the role is assumed, **Then** the
   session name `scylla-migrator` is used.
3. **Given** `assumeRole` without `region`, **When** the configuration is loaded, **Then** it is
   accepted; the role is assumed using the AWS SDK's default region resolution, and the run fails
   at the first AWS call if no region can be resolved.
4. **Given** `assumeRole` on an `alternator` source or target, **When** the configuration is
   loaded, **Then** it is rejected ("does not support 'assumeRole' in credentials").

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
   client targets nodes in that datacenter/rack; with only `datacenter` set, it targets that
   datacenter.
3. **Given** `rack` without `datacenter`, or a non-positive `maxConnections`,
   `activeRefreshIntervalMs`, `idleRefreshIntervalMs` or `maxItemsPerBatch`, or a negative
   connection timeout/TTL, **When** the configuration is loaded, **Then** it is rejected and all
   problems are reported together.

### Edge Cases

- Alternator `endpoint.host` MUST carry `http://` or `https://`. DynamoDB and DynamoDB S3 export
  endpoints accept either a bare host (contacted over `http://`) or an `http(s)://` URL. In all
  cases the host must not contain a port, path, query, fragment or user-info (the port is a
  separate property, 1–65535).
- The `dynamodb` source table must already exist; a missing source table is an error, whereas a
  missing target table is created on the fly.
- `throughputReadPercent` / `throughputWritePercent` outside the inclusive range 0.1–1.5 are
  rejected at startup.
- The actual read/write rate varies with key distribution; a non-uniform key distribution may not
  reach the configured percentage.
- On-demand DynamoDB tables have no configured capacity; the migrator assumes 40000 as the
  throughput basis (source reads and target writes), which may be higher or lower than the
  account's real on-demand limits.
- `region` is not required with a custom endpoint nor with `assumeRole` at configuration load;
  when omitted, the AWS SDK default region resolution applies and a missing region surfaces as a
  runtime error. With `assumeRole`, the STS call is sent to the same custom `endpoint` (if any)
  as the resource itself.
- Items whose DynamoDB TTL has already expired (TTL enabled on the source table, TTL attribute in
  the past) but which DynamoDB has not yet deleted are skipped by the scan and not copied.
- The S3 export carries no table schema the migrator relies on: `tableDescription` (key schema and
  attribute definitions) MUST be supplied by the operator and must match the export. Only full
  exports in DynamoDB JSON format are supported.
- When the migrator creates a provisioned target table it copies the source's provisioned
  capacity; requesting `billingMode: PROVISIONED` when the source has no provisioned capacity
  (on-demand source, or an S3 export without `provisionedThroughput`) fails table creation.
- Only one `source` and one `target` property may appear in the configuration file.
- `skipInitialSnapshotTransfer` is ignored when `streamChanges` is false; change streaming itself
  is specified in `004-dynamodb-change-streaming`.
- An `alternator` source cannot be combined with change streaming or with a
  `dynamodb-s3-export` target; both are rejected at configuration load.
- Very old or non-standard S3 export layouts are out of scope beyond what `manifest-summary.json`
  describes.

## Requirements *(mandatory)*

### Functional Requirements

**Source/target compatibility**

- **FR-001**: System MUST accept source types `dynamodb` (alias `dynamo`), `alternator` and
  `dynamodb-s3-export` and target types `dynamodb` (alias `dynamo`) and `alternator` for
  DynamoDB-family migrations.
- **FR-002**: System MUST support a `dynamodb` source with a `dynamodb` or `alternator` target.
  A `dynamodb` source MAY also be used with a `dynamodb-s3-export` **target** (export a live table
  to S3); that path is specified in `012-dynamodb-to-s3-export`. An `alternator` source with a
  `dynamodb-s3-export` target MUST be rejected at configuration load.
- **FR-003**: System MUST support an `alternator` source with a `dynamodb` or `alternator` target.
- **FR-004**: System MUST support a `dynamodb-s3-export` source with a `dynamodb` or `alternator`
  target.

**DynamoDB source**

- **FR-005**: A `dynamodb` source MUST require a non-blank `table`, naming an already existing
  table.
- **FR-006**: A `dynamodb` source MUST accept optional `region`, `credentials` and `endpoint`
  (`host`, `port`), where `endpoint` lets the migrator read from DynamoDB-compatible services such
  as DynamoDB Local or LocalStack. `endpoint.host` MAY be a bare host (contacted over `http://`)
  or an `http://`/`https://` URL.
- **FR-007**: `region` MUST be optional for DynamoDB, Alternator and S3 communication, including
  when a custom `endpoint` is configured; when it is omitted, System MUST fall back to the AWS
  SDK's default region resolution (environment, profile, instance metadata). (Docs say region is
  "required if you use a custom endpoint"; code does not enforce this — see Docs vs Code Review.)

**Alternator source**

- **FR-008**: An `alternator` source MUST require `table` and `endpoint` (`host`, `port`), and
  `endpoint.host` MUST include the protocol prefix `http://` or `https://`.
- **FR-009**: An `alternator` source MUST accept optional `region` and `credentials`
  (`accessKey`/`secretKey` only; see FR-029).

**Common DynamoDB/Alternator read tuning**

- **FR-010**: `scanSegments` MUST be optional and, when set, a positive integer giving the number
  of parallel scan segments. When omitted, System MUST compute the number of segments from the
  table size (about one segment per 100 MB) and the read-throughput budget, with a minimum of one.
- **FR-011**: `maxMapTasks` MUST be optional and, when set, a positive integer capping the number
  of parallel scan tasks the segments are distributed over; when omitted, one task per segment is
  used (further limited by the read-throughput budget).
- **FR-012**: `readThroughput` MUST be optional and, when set, a positive integer giving the
  read-capacity basis used for throttling; when omitted, System MUST use the source table's
  provisioned read capacity units, or 40000 for on-demand tables.
- **FR-013**: `throughputReadPercent` MUST be optional, MUST accept values between 0.1 and 1.5
  inclusive (other values rejected at startup), and MUST default to 0.5 (attempt to consume half
  the read-capacity basis); values above 0.5 increase and values below 0.5 decrease the read
  request rate.
- **FR-013a**: When the source table has DynamoDB TTL enabled, System MUST skip items whose TTL
  attribute is already in the past (expired but not yet purged) during the scan.

**DynamoDB S3 export source**

- **FR-014**: A `dynamodb-s3-export` source MUST require `bucket`, `manifestKey` (key of the
  `manifest-summary.json` object in the bucket) and `tableDescription` with `attributeDefinitions`
  (each `name`, `type` ∈ {`S`, `N`, `B`}) and `keySchema` (each `name`, `type` ∈ {`HASH`,
  `RANGE`}); other type values MUST be rejected at load.
- **FR-014a**: `tableDescription` MUST accept optional `billingMode` (`PROVISIONED` or
  `PAY_PER_REQUEST`, default `PAY_PER_REQUEST`) and optional `provisionedThroughput`
  (`readCapacityUnits`, `writeCapacityUnits`), which describe the exported table for target-table
  creation and write-throughput defaults.
- **FR-015**: A `dynamodb-s3-export` source MUST accept optional `endpoint` (`host`, `port`) to
  read from a custom S3-compatible endpoint; `host` MAY be a bare host (contacted over `http://`)
  or an `http://`/`https://` URL.
- **FR-016**: A `dynamodb-s3-export` source MUST accept optional `region` and `credentials`
  (including `assumeRole`).
- **FR-017**: `usePathStyleAccess` MUST be optional; when `true`, S3 path-style access MUST be
  used; when omitted or `false`, the S3 client's default (virtual-hosted-style) access is used.
- **FR-018**: Reading a DynamoDB S3 export MUST NOT read from the live DynamoDB table.
- **FR-018a**: System MUST support only full exports (`exportType: FULL_EXPORT`, or absent) in
  `DYNAMODB_JSON` output format and MUST fail with an explicit "Unsupported export type/format"
  error otherwise; data-file keys listed in the manifest MUST be resolved relative to the
  directory of `manifestKey`.

**DynamoDB target**

- **FR-019**: A `dynamodb` target MUST require a non-blank `table`; if the table does not exist,
  System MUST create it on the fly with the source's key schema, attribute definitions, local
  secondary indexes and global secondary indexes, and wait until it exists before writing.
- **FR-019a**: A DynamoDB-family target MUST accept optional `billingMode` (`PROVISIONED` or
  `PAY_PER_REQUEST`) used only when creating the target table: when omitted or `PROVISIONED` and
  the source has non-zero provisioned read and write capacity, the table (and its GSIs) MUST be
  created provisioned with the source's capacity; `PROVISIONED` with zero source capacity MUST fail
  ("readCapacityUnits and writeCapacityUnits must be set for PROVISIONED billing mode"); otherwise
  the table MUST be created `PAY_PER_REQUEST`.
- **FR-020**: A `dynamodb` target MUST accept optional `endpoint`, `region` and `credentials`
  (including `assumeRole`).
- **FR-021**: `writeThroughput` MUST be optional and, when set, a positive integer; when omitted,
  System MUST use the target table's configured write capacity units for provisioned tables and
  the value 40000 for on-demand tables.
- **FR-022**: `throughputWritePercent` MUST be optional, MUST accept values between 0.1 and 1.5
  inclusive (other values rejected at startup), and MUST default to 0.5 (attempt to consume half
  the table's write capacity); values above 0.5 increase and values below 0.5 decrease the write
  request rate.
- **FR-023**: A DynamoDB-family target MUST require `streamChanges` (`false` for snapshot-only;
  a missing property is rejected at load) and accept optional `skipInitialSnapshotTransfer`
  (default `false`, ignored when `streamChanges` is `false`); streaming semantics, accepted
  shapes and source restrictions are specified in `004-dynamodb-change-streaming`.

**Alternator target**

- **FR-024**: An `alternator` target MUST require `table` and `endpoint`, `endpoint.host` MUST
  include `http://` or `https://`, and a missing table MUST be created on the fly (as FR-019).
- **FR-025**: An `alternator` target MUST accept the same optional `writeThroughput`,
  `throughputWritePercent` (0.1–1.5), `billingMode`, `streamChanges` and
  `skipInitialSnapshotTransfer` properties as a `dynamodb` target.

**Alternator client tuning (source and target)**

- **FR-026**: For `alternator` sources and targets, `removeConsumedCapacity` MUST be optional and
  default to `true`, removing consumed-capacity headers from requests. `dynamodb` sources/targets
  never remove them and MUST reject the property.
- **FR-027**: For `alternator` sources and targets, System MUST accept optional routing/client
  tuning properties `datacenter`, `rack`, `activeRefreshIntervalMs`, `idleRefreshIntervalMs`,
  `compression`, `optimizeHeaders`, `maxConnections`, `connectionMaxIdleTimeMs`,
  `connectionTimeToLiveMs`, `connectionAcquisitionTimeoutMs`, `connectionTimeoutMs` and
  `maxItemsPerBatch` at the top level of the source/target block (documented example values: dc1,
  rack1, 1000, 60000, false, false, 400, 600000, 0, 10000, 15000, 100). When omitted, the
  Alternator client library's defaults apply.
- **FR-027a**: System MUST reject at load: `rack` without `datacenter`; non-positive
  `maxConnections`, `activeRefreshIntervalMs`, `idleRefreshIntervalMs` or `maxItemsPerBatch`;
  negative `connectionMaxIdleTimeMs`, `connectionTimeToLiveMs`, `connectionAcquisitionTimeoutMs`
  or `connectionTimeoutMs`. All errors for one block MUST be reported together.
- **FR-027b**: A nested `alternator:` block MUST be rejected for both `type: alternator` (settings
  belong at the top level) and `type: dynamodb` (suggesting `type: alternator`); `type: dynamodb`
  MUST also reject any Alternator-only property listed in FR-026/FR-027.

**AWS authentication**

- **FR-028**: For DynamoDB, Alternator and S3 communication, System MUST accept
  `credentials.accessKey` and `credentials.secretKey`; when `credentials` is omitted, the AWS SDK
  default credential resolution applies.
- **FR-029**: System MUST accept `credentials.assumeRole.arn` (for `dynamodb`,
  `dynamodb-s3-export` and the `dynamodb` target) to access the resource through an IAM role
  assumed with the given access/secret key; `alternator` sources and targets MUST reject
  `assumeRole` at load.
- **FR-030**: When `assumeRole` is set, `region` SHOULD be configured; System does not reject a
  missing `region` at load (the STS call then uses the AWS SDK default region resolution and is
  sent to the configured `endpoint`, if any).
- **FR-031**: When `assumeRole.sessionName` is omitted, System MUST use the session name
  `scylla-migrator`.
- **FR-032**: Credentials MUST NOT be written in clear text to logs or savepoints
  (constitution principle VI; savepoint behaviour in `008-savepoints-and-resume`).

**Configuration validity**

- **FR-033**: A configuration file MUST contain exactly one `source` and one `target`; a malformed
  or incomplete file MUST fail at startup with a `DecodingFailure` / `ParsingFailure` message
  naming the problem.
- **FR-034**: Endpoints (`endpoint.host`/`endpoint.port`) MUST be validated at load: host
  non-empty, no user-info, path, query, fragment or embedded port; port between 1 and 65535;
  IPv6 hosts accepted (bracketed when rendered).
- **FR-035**: A `dynamodb` or `alternator` source MUST require a `savepoints` section (resume is
  specified in `008-savepoints-and-resume`); a `dynamodb-s3-export` source does not, and System
  MUST log a warning at start that the source does not support savepoints and an interrupted run
  must restart from scratch.
- **FR-036**: Source/target combinations other than those in FR-002–FR-004 (and the other specs'
  pairs) MUST be rejected with an "Unsupported combination of source and target" error before any
  data is read.

### Key Entities

- **DynamoDB source config**: `type: dynamodb` (alias `dynamo`), `table`, optional `endpoint`,
  `region`, `credentials`, `scanSegments`, `readThroughput`, `throughputReadPercent`,
  `maxMapTasks`.
- **Alternator source config**: as DynamoDB source plus mandatory `endpoint` with protocol prefix,
  `removeConsumedCapacity` and top-level Alternator client-tuning properties; no `assumeRole`.
- **DynamoDB S3 export source config**: `type: dynamodb-s3-export`, `bucket`, `manifestKey`,
  `tableDescription`, optional `endpoint`, `region`, `credentials`, `usePathStyleAccess`; may also
  carry `streamSource` (see `004-dynamodb-change-streaming`).
- **Table description**: operator-supplied key schema (`keySchema`: name + HASH/RANGE),
  attribute definitions (`attributeDefinitions`: name + S/N/B), optional `billingMode` and
  `provisionedThroughput`.
- **DynamoDB / Alternator target config**: `type`, `table`, optional `endpoint`, `region`,
  `credentials`, `writeThroughput`, `throughputWritePercent`, `billingMode`, required
  `streamChanges`, optional `skipInitialSnapshotTransfer`; Alternator adds client tuning and
  `removeConsumedCapacity`.
- **AWS credentials**: `accessKey`, `secretKey`, optional `assumeRole` (`arn`, `sessionName`).
- **Endpoint**: `host` (protocol mandatory for Alternator, optional otherwise) and `port`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For each supported pair (dynamodb, alternator, dynamodb-s3-export → dynamodb,
  alternator), 100% of source items (excluding already-expired TTL items) are present and equal in
  the target as confirmed by the Validator (where the Validator supports the pair; see
  `009-migration-validation`).
- **SC-002**: With default throughput settings, the migration consumes on average no more than
  about half of the source table's read capacity and the target table's write capacity (subject
  to key-distribution variance).
- **SC-003**: A migration into a non-existent target table succeeds without any manual table
  creation step.
- **SC-004**: Loading from a DynamoDB S3 export consumes zero read capacity on the live source
  table.
- **SC-005**: 100% of invalid configurations covered by this spec (missing required field,
  missing `streamChanges`, out-of-range throughput percent, non-positive tuning values,
  Alternator endpoint without protocol, `assumeRole` on Alternator, Alternator-only fields on
  `type: dynamodb`) are rejected before any item is read or written.
- **SC-006**: No AWS secret key appears in clear text in migrator logs or savepoints.

## Assumptions

- The operator provides network access and IAM permissions to the source table / S3 bucket and
  the target table; permissions for streaming are covered in `004-dynamodb-change-streaming`.
- The DynamoDB S3 export already exists; the migrator does not trigger exports (writing an
  export-shaped output from a live table is `012-dynamodb-to-s3-export`).
- `tableDescription` supplied by the operator matches the exported table.
- Resume of interrupted DynamoDB/Alternator scans via savepoints is specified in
  `008-savepoints-and-resume`; S3 export sources are not resumable (constitution principle II).
- Column renames for DynamoDB-family migrations are specified in `007-column-renames`.
- Cluster setup and `spark-submit` invocation are specified in
  `010-spark-cluster-and-job-submission`.
- Numeric defaults for the Alternator client tuning properties come from the Alternator client
  library and are not asserted by this spec; the documented example values are recorded only.
- Segment sizing (≈100 MB per segment) and the on-demand capacity basis (40000) come from the
  bundled EMR DynamoDB Hadoop connector (reviewed sources 5.8.0; build uses 5.9.2) and may change
  with connector upgrades.
- Migration of DynamoDB data into CQL tables is out of scope (rejected as an unsupported
  combination).

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | `region` with custom endpoint | configuration.rst "AWS Authentication": region "required if you use a custom endpoint" | No load-time check; `region` applied only if present, else SDK default region chain (`AwsUtils.scala:27-30`; `config/SourceSettings.scala:238-247`) | Code (FR-007) | Say "Optional; recommended with a custom endpoint — falls back to the AWS SDK default region (env/profile)". |
| 2 | `region` with `assumeRole` | "region is mandatory when you use assumeRole" | Not enforced at load; STS client built with configured region **and the resource's custom endpoint** (`AwsUtils.scala:43-65`) | Code (FR-030, US5 AS3) | State that region is not validated but needed at runtime, and that STS calls go to the configured `endpoint`. |
| 3 | `assumeRole` on Alternator | Not mentioned (Alternator Source/Target list `credentials`) | Rejected: "does not support 'assumeRole' in credentials" (`config/AlternatorSettings.scala:89-90`) | Code (FR-029) | Note in Alternator sections that only `accessKey`/`secretKey` are accepted. |
| 4 | S3 export endpoint protocol | "host without protocol" | Accepts bare host (rendered `http://`) or `http(s)://` URL; test fixture uses `http://s3` (`config/HostValidation.scala:74-81`; `tests/src/test/configurations/dynamodb-s3-export-to-alternator-basic.yaml:6`) | Code (FR-015) | Document both forms and that bare hosts use plain HTTP. |
| 5 | Default `scanSegments` size | "segments of 128 MB" | Connector computes `min(tableSize/100 MB, readBudget/MIN_IO_PER_SEGMENT)`, min 1 (emr-dynamodb-hadoop `DynamoDBConstants.MAX_BYTES_PER_SEGMENT`, `AbstractDynamoDBInputFormat.getNumSegments`) | Code (FR-010) | Replace "128 MB" with "about 100 MB, also bounded by read throughput". |
| 6 | `maxMapTasks` meaning/default | "At most how many tasks per Spark executor? default = scanSegments" | Caps the total number of scan tasks (splits); default one per segment, limited by read budget (`AbstractDynamoDBInputFormat.getNumMappers`) | Code (FR-011) | Describe as total scan-task cap, not per executor. |
| 7 | `readThroughput` default | Not documented | Provisioned RCU, or 40000 for on-demand (`readers/DynamoDB.scala:163-175`; `DynamoUtils.scala:817-838`) | Code (FR-012) | Mirror the `writeThroughput` default sentence. |
| 8 | `dynamodb-s3-export` as target | Mentioned in "DynamoDB Source" but absent from target type list | Supported for `dynamodb` source only (`Migrator.scala:100-104`; `config/TargetSettings.scala:376-387`); `alternator` source rejected (`config/MigratorConfig.scala:109-116`) | Code (FR-002, see 012) | Add a target section (covered by spec 012). |
| 9 | Target `billingMode` | Not in configuration.rst (only `config.yaml.example`) | Optional `PROVISIONED`/`PAY_PER_REQUEST` driving table creation (`config/TargetSettings.scala:285`; `DynamoUtils.scala:256-316`) | Code (FR-019a) | Document `billingMode` and the provisioned-capacity copy rule. |
| 10 | `tableDescription.billingMode` / `provisionedThroughput` | Not documented | Optional; default `PAY_PER_REQUEST` (`config/SourceSettings.scala:633-640`; `readers/DynamoDBS3Export.scala:214-229`) | Code (FR-014a) | Add to S3 export source example. |
| 11 | `streamChanges` presence | Shown without "Optional"; stream-changes.rst says `false` is "same as omitting the field historically" | Missing property fails decoding (`config/TargetSettings.scala:280`; `DynamoDBTargetSettingParserTest.scala:720`) | Code (FR-023) | State that `streamChanges` is required on DynamoDB/Alternator targets. |
| 12 | `dynamo` type alias | Not documented | Accepted for source and target (`config/SourceSettings.scala:767`; `config/TargetSettings.scala:248`) | Code (FR-001) | Mention alias (constitution III requires aliases to be documented). |
| 13 | Alternator-only fields on `type: dynamodb`, nested `alternator:` block | Not documented | Rejected with hint to use `type: alternator` (`config/AlternatorSettings.scala:48-73`) | Code (FR-027b) | Add a note. |
| 14 | Alternator tuning validation | Example values only | `rack` requires `datacenter`; positivity / non-negativity checks (`config/AlternatorSettings.scala:96-117`) | Code (FR-027a) | Document constraints. |
| 15 | Expired TTL items | Not documented | Scan filter skips items whose TTL attribute is in the past when TTL is enabled (`readers/DynamoDB.scala:186-192`; connector `ScanReadManager`) | Code (FR-013a) | Document that expired-but-unpurged items are not migrated. |
| 16 | S3 export format support | Not documented | Only `FULL_EXPORT` + `DYNAMODB_JSON`; else error (`readers/DynamoDBS3Export.scala:105-113`) | Code (FR-018a) | Document supported export kinds (no incremental/ION exports). |
| 17 | Target table creation | "created on the fly" | Copies key schema, attribute definitions, LSIs, GSIs; waits for table (`DynamoUtils.scala:227-345`) | Code (FR-019) | Mention indexes are replicated. |
| 18 | Endpoint validation | Not documented | Host/port syntax validated at load (`config/HostValidation.scala:52-71`) | Code (FR-034) | Brief note. |

Verified unchanged: source/target type names, required `table`/`bucket`/`manifestKey`/`tableDescription`,
Alternator `endpoint` with mandatory `http(s)://` prefix, `removeConsumedCapacity` default `true`
for Alternator, throughput-percent range 0.1–1.5 inclusive with 0.5 default, `writeThroughput`
40000 on-demand default, `usePathStyleAccess`, `assumeRole.sessionName` default `scylla-migrator`,
credential redaction in `toString`, and on-the-fly target table creation all match the docs.
