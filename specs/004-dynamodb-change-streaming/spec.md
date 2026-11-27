# Feature Specification: DynamoDB Change Streaming (DynamoDB Streams and Kinesis Data Streams)

**Feature Branch**: `004-dynamodb-change-streaming`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/stream-changes.rst` (whole page: "DynamoDB Streams mode (legacy, 24h retention)", "Kinesis Data Streams mode (up to 1y retention)", "When to prefer Kinesis over DynamoDB Streams", "Chaining S3-export with streamChanges"); `docs/source/configuration.rst` sections "DynamoDB Target" and "Alternator Target" (`streamChanges`, `skipInitialSnapshotTransfer`); `docs/source/migrate-from-dynamodb.rst` ("Writing to DynamoDB").

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Snapshot then replicate live changes via DynamoDB Streams (Priority: P1)

An operator migrating a live DynamoDB table to Alternator or DynamoDB sets
`streamChanges: true` on the target. The migrator copies an initial snapshot, then keeps running
and replicates every change applied to the source table, until the operator stops it.

**Why this priority**: Enables near-zero-downtime cut-over for tables whose snapshot completes in
under 24 hours, with no extra infrastructure.

**Independent Test**: Start a migration with `streamChanges: true` from a DynamoDB table to an
Alternator table; write, update and delete items on the source during and after the snapshot;
confirm the target converges to the source; stop with SIGINT.

**Acceptance Scenarios**:

1. **Given** a `dynamodb` source and an `alternator` (or `dynamodb`) target with
   `streamChanges: true`, **When** the migrator runs, **Then** it transfers the snapshot and then
   continuously replicates changes instead of terminating.
2. **Given** the migrator is streaming, **When** the operator presses Ctrl+C or sends SIGINT to the
   `spark-submit` process, **Then** the migrator stops; the DynamoDB stream it enabled is **not**
   deleted and must be removed manually.
3. **Given** `streamChanges: true` and `skipInitialSnapshotTransfer: true`, **When** the migrator
   runs, **Then** it skips the snapshot and only replicates changes.
4. **Given** `streamChanges: false` and `skipInitialSnapshotTransfer: true`, **When** the migrator
   runs, **Then** `skipInitialSnapshotTransfer` is ignored and a snapshot-only migration runs.
5. **Given** the snapshot takes longer than 24 hours, **When** streaming starts, **Then** changes
   older than the stream's 24-hour retention may be lost (documented limitation).

---

### User Story 2 - Replicate via Kinesis Data Streams for long snapshots (Priority: P1)

An operator with a large table whose snapshot may exceed 24 hours pre-creates a Kinesis Data
Stream and configures `streamChanges: {type: kinesis, streamArn: ...}`. The migrator enables the
DynamoDB→Kinesis streaming destination, captures a start instant just before the snapshot, copies
the snapshot, then replays changes from that instant (`AT_TIMESTAMP`).

**Why this priority**: Removes the 24-hour data-loss window for large migrations (constitution
principle I).

**Independent Test**: Pre-create a Kinesis stream in the source region, run a migration with
`type: kinesis` and a valid `streamArn`, write to the source during the snapshot, and confirm
those writes reach the target after the snapshot.

**Acceptance Scenarios**:

1. **Given** `streamChanges.type: kinesis` and `streamArn:
   arn:aws:kinesis:us-east-1:123456789012:stream/my-dynamo-stream` with source region
   `us-east-1`, **When** the migrator starts, **Then** it enables the Kinesis streaming
   destination on the source table, waits until it is ACTIVE, and captures the replay start
   instant.
2. **Given** `initialTimestamp` is omitted (and no S3 export `exportTime` applies), **When** the
   destination reaches ACTIVE, **Then** the replay start instant is the driver's current time at
   that moment, with no safety margin.
3. **Given** `initialTimestamp: 2026-04-13T08:00:00Z`, **When** the migrator starts, **Then**
   replay starts at that instant regardless of the driver clock.
4. **Given** `initialTimestamp` more than 5 minutes in the future, **When** the configuration is
   loaded, **Then** it is rejected.
5. **Given** a bare stream name, a malformed ARN, a partition other than `aws`, `aws-cn` or
   `aws-us-gov`, or an ARN region different from `source.region`, **When** the configuration is
   loaded, **Then** it is rejected at startup.
6. **Given** `type: kinesis-data-streams`, **When** the configuration is loaded, **Then** it is
   treated exactly like `type: kinesis`.
7. **Given** the migrator is stopped, **When** it shuts down, **Then** it does not disable the
   DynamoDB→Kinesis destination and does not delete the Kinesis stream, so a later restart can
   resume without data loss.

---

### User Story 3 - Seed from an S3 export, then stream from the live table (Priority: P2)

An operator uses an existing DynamoDB S3 export as the initial snapshot (no read load on the live
table) and adds a `streamSource` block pointing at the still-live source table, so changes made
after the export are replayed from the export's `exportTime`.

**Why this priority**: Combines the cheapest snapshot path with lossless catch-up.

**Independent Test**: Configure a `dynamodb-s3-export` source with `streamSource` and a Kinesis
`streamChanges` target without `initialTimestamp`; write to the live table after the export time;
confirm those writes reach the target.

**Acceptance Scenarios**:

1. **Given** a `dynamodb-s3-export` source with `streamSource: {type: dynamodb, table: MyLiveTable,
   region: us-east-1}` and a target with `streamChanges`, **When** the migrator runs, **Then** it
   applies the export as the snapshot and then enables the configured streaming mode on the live
   table named in `streamSource`.
2. **Given** Kinesis mode without `initialTimestamp`, **When** replay starts, **Then** the start
   instant is the manifest's `exportTime`, or `startTime` when `exportTime` is absent.
3. **Given** the manifest has neither `exportTime` nor `startTime`, **When** replay starts,
   **Then** the migrator logs a warning and falls back to the driver's current time (writes between
   the export and that instant are lost unless `initialTimestamp` is set).
4. **Given** a `dynamodb-s3-export` source with `streamChanges` requested but no `streamSource`,
   **When** the migrator starts, **Then** it fails fast instead of silently skipping streaming.

---

### User Story 4 - Fan out one source to several targets (Priority: P3)

An operator replicates the same source table to two targets concurrently using Kinesis mode, each
migrator with its own lease table (default name includes a hash of the stream ARN, or an explicit
`appName`).

**Why this priority**: Less common topology; DynamoDB Streams mode cannot do it safely.

**Independent Test**: Run two migrators with Kinesis mode against the same source but different
`streamArn` values (or different `appName`), and confirm each target receives all changes.

**Acceptance Scenarios**:

1. **Given** two migrators on the same source with different `streamArn`, **When** both run,
   **Then** each uses its own default lease table `migrator_<source-table>_<arn-hash-8>` and each
   target receives every change.
2. **Given** two migrators sharing the same `appName`, **When** both run, **Then** they share one
   lease table and the shards are split between them (each sees only part of the changes).
3. **Given** two DynamoDB-Streams-mode migrators on the same source table, **When** both run,
   **Then** they share the lease table `migrator_<source-table>` and each target sees only part of
   the changes (documented limitation; avoid this setup).

---

### User Story 5 - Upgrade an existing DynamoDB Streams deployment (Priority: P3)

An operator upgrading from a version that used `migrator_<source-table>_<millis>` lease tables
understands the upgrade impact of the now-deterministic `migrator_<source-table>` lease name.

**Why this priority**: Operational safety during upgrades only.

**Independent Test**: With an old `_<millis>` lease table present, run the new version and confirm
it creates/uses `migrator_<source-table>` and replays from `TrimHorizon`.

**Acceptance Scenarios**:

1. **Given** an existing `migrator_<source-table>_<millis>` lease table, **When** the upgraded
   migrator first runs, **Then** it does not find it, uses `migrator_<source-table>`, and replays
   the stream from `TrimHorizon`; the old lease table is left orphaned (and still billed) for
   manual deletion.

### Edge Cases

- DynamoDB Streams retention is 24 hours: a snapshot longer than that can lose early writes.
- Driver clock **ahead** of AWS by Δ in Kinesis default mode: writes in the first Δ seconds are
  silently lost; driver clock **behind** by Δ: Δ seconds are replayed (harmless, idempotent
  writes cost extra load only).
- No default safety margin is applied to the captured instant; hosts with weak clocks (laptops,
  fresh containers, resumed VMs) should set `initialTimestamp` explicitly, biased earlier.
- The S3-export `exportTime` default is AWS-generated and does not depend on the driver clock.
- Old export manifests lacking `exportTime`/`startTime` fall back to driver time with a warning.
- The Kinesis stream is not created by the migrator; a missing stream or missing IAM permissions
  prevent streaming.
- Streams and destinations are never cleaned up by the migrator on shutdown (DynamoDB stream,
  Kinesis destination, Kinesis stream, lease tables) — the operator removes them.
- The migrator never terminates on its own in streaming mode; it must be interrupted.
- Upgrading from `_<millis>` lease names causes a full replay from `TrimHorizon`; perform it in a
  quiet window.
- DynamoDB Streams mode offers no `appName` override; concurrent migrators on one source table
  split shards between them.
- An `initialTimestamp` more than 5 minutes in the future is rejected as a likely typo or clock
  skew.

## Requirements *(mandatory)*

### Functional Requirements

**Applicability and configuration shapes**

- **FR-001**: System MUST support change streaming only when reading from DynamoDB (directly, or
  via a `dynamodb-s3-export` source with `streamSource`) and writing to an `alternator` or
  `dynamodb` target.
- **FR-002**: The target property `streamChanges` MUST accept three shapes: `false` (snapshot
  only), `true` (DynamoDB Streams mode) and an object with `type: kinesis` or its alias
  `type: kinesis-data-streams` (Kinesis Data Streams mode).
- **FR-003**: Existing configuration files using boolean `streamChanges` MUST keep working
  unchanged.
- **FR-004**: `skipInitialSnapshotTransfer` MUST be optional, default `false`, and, when `true`
  with streaming enabled, MUST skip the snapshot and only replicate changes; it MUST be ignored
  when `streamChanges` is `false`.
- **FR-005**: In both streaming modes, System MUST transfer the initial snapshot (unless skipped)
  and then keep replicating changes until interrupted by Ctrl+C / SIGINT.

**DynamoDB Streams mode**

- **FR-006**: With `streamChanges: true`, System MUST enable and read the source table's DynamoDB
  Stream.
- **FR-007**: System MUST NOT delete the DynamoDB stream when stopped.
- **FR-008**: System MUST use the deterministic lease (checkpoint) table name
  `migrator_<source-table>`.
- **FR-009**: When no lease table with the deterministic name exists, System MUST start replay
  from `TrimHorizon`.
- **FR-010**: DynamoDB Streams mode MUST NOT accept an `appName` override (current limitation);
  concurrent migrators on one source table share the lease table.

**Kinesis Data Streams mode**

- **FR-011**: Kinesis mode MUST require `streamArn`, the full ARN of a pre-created Kinesis Data
  Stream; bare stream names MUST be rejected.
- **FR-012**: System MUST validate at startup that `streamArn` matches
  `arn:<partition>:kinesis:<region>:<12-digit-account>:stream/<name>` with partition `aws`,
  `aws-cn` or `aws-us-gov`, and that its region equals `source.region`; mismatches MUST be rejected
  before any data is read or written.
- **FR-013**: System MUST enable the DynamoDB→Kinesis streaming destination on the source table and
  wait until it reaches ACTIVE; the migrator MUST NOT create the Kinesis stream itself.
- **FR-014**: `initialTimestamp` MUST be optional and accept an ISO-8601 instant used as the
  `AT_TIMESTAMP` initial position.
- **FR-015**: When `initialTimestamp` is omitted and no S3-export time applies, System MUST use the
  driver's current time captured right after the destination reaches ACTIVE, with no safety
  margin.
- **FR-016**: System MUST reject at load time an `initialTimestamp` more than 5 minutes in the
  future.
- **FR-017**: `appName` MUST be optional and name the lease table; it MUST default to
  `migrator_<source-table>_<arn-hash-8>` so that different stream ARNs on the same source get
  different lease tables. [NEEDS CLARIFICATION: the IAM prerequisites state the Kinesis lease
  table is `migrator_<source-table>` "by default", contradicting the documented `appName` default
  `migrator_<source-table>_<arn-hash-8>`; which is correct?]
- **FR-018**: Migrators sharing an `appName` MUST share a lease table and split shard ownership.
- **FR-019**: On shutdown, System MUST NOT disable the DynamoDB→Kinesis destination nor delete the
  Kinesis stream, so a restart can resume without data loss.
- **FR-020**: Required IAM permissions MUST be documented: DynamoDB
  `DescribeKinesisStreamingDestination`, `EnableKinesisStreamingDestination`, `DescribeTable`;
  Kinesis `DescribeStream`, `DescribeStreamSummary`, `GetRecords`, `GetShardIterator`,
  `ListShards`, `ListStreams`, `SubscribeToShard`; lease-table access (`CreateTable`,
  `DeleteItem`, `DescribeTable`, `GetItem`, `PutItem`, `Scan`, `UpdateItem` on
  `table/migrator_*`); and `cloudwatch:PutMetricData`.
- **FR-021**: The placement of `initialTimestamp` and `appName` in the YAML MUST be unambiguous.
  [NEEDS CLARIFICATION: in the stream-changes.rst Kinesis configuration example, `initialTimestamp`
  and `appName` are indented as siblings of `streamChanges` under `target`, while the S3-export
  section refers to `streamChanges.initialTimestamp`; are they fields of the `streamChanges`
  object?]

**Chaining S3 export with streaming**

- **FR-022**: A `dynamodb-s3-export` source MUST accept an optional `streamSource` block with the
  same options as a direct `dynamodb` source, identifying the live table the export came from.
- **FR-023**: With `streamSource` and `streamChanges`, System MUST apply the S3 export as the
  snapshot (no read load on the live table) and then enable the configured streaming mode
  (DynamoDB Streams or Kinesis) on the `streamSource` table.
- **FR-024**: In Kinesis mode without `initialTimestamp`, System MUST default the start instant to
  the manifest's `exportTime`, falling back to `startTime`.
- **FR-025**: When the manifest has neither `exportTime` nor `startTime`, System MUST log a warning
  and fall back to the driver's current time.
- **FR-026**: When `streamChanges` is requested on a `dynamodb-s3-export` source without
  `streamSource`, System MUST fail at startup rather than skip streaming.
- **FR-027**: `skipInitialSnapshotTransfer` behaviour in Kinesis mode and in S3-export chaining
  MUST be defined. [NEEDS CLARIFICATION: docs only describe `skipInitialSnapshotTransfer` with
  `streamChanges: true`; it is unspecified whether it applies to the Kinesis object form and, if
  so, what `AT_TIMESTAMP` is used.]

**Write semantics**

- **FR-028**: Replayed changes MUST be applied idempotently on the target primary key, so that
  replays (clock skew behind, restarts, `TrimHorizon` replay) only add write load.

### Key Entities

- **streamChanges setting**: boolean (`false` / `true`) or Kinesis object (`type`, `streamArn`,
  optional `initialTimestamp`, optional `appName`).
- **DynamoDB Stream**: per-table change log with 24 h retention, enabled by the migrator, not
  deleted on stop.
- **Kinesis Data Stream**: operator-created stream, retention up to one year, fed by the source
  table's Kinesis streaming destination.
- **Lease (checkpoint) table**: DynamoDB table tracking shard ownership/progress;
  `migrator_<source-table>` (DynamoDB Streams) or `appName` / `migrator_<source-table>_<arn-hash-8>`
  (Kinesis).
- **Replay start instant (AT_TIMESTAMP)**: `initialTimestamp`, else S3 export `exportTime` /
  `startTime`, else driver current time.
- **streamSource**: live-table DynamoDB source configuration attached to a `dynamodb-s3-export`
  source.
- **Export manifest summary**: `manifest-summary.json` carrying `exportTime` / `startTime`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In DynamoDB Streams mode with a snapshot completing in under 24 h, 100% of source
  writes made during and after the snapshot are present in the target after replay catches up.
- **SC-002**: In Kinesis mode with an NTP-synced driver (or explicit `initialTimestamp`), 100% of
  source writes made during a snapshot of any duration within the Kinesis retention period reach
  the target.
- **SC-003**: When chaining from an S3 export with a manifest containing `exportTime`, 100% of
  writes made to the live table after `exportTime` reach the target.
- **SC-004**: 100% of invalid streaming configurations (bad ARN, partition, region mismatch,
  `initialTimestamp` > 5 min in the future, `streamChanges` without `streamSource` on an S3 export
  source) are rejected before any data is read or written.
- **SC-005**: A restart after a clean stop in Kinesis mode loses no change records.
- **SC-006**: Existing configuration files using `streamChanges: true|false` run unchanged after
  upgrade.

## Assumptions

- The operator creates the Kinesis Data Stream (shards, retention, KMS) and grants the IAM
  permissions listed in FR-020.
- The operator runs the driver on an NTP-synced host or sets `initialTimestamp` explicitly.
- The operator stops the migrator manually and cleans up streams, destinations and orphaned lease
  tables.
- Source/target connection settings, throughput controls and AWS authentication are specified in
  `003-dynamodb-family-migration`.
- Cut-over orchestration (stopping application writes, switching clients) is out of scope.
- Streaming from CQL, Parquet, Aerospike or MySQL sources is out of scope (not supported).
