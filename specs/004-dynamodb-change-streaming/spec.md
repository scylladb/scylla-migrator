# Feature Specification: DynamoDB Change Streaming (DynamoDB Streams and Kinesis Data Streams)

**Feature Branch**: `004-dynamodb-change-streaming`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/stream-changes.rst` (whole page: "DynamoDB Streams mode (legacy, 24h retention)", "Kinesis Data Streams mode (up to 1y retention)", "When to prefer Kinesis over DynamoDB Streams", "Chaining S3-export with streamChanges"); `docs/source/configuration.rst` sections "DynamoDB Target" and "Alternator Target" (`streamChanges`, `skipInitialSnapshotTransfer`); `docs/source/migrate-from-dynamodb.rst` ("Writing to DynamoDB"); source code at commit 06835d0 (`config/StreamChangesSetting.scala`, `config/TargetSettings.scala`, `config/SourceSettings.scala` (`streamSource`), `config/MigratorConfig.scala`, `alternator/AlternatorMigrator.scala`, `DynamoUtils.scala`, `writers/DynamoStreamReplication.scala`, `writers/KinesisJsonDeserializer.scala`, `readers/DynamoDBS3Export.scala`, `spark-kinesis-dynamodb/` module, tests `StreamChangesSettingParserTest`, `KinesisStreamingYamlExampleTest`, `DynamoDBTargetSettingParserTest`, fixtures `dynamodb-to-alternator-streaming*.yaml`).

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
   `streamChanges: true`, **When** the migrator runs, **Then** it enables the source table's
   DynamoDB Stream (new-image view), waits until the stream is ENABLED, transfers the snapshot and
   then continuously replicates changes instead of terminating.
2. **Given** the migrator is streaming, **When** the operator presses Ctrl+C or sends SIGINT to the
   `spark-submit` process, **Then** the migrator stops; the DynamoDB stream it enabled is **not**
   deleted and must be removed manually.
3. **Given** `streamChanges: true` and `skipInitialSnapshotTransfer: true`, **When** the migrator
   runs, **Then** it skips the snapshot and only replicates changes.
4. **Given** `streamChanges: false` and `skipInitialSnapshotTransfer: true`, **When** the migrator
   runs, **Then** `skipInitialSnapshotTransfer` is ignored and a snapshot-only migration runs.
5. **Given** the snapshot takes longer than 24 hours, **When** streaming starts, **Then** changes
   older than the stream's 24-hour retention may be lost (documented limitation).
6. **Given** the source stream does not reach ENABLED within 5 minutes, **When** the migrator is
   waiting, **Then** it fails with a timeout error naming the table and last status.

---

### User Story 2 - Replicate via Kinesis Data Streams for long snapshots (Priority: P1)

An operator with a large table whose snapshot may exceed 24 hours pre-creates a Kinesis Data
Stream and configures `streamChanges: {type: kinesis, streamArn: ...}`. The migrator enables the
DynamoDB→Kinesis streaming destination, captures a start instant once the destination is ACTIVE,
copies the snapshot, then replays changes from that instant (`AT_TIMESTAMP`).

**Why this priority**: Removes the 24-hour data-loss window for large migrations (constitution
principle I).

**Independent Test**: Pre-create a Kinesis stream in the source region, run a migration with
`type: kinesis` and a valid `streamArn`, write to the source during the snapshot, and confirm
those writes reach the target after the snapshot.

**Acceptance Scenarios**:

1. **Given** `streamChanges.type: kinesis` and `streamArn:
   arn:aws:kinesis:us-east-1:123456789012:stream/my-dynamo-stream` with source region
   `us-east-1`, **When** the migrator starts, **Then** it enables the Kinesis streaming
   destination on the source table (skipping the call when it is already ACTIVE or ENABLING),
   waits up to 15 minutes until it is ACTIVE, and captures the replay start instant.
2. **Given** `initialTimestamp` is omitted (and no S3 export `exportTime` applies), **When** the
   destination reaches ACTIVE, **Then** the replay start instant is the driver's current time
   captured just after that moment, with no safety margin.
3. **Given** `streamChanges.initialTimestamp: 2026-04-13T08:00:00Z`, **When** the migrator
   starts, **Then** replay starts at that instant regardless of the driver clock (and regardless
   of any S3 export time).
4. **Given** `initialTimestamp` more than 5 minutes in the future, **When** the configuration is
   loaded, **Then** it is rejected.
5. **Given** a bare stream name, a malformed ARN, a non-12-digit account, or a partition other
   than `aws`, `aws-cn` or `aws-us-gov`, **When** the configuration is loaded, **Then** it is
   rejected; **given** an ARN region different from `source.region`, or no `source.region` at
   all, **When** the migrator starts, **Then** it is rejected before the streaming destination is
   enabled and before any item is written.
6. **Given** `type: kinesis-data-streams`, **When** the configuration is loaded, **Then** it is
   treated exactly like `type: kinesis`.
7. **Given** the migrator is stopped, **When** it shuts down, **Then** it does not disable the
   DynamoDB→Kinesis destination and does not delete the Kinesis stream, so a later restart can
   resume without data loss.
8. **Given** the destination ends in ENABLE_FAILED, or the stream ARN is not registered on the
   table after enabling, **When** the migrator waits, **Then** it fails with an error describing
   the destinations currently registered.
9. **Given** `streamChanges: {type: kinesis, ...}` and `skipInitialSnapshotTransfer: true`,
   **When** the migrator runs, **Then** it skips the snapshot and replays from `initialTimestamp`
   (or the default start instant of AS2 / US3).

---

### User Story 3 - Seed from an S3 export, then stream from the live table (Priority: P2)

An operator uses an existing DynamoDB S3 export as the initial snapshot (no read load on the live
table) and adds a `streamSource` block pointing at the still-live source table, so changes made
after the export are replayed from the export's `exportTime` via Kinesis Data Streams.

**Why this priority**: Combines the cheapest snapshot path with lossless catch-up.

**Independent Test**: Configure a `dynamodb-s3-export` source with `streamSource` and a Kinesis
`streamChanges` target without `initialTimestamp`; write to the live table after the export time;
confirm those writes reach the target.

**Acceptance Scenarios**:

1. **Given** a `dynamodb-s3-export` source with `streamSource: {table: MyLiveTable,
   region: us-east-1}` and a target with `streamChanges: {type: kinesis, streamArn: ...}`,
   **When** the migrator runs, **Then** it enables the Kinesis destination on the live table named
   in `streamSource`, applies the export as the snapshot, and then replays changes from the live
   table.
2. **Given** Kinesis mode without `initialTimestamp`, **When** replay starts, **Then** the start
   instant is the manifest's `exportTime`, or `startTime` when `exportTime` is absent.
3. **Given** the manifest has neither `exportTime` nor `startTime`, **When** replay starts,
   **Then** the migrator logs a warning and falls back to the driver's current time (writes between
   the export and that instant are lost unless `initialTimestamp` is set).
4. **Given** a `dynamodb-s3-export` source with Kinesis `streamChanges` but no `streamSource`,
   **When** the configuration is loaded, **Then** it is rejected instead of silently skipping
   streaming.
5. **Given** a `dynamodb-s3-export` source with `streamChanges: true` (DynamoDB Streams mode),
   **When** the configuration is loaded, **Then** it is rejected with a message directing the
   operator to Kinesis Data Streams plus `streamSource`.

---

### User Story 4 - Fan out one source to several targets (Priority: P3)

An operator replicates the same source table to two targets concurrently using Kinesis mode, each
migrator with its own lease table (default name includes a hash of the stream ARN, or an explicit
`streamChanges.appName`).

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
   the changes (documented limitation, also logged as a warning at start; avoid this setup).

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
2. **Given** a `migrator_<source-table>` lease table from a previous run of this version, **When**
   the migrator restarts, **Then** it resumes from the checkpoints in that lease table instead of
   replaying from `TrimHorizon`.

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
  skew; timestamps in the past are accepted (retention is checked by AWS at runtime).
- An `alternator` source cannot stream changes (rejected at load): Alternator does not publish
  DynamoDB Streams or Kinesis records from the source side here.
- The change consumers always connect to the public AWS endpoints of `source.region`; a custom
  source `endpoint` (DynamoDB Local, LocalStack) is used for enabling the stream/destination but
  not for consuming changes, so streaming against local emulators is not supported.
- A replayed put or delete that fails on the target is logged as an error and skipped (not
  retried); the micro-batch continues. Kinesis records that cannot be decoded (unknown event name,
  missing keys, malformed JSON) are dropped and counted in the per-batch log line.
- If the source table's stream was already enabled with a different view type, enabling it again
  may fail; the migrator requests the new-image view.

## Requirements *(mandatory)*

### Functional Requirements

**Applicability and configuration shapes**

- **FR-001**: System MUST support change streaming only when reading from an AWS DynamoDB
  `dynamodb` source (DynamoDB Streams or Kinesis mode), or from a `dynamodb-s3-export` source with
  `streamSource` (Kinesis mode only), and writing to an `alternator` or `dynamodb` target. An
  `alternator` source with streaming enabled MUST be rejected at configuration load.
- **FR-002**: The target property `streamChanges` MUST be present and accept: boolean `false`
  (snapshot only), boolean `true` (DynamoDB Streams mode), an explicit null (snapshot only), or an
  object whose `type` is `disabled` / `none` / `off` (snapshot only), `dynamodb-streams` /
  `dynamodb` / `ddb` (DynamoDB Streams mode), or `kinesis` / `kinesis-data-streams` (Kinesis Data
  Streams mode). Other scalars and unknown `type` values MUST be rejected with a message listing
  the valid values; a missing `streamChanges` property MUST be rejected.
- **FR-003**: Existing configuration files using boolean `streamChanges` MUST keep working
  unchanged.
- **FR-004**: `skipInitialSnapshotTransfer` MUST be optional, default `false`, and, when `true`
  with streaming enabled (either mode, including S3-export chaining), MUST skip the snapshot and
  only replicate changes; it MUST be ignored when streaming is disabled.
- **FR-005**: In both streaming modes, System MUST transfer the initial snapshot (unless skipped)
  and then keep replicating changes in micro-batches (5-second interval) until interrupted by
  Ctrl+C / SIGINT.

**DynamoDB Streams mode**

- **FR-006**: With DynamoDB Streams mode, System MUST enable the source table's DynamoDB Stream
  (new-image view), wait for it to become ENABLED (polling every 5 s for at most 5 minutes, then
  failing with a timeout error), and read it.
- **FR-007**: System MUST NOT delete the DynamoDB stream when stopped.
- **FR-008**: System MUST use the deterministic lease (checkpoint) table name
  `migrator_<source-table>` and log a warning at start explaining the restart and
  concurrent-migrator consequences.
- **FR-009**: When no lease table with the deterministic name exists, System MUST start replay
  from `TrimHorizon`; when it exists, System MUST resume from its checkpoints.
- **FR-010**: DynamoDB Streams mode MUST NOT accept an `appName` override (current limitation);
  concurrent migrators on one source table share the lease table.

**Kinesis Data Streams mode**

- **FR-011**: Kinesis mode MUST require `streamChanges.streamArn`, the full ARN of a pre-created
  Kinesis Data Stream; bare stream names MUST be rejected.
- **FR-012**: System MUST validate at configuration load that `streamArn` matches
  `arn:<partition>:kinesis:<region>:<12-digit-account>:stream/<name>` with partition `aws`,
  `aws-cn` or `aws-us-gov` and a stream name without `:`, `/` or whitespace. At start-up, before
  enabling the destination or writing any item, System MUST require `source.region` to be set
  explicitly and to equal the ARN's region.
- **FR-013**: System MUST enable the DynamoDB→Kinesis streaming destination on the source table
  (skipping the call when the destination is already ACTIVE or ENABLING, treating a concurrent
  "already enabled" response as success, and retrying transient errors up to 5 times with
  backoff) and wait until it reaches ACTIVE (at most 15 minutes; ENABLE_FAILED or an unregistered
  ARN MUST fail with a descriptive error); the migrator MUST NOT create the Kinesis stream itself.
- **FR-014**: `streamChanges.initialTimestamp` MUST be optional and accept an ISO-8601 instant
  used as the `AT_TIMESTAMP` initial position; when set it MUST take precedence over every
  default (including an S3 export time).
- **FR-015**: When `initialTimestamp` is omitted and no S3-export time applies, System MUST use the
  driver's current time captured right after the destination reaches ACTIVE, with no safety
  margin.
- **FR-016**: System MUST reject at load time an `initialTimestamp` more than 5 minutes in the
  future.
- **FR-017**: `streamChanges.appName` MUST be optional and name the lease table; it MUST default
  to `migrator_<source-table>_<arn-hash-8>` (first 8 hex characters of the SHA-256 of
  `streamArn`) so that different stream ARNs on the same source get different lease tables.
- **FR-018**: Migrators sharing an `appName` MUST share a lease table and split shard ownership.
- **FR-019**: On shutdown, System MUST NOT disable the DynamoDB→Kinesis destination nor delete the
  Kinesis stream, so a restart can resume without data loss.
- **FR-020**: Required IAM permissions MUST be documented: DynamoDB
  `DescribeKinesisStreamingDestination`, `EnableKinesisStreamingDestination`, `DescribeTable`;
  Kinesis `DescribeStream`, `DescribeStreamSummary`, `GetRecords`, `GetShardIterator`,
  `ListShards`, `ListStreams`, `SubscribeToShard`; lease-table access (`CreateTable`,
  `DeleteItem`, `DescribeTable`, `GetItem`, `PutItem`, `Scan`, `UpdateItem` on
  `table/migrator_*`); and `cloudwatch:PutMetricData`.
- **FR-021**: `initialTimestamp` and `appName` MUST be fields of the `streamChanges` object
  (siblings of `type` and `streamArn`); keys placed directly under `target` are not read.

**Chaining S3 export with streaming**

- **FR-022**: A `dynamodb-s3-export` source MUST accept an optional `streamSource` block with the
  same fields as a direct `dynamodb` source (`table`, `region`, `credentials`, `endpoint`, ...),
  identifying the live table the export came from; a `type` key inside it is ignored, and it is
  ignored entirely when the target does not stream.
- **FR-023**: With `streamSource` and Kinesis `streamChanges`, System MUST apply the S3 export as
  the snapshot (no read load on the live table), enable the Kinesis destination on the
  `streamSource` table, and replay its changes using the `streamSource` region and credentials.
  DynamoDB Streams mode with a `dynamodb-s3-export` source MUST be rejected at configuration load.
- **FR-024**: In Kinesis mode without `initialTimestamp`, System MUST default the start instant to
  the manifest's `exportTime`, falling back to `startTime`, and log the chosen instant.
- **FR-025**: When the manifest has neither `exportTime` nor `startTime`, System MUST log a warning
  and fall back to the driver's current time.
- **FR-026**: When Kinesis `streamChanges` is requested on a `dynamodb-s3-export` source without
  `streamSource`, System MUST fail at configuration load rather than skip streaming.
- **FR-027**: `skipInitialSnapshotTransfer: true` MUST also apply in Kinesis mode and in
  S3-export chaining: the snapshot (or export) is not written and replay starts at the instant
  defined by FR-014/FR-015/FR-024.

**Write semantics**

- **FR-028**: Replayed changes MUST be applied idempotently on the target primary key, so that
  replays (clock skew behind, restarts, `TrimHorizon` replay) only add write load: INSERT and
  MODIFY events MUST write the full new image (put), REMOVE events MUST delete by the target key
  attributes; configured `renames` MUST be applied to replayed items.
- **FR-029**: A replayed put/delete that fails on the target MUST be logged as an error; it is
  currently skipped without retry (see Docs vs Code Review row 10).
- **FR-030**: Kinesis records that cannot be decoded MUST be dropped and counted; each micro-batch
  with changes MUST log the number of upserts, deletes and (lifetime) dropped records.

**Connectivity and credentials**

- **FR-031**: The change consumers MUST use the source (or `streamSource`) `credentials`,
  including `assumeRole`, or the AWS default credential chain when none are configured, and MUST
  connect to the AWS endpoints of the source region (a custom source `endpoint` is not used for
  consuming changes).

### Key Entities

- **streamChanges setting**: boolean (`false` / `true`), null, or object (`type`, and for Kinesis
  `streamArn`, optional `initialTimestamp`, optional `appName`).
- **DynamoDB Stream**: per-table change log with 24 h retention, enabled by the migrator
  (new-image view), not deleted on stop.
- **Kinesis Data Stream**: operator-created stream, retention up to one year, fed by the source
  table's Kinesis streaming destination.
- **Lease (checkpoint) table**: DynamoDB table tracking shard ownership/progress;
  `migrator_<source-table>` (DynamoDB Streams) or `appName` / `migrator_<source-table>_<arn-hash-8>`
  (Kinesis).
- **Replay start instant (AT_TIMESTAMP)**: `initialTimestamp`, else S3 export `exportTime` /
  `startTime`, else driver current time after the destination is ACTIVE.
- **streamSource**: live-table DynamoDB source configuration attached to a `dynamodb-s3-export`
  source.
- **Export manifest summary**: `manifest-summary.json` carrying `exportTime` / `startTime`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In DynamoDB Streams mode with a snapshot completing in under 24 h, 100% of source
  writes made during and after the snapshot are present in the target after replay catches up
  (absent target write errors, which are logged).
- **SC-002**: In Kinesis mode with an NTP-synced driver (or explicit `initialTimestamp`), 100% of
  source writes made during a snapshot of any duration within the Kinesis retention period reach
  the target.
- **SC-003**: When chaining from an S3 export with a manifest containing `exportTime`, 100% of
  writes made to the live table after `exportTime` reach the target.
- **SC-004**: 100% of invalid streaming configurations (bad ARN, partition, unknown `type`,
  `initialTimestamp` > 5 min in the future, missing `streamChanges`, Alternator source with
  streaming, DynamoDB Streams with an S3 export source, Kinesis without `streamSource` on an S3
  export source) are rejected at configuration load; missing `source.region` or ARN/region
  mismatch in Kinesis mode is rejected before any item is written.
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
- Streaming from CQL, Parquet, Aerospike, MySQL or Alternator sources is out of scope (not
  supported).

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Kinesis default lease-table name | IAM note: lease table is `migrator_<source-table>` "by default"; `appName` comment: `migrator_<source-table>_<arn-hash-8>` | `migrator_<table>_<first 8 hex of SHA-256(streamArn)>` for Kinesis; `migrator_<table>` only for DynamoDB Streams (`writers/DynamoStreamReplication.scala:345-363`) | Code (FR-017) | Fix the IAM note: "`migrator_<source-table>` (DynamoDB Streams) or `migrator_<source-table>_<arn-hash-8>` / `appName` (Kinesis)"; `table/migrator_*` still covers both. |
| 2 | Placement of `initialTimestamp` / `appName` | Kinesis example indents them as siblings of `streamChanges` under `target` | Read only from inside the `streamChanges` object (`config/StreamChangesSetting.scala:136-142`; fixture `dynamodb-to-alternator-streaming-kinesis.yaml:28-31`) | Code (FR-021) | Re-indent the example under `streamChanges`. |
| 3 | S3 export chaining with DynamoDB Streams | "enables the configured streaming destination (DynamoDB Streams or Kinesis Data Streams)" on the live table | `streamChanges: true` with a `dynamodb-s3-export` source is rejected at load (`config/MigratorConfig.scala:117-127`) | Code (FR-023, US3 AS5) | State that only Kinesis mode is supported for S3-export chaining. |
| 4 | Omitting `streamChanges` | "`false` — snapshot-only. Same as omitting the field historically." | Missing property fails decoding; explicit null = disabled (`config/TargetSettings.scala:280`; `config/StreamChangesSetting.scala:105-108`; `DynamoDBTargetSettingParserTest.scala:720-735`) | Code (FR-002) | Say `streamChanges` is required. |
| 5 | Object `type` aliases | Only `kinesis` / `kinesis-data-streams` | Also `disabled`/`none`/`off` and `dynamodb-streams`/`dynamodb`/`ddb` (`config/StreamChangesSetting.scala:132-135`) | Code (FR-002) | Document the additional object forms (constitution III: aliases documented). |
| 6 | `skipInitialSnapshotTransfer` in Kinesis / S3 chaining | Only described for `streamChanges: true` | Applies whenever streaming is enabled; AT_TIMESTAMP unchanged (`alternator/AlternatorMigrator.scala:252-256`) | Code (FR-004, FR-027) | Document behaviour for both modes. |
| 7 | `source.region` in Kinesis mode | Implied by "ARN region must match source.region" | Explicit `source.region` required; checked at orchestration (after source describe, before enable/snapshot), not at config load (`alternator/AlternatorMigrator.scala:202-223`) | Code (FR-012) | Say region is mandatory for Kinesis and when it is checked. |
| 8 | Alternator source with streaming | Implied ("only when reading from DynamoDB") | Rejected at load with explicit message (`config/MigratorConfig.scala:100-108`) | Code (FR-001) | Mention the error. |
| 9 | Custom source endpoint during streaming | Not documented | Stream consumers use the AWS default endpoints for `source.region` (`spark-kinesis-dynamodb/.../KinesisDynamoDBInputDStream.scala:28`, `KinesisDynamoDBReceiver.scala:168-191`; `writers/DynamoStreamReplication.scala:305-311`) | Code (FR-031) | Document that streaming requires real AWS DynamoDB/Kinesis. |
| 10 | Failed replay writes | Not documented (idempotent replays assumed) | Put/delete exceptions are logged and skipped, no retry (`writers/DynamoStreamReplication.scala:137-158`) | Code (FR-029) | Document, and consider a code fix (constitution I: rows must be written or skipped by explicit policy). |
| 11 | Undecodable Kinesis records | Not documented | Dropped and counted in per-batch log (`writers/DynamoStreamReplication.scala:312-317`; `writers/KinesisJsonDeserializer.scala:30-33,80-88`) | Code (FR-030) | Document the drop counter. |
| 12 | Enable/wait timeouts and stream view | Not documented | DynamoDB stream: `NEW_IMAGE`, 60×5 s wait; Kinesis: idempotent enable, 5 transient retries, 15 min ACTIVE deadline, ENABLE_FAILED error (`DynamoUtils.scala:373-404,432-530,564-656`) | Code (FR-006, FR-013) | Add a short "start-up checks and timeouts" note. |
| 13 | Restart in DynamoDB Streams mode | Upgrade note covers `_<millis>` → TrimHorizon | Restart with existing `migrator_<table>` resumes from lease checkpoints; warning logged (`writers/DynamoStreamReplication.scala:233-242`) | Code (FR-008, FR-009) | Already partly documented; add restart behaviour explicitly. |

Verified unchanged: boolean `streamChanges` compatibility, `kinesis-data-streams` alias, ARN
regex and partitions, 5-minute future limit on `initialTimestamp`, default AT_TIMESTAMP captured
after ACTIVE with no margin, `exportTime` → `startTime` → now fallback with warning, fail-fast
without `streamSource`, deterministic `migrator_<table>` lease name for DynamoDB Streams, no
cleanup on shutdown, TrimHorizon start for DynamoDB Streams, and the IAM permission list all match
the docs.
