# Feature Specification: DynamoDB-to-S3-Export (DynamoDB table → DynamoDB S3 Export-compatible files)

**Feature Branch**: `012-dynamodb-to-s3-export`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from source code at commit 06835d0 — undocumented in docs/source)

**Input**: Source code at commit 06835d0: `migrator/src/main/scala/com/scylladb/migrator/Migrator.scala`
(source/target dispatch), `config/TargetSettings.scala` (`DynamoDBS3Export` target and decoder),
`config/MigratorConfig.scala` (Alternator-source rejection, savepoints requirement, target-table
savepoint restriction), `config/SourceSettings.scala` (DynamoDB source, `supportsSavepoints`),
`config/Savepoints.scala`, `writers/DynamoDBS3Export.scala`, `alternator/AlternatorMigrator.scala`
(`migrateToS3Export`), `alternator/DynamoDbSavepointsManager.scala`, `readers/DynamoDB.scala`
(`readRDD`), `readers/DynamoDBS3Export.scala` (round-trip import), `SavepointsResume.scala`,
`Validator.scala`; tests
`tests/src/test/scala/com/scylladb/migrator/alternator/DynamoDBToS3ExportMigrationTest.scala`,
`alternator/DynamoDBToS3ExportSavepointsTest.scala`, `alternator/DynamoDBToS3ExportE2EBenchmark.scala`,
`writers/DynamoDBS3ExportWriterTest.scala`, `writers/DynamoDBS3ExportEncoderTest.scala`,
`config/ParquetTargetValidationTest.scala` (S3-export target cases); fixtures
`tests/src/test/configurations/dynamodb-to-s3export-basic.yaml`,
`dynamodb-to-s3export-savepoints.yaml`, `bench-e2e-dynamodb-to-s3export.yaml`. The only
`docs/source/` mention is the compatibility sentence in `configuration.rst:261`.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Export a DynamoDB table in the DynamoDB S3 Export layout (Priority: P1)

A migration operator wants a file copy of an AWS DynamoDB table (or any DynamoDB-API endpoint
configured as a `dynamodb` source) in the same layout that AWS "Export to S3" produces, for
example to stage data in S3 and later import it into ScyllaDB Alternator with the
`dynamodb-s3-export` source (see `003-dynamodb-family-migration`) without enabling AWS PITR
exports. They write:

```yaml
source:
  type: dynamodb
  table: MyTable
  region: us-east-1
  # endpoint / credentials / scanSegments / readThroughput ... as in 003
target:
  type: dynamodb-s3-export
  path: s3a://my-bucket/exports/MyTable    # any Hadoop-compatible path
savepoints:
  path: /app/savepoints
  intervalSeconds: 300
```

**Why this priority**: It is the whole feature.

**Independent Test**: Create a table with N items, run the Migrator, then check that
`<path>/manifest-summary.json` reports `itemCount: N`, `manifest-files.json` lists the data files,
and the gzipped JSON lines under `<path>/data/` decode to the original items.

**Acceptance Scenarios**:

1. **Given** a source table with N items and a `path` that does not exist, **When** the Migrator
   runs to completion, **Then** `path` contains `data/` with gzipped JSON-lines files,
   `manifest-files.json` and `manifest-summary.json` whose `itemCount` is N.
2. **Given** an item with attributes of every DynamoDB type (S, N, B, BOOL, NULL, SS, NS, BS, L,
   M, nested), **When** it is exported, **Then** its line is `{"Item":{...}}` in DynamoDB JSON and
   decodes back to an equal item.
3. **Given** the source table has a key schema and attribute definitions, **When** the export
   completes, **Then** `manifest-summary.json` also contains `tableKeySchema` and
   `tableAttributeDefinitions`.
4. **Given** an empty source table, **When** the export completes, **Then** the manifests are
   written with `itemCount: 0` and no data files.

---

### User Story 2 - Re-import the export with the S3-export source (Priority: P1)

Having exported a table to S3, the operator migrates it into ScyllaDB Alternator (or DynamoDB)
using a `dynamodb-s3-export` source that points at the export.

**Why this priority**: Round-trip import is the main reason to produce this layout.

**Independent Test**: Export to `s3a://bucket/prefix`, then run a second job with
`source.type: dynamodb-s3-export`, `bucket: bucket`, `manifestKey: prefix/manifest-summary.json`
and a `tableDescription` matching the source key schema, and compare item counts and contents.

**Acceptance Scenarios**:

1. **Given** an export written to `s3a://<bucket>/<prefix>`, **When** a `dynamodb-s3-export`
   source is configured with `bucket: <bucket>` and `manifestKey: <prefix>/manifest-summary.json`,
   **Then** the relative keys in the manifests (`manifest-files.json`, `data/NNNNN.json.gz`) are
   resolved against `<prefix>/` and every exported item is read.
2. **Given** the same import, **When** the source configuration omits `tableDescription`,
   **Then** configuration loading fails, because the source does not use the `tableKeySchema` /
   `tableAttributeDefinitions` written in the summary.
3. **Given** the import's target enables `streamChanges` (Kinesis) with `source.streamSource`,
   **When** it starts, **Then** no `exportTime`/`startTime` is found in the summary written by this
   feature, so the stream start position falls back to the time streaming is enabled unless
   `streamChanges.initialTimestamp` is set (see `004-dynamodb-change-streaming`).

---

### User Story 3 - Savepoints during the export (Priority: P3)

The export reuses the DynamoDB source's scan-segment savepoints (see `008-savepoints-and-resume`).

**Why this priority**: Required by configuration validation, but of limited practical use (see
Edge Cases).

**Independent Test**: Run an export with a `savepoints` section and check that a savepoint file
with a non-empty `skipSegments` is written.

**Acceptance Scenarios**:

1. **Given** no `savepoints` section, **When** the Migrator starts, **Then** it fails with
   `Missing required field: savepoints`.
2. **Given** an export runs to completion, **When** it ends, **Then** a final savepoint is written
   whose `skipSegments` lists the scan segments that were exported.
3. **Given** `savepoints.target.type: target-table`, **When** the Migrator starts, **Then** it
   fails with `savepoints.target.type: target-table is supported only when target.type is 'scylla'.`

---

### User Story 4 - Reject unsupported combinations at startup (Priority: P2)

**Why this priority**: Prevents producing exports from sources that cannot be exported.

**Acceptance Scenarios**:

1. **Given** `source.type: alternator` and `target.type: dynamodb-s3-export`, **When** the
   configuration is loaded, **Then** it fails with `A source of type 'alternator' is not supported
   with target type 'dynamodb-s3-export'. DynamoDB S3 export output is only supported when the
   source is AWS DynamoDB.`
2. **Given** `target.path` is empty or whitespace, **When** the configuration is loaded, **Then**
   it fails with `DynamoDB S3 Export target 'path' must not be empty`.
3. **Given** any source other than `dynamodb`/`dynamo` (e.g. `cassandra`, `dynamodb-s3-export`)
   with this target, **When** the job starts, **Then** it fails with `Unsupported combination of
   source and target`.

### Edge Cases

- **Failures are not reported as failures.** If writing the export throws (including "Output path
  already exists"), the error is logged as `Caught error while writing DynamoDB S3 Export. Will
  create a savepoint before exiting`, a final savepoint is written, and the job then ends
  **normally** (the error is not re-thrown), so `spark-submit` reports success.
- **Existing output is never overwritten or appended.** If `path` exists the run fails with
  `Output path already exists: <path>. Delete it or choose a different path.` (a best-effort,
  non-atomic check) — and, per the previous point, still exits successfully.
- **Partial output is deleted on failure.** When writing data files or manifests fails, the whole
  `path` is deleted recursively (a failure to delete is only logged as a warning).
- **Resume is not meaningful and can lose items.** Scan segments are recorded as exported as soon
  as their data file task succeeds. After a failure the output directory is deleted, but the
  savepoint keeps those segments; with `autoResume: true` (default) the next run skips them and
  produces a smaller, apparently valid export. After a successful run, re-running the same
  configuration skips every segment (and fails on the existing path, silently). Operators must
  clear the savepoints directory or set `savepoints.autoResume: false` before re-exporting.
- If the driver is killed between data files and manifests, data files remain without manifests.
- Data file names are `data/<partition index, 5 digits>.json.gz`; empty partitions produce no file,
  so indices can have gaps (e.g. `00000`, `00003`).
- Attribute names are sorted alphabetically within each item and map, and SS/NS/BS set members are
  sorted, for deterministic output; AWS exports do not sort, so byte-level comparison with a real
  AWS export differs even though items are equal.
- The manifests are a subset of the AWS format: `manifest-files.json` lines carry only `itemCount`
  and `dataFileS3Key` (relative key, no checksum/etag); `manifest-summary.json` carries `version`
  (`2020-06-30`), `manifestFilesS3Key`, `itemCount`, `outputFormat: DYNAMODB_JSON` and optionally
  the table key schema, but no export ARN, `exportTime`, `startTime` or `exportType`.
- Files are written directly at `path`, not under an `AWSDynamoDB/<export-id>/` prefix.
- The `dynamodb-s3-export` source reads only from S3 (bucket + key via the S3 API, optionally a
  custom S3 endpoint); an export written to a local or HDFS path must be copied to S3 (or an
  S3-compatible store) before it can be re-imported.
- `renames` are not applied to exported attributes.
- The target has no credentials/endpoint properties; writing to `s3a://` relies on the Spark/Hadoop
  configuration (`spark.hadoop.fs.s3a.*`, instance profile), independent of the DynamoDB source's
  `credentials`.
- The Validator does not support this target (`Unsupported combination of source and target`).

## Requirements *(mandatory)*

### Functional Requirements

**Configuration and startup validation**

- **FR-001**: System MUST accept `target.type: dynamodb-s3-export` with a single required property
  `path`, and MUST reject an empty or whitespace-only `path` at startup with `DynamoDB S3 Export
  target 'path' must not be empty`; surrounding whitespace MUST be trimmed.
- **FR-002**: System MUST support this target only with a DynamoDB source (`source.type: dynamodb`
  or its alias `dynamo`), using that source's settings (`table`, `region`, `endpoint`,
  `credentials`, `scanSegments`, `maxMapTasks`, `readThroughput`, `throughputReadPercent`) as in
  `003-dynamodb-family-migration`.
- **FR-003**: System MUST reject `source.type: alternator` with this target at configuration load
  time with the message given in User Story 4.
- **FR-004**: System MUST fail other source types combined with this target when the job starts,
  with an "Unsupported combination of source and target" error.
- **FR-005**: System MUST require a `savepoints` section and MUST reject
  `savepoints.target.type: target-table` with this target.

**Output layout and format**

- **FR-006**: System MUST refuse to write when `path` already exists, with `Output path already
  exists: <path>. Delete it or choose a different path.`
- **FR-007**: System MUST write one gzip-compressed, UTF-8, newline-delimited file per non-empty
  read partition at `<path>/data/<NNNNN>.json.gz`, where `<NNNNN>` is the zero-padded partition
  index.
- **FR-008**: System MUST write each item as one line `{"Item":{<attribute>:{<type>:<value>}}}` in
  DynamoDB JSON, using `S`, `N` (string), `B` (base64), `BOOL`, `NULL: true`, `SS`, `NS`, `BS`
  (base64), `L` and `M`, with attribute/map keys sorted alphabetically and set members sorted
  (`NS` numerically).
- **FR-009**: System MUST write `<path>/manifest-files.json` with one JSON line per data file
  holding `itemCount` and `dataFileS3Key` (`data/<NNNNN>.json.gz`, relative to `path`).
- **FR-010**: System MUST write `<path>/manifest-summary.json` with `version: "2020-06-30"`,
  `manifestFilesS3Key: "manifest-files.json"`, the total `itemCount`, `outputFormat:
  "DYNAMODB_JSON"`, and `tableKeySchema` / `tableAttributeDefinitions` taken from the source table
  description.
- **FR-011**: System MUST accept any Hadoop-compatible filesystem URI for `path` and MUST rely on
  the Spark/Hadoop configuration for access to it.
- **FR-012**: System MUST produce output whose summary, manifest and item lines are accepted by the
  `dynamodb-s3-export` source of `003-dynamodb-family-migration` when the files are in S3 and
  `manifestKey` names `<prefix>/manifest-summary.json`.
- **FR-013**: System MUST log the target path at start and `Successfully wrote DynamoDB S3 Export:
  <items> items in <files> files` on success.
- **FR-014**: System MUST NOT apply `renames` to exported items.

**Failure handling and savepoints**

- **FR-015**: System MUST, when writing data files or manifests fails, log the error, delete
  `path` recursively (logging a warning if the deletion fails), and propagate the failure to the
  migration step.
- **FR-016**: System MUST, on any export failure, log `Caught error while writing DynamoDB S3
  Export. Will create a savepoint before exiting` and write a final savepoint; as built, it then
  completes without re-raising the error, so the job's exit status does not reflect the failure.
- **FR-017**: System MUST record exported scan segments in `skipSegments`, writing savepoints
  periodically (`savepoints.intervalSeconds`), on SIGINT/SIGTERM/USR2 and at the end of the run.
- **FR-018**: System MUST, when `savepoints.autoResume` is `true` (default) and a savepoint exists,
  skip the recorded scan segments on the next run (with the consequences listed in Edge Cases).

### Key Entities

- **S3-export target configuration** (`target`, `type: dynamodb-s3-export`): `path` (required).
- **Export directory**: `path` containing `manifest-summary.json`, `manifest-files.json` and
  `data/*.json.gz`.
- **Manifest summary**: `version`, `manifestFilesS3Key`, `itemCount`, `outputFormat`,
  `tableKeySchema`, `tableAttributeDefinitions`.
- **Savepoint**: configuration snapshot whose `skipSegments` lists exported scan segments.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For a quiesced source table, `manifest-summary.json` `itemCount` equals the table's
  item count and the sum of `itemCount` over `manifest-files.json` lines, in 100% of successful
  exports.
- **SC-002**: 100% of exported items decode, with the S3-export source's item decoder, to items
  equal to the source items (set and map ordering aside).
- **SC-003**: 100% of configurations with an empty `path` or an `alternator` source fail before
  any data is read.
- **SC-004**: An export never modifies a pre-existing `path`.
- **SC-005**: An export written to S3 can be imported with the `dynamodb-s3-export` source into an
  Alternator table with 100% of items present.

## Assumptions

- The source table is quiesced or the operator accepts a non-point-in-time export (the export is a
  parallel scan, not a PITR snapshot).
- Operators check the logs (not only the exit code) for export failures, because failures do not
  change the exit status (FR-016).
- Operators re-run a failed or repeated export with a clean savepoints directory or
  `savepoints.autoResume: false`.
- DynamoDB source reading (throughput, segments, TTL handling) is specified in `003`; savepoints in
  `008`; stream chaining in `004`.
- End-to-end re-import through S3 is supported by format compatibility (writer output is decoded by
  the reader's item decoder in tests, and relative manifest keys are resolved by the reader); no
  integration test runs the full DynamoDB → S3 export → Alternator chain.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Feature existence | Only `configuration.rst:261` ("A source of type `dynamodb` can be used together with a target of type `dynamodb`, `alternator`, or `dynamodb-s3-export`"); the target type list at `configuration.rst:572-576` omits it; no target subsection; `config.yaml.example` has only an S3-export *source* example (line 325) | `target.type: dynamodb-s3-export` decoded at `config/TargetSettings.scala:376-387`, dispatched at `Migrator.scala:100-104` | Code | Add `dynamodb-s3-export` to the target type list and a "DynamoDB S3 Export Target" subsection in `configuration.rst` (property `path`, output layout, path-must-not-exist rule); add a target example to `config.yaml.example` |
| 2 | User guide | `migrate-from-dynamodb.rst:121` documents reading an S3 export only | Writing an export exists (`writers/DynamoDBS3Export.scala:60-192`) | Code | Add an "Exporting a DynamoDB table to S3" section to `migrate-from-dynamodb.rst`, including the round-trip import recipe (`bucket`, `manifestKey: <prefix>/manifest-summary.json`, `tableDescription` still required) |
| 3 | Alternator source | — | `alternator` source rejected with this target (`config/MigratorConfig.scala:109-115`) | Code | State the restriction in the new target subsection |
| 4 | Exit status on failure | — | Errors caught and not re-thrown (`alternator/AlternatorMigrator.scala:71-81`), unlike the Parquet export (`scylla/ScyllaMigrator.scala:350`) | Code (flagged) | Document that failures must be detected from logs; preferably fix the code to re-throw |
| 5 | Savepoints / resume | `resume-interrupted-migration.rst:5`: DynamoDB is resumable; S3-export *sources* are not | Segments recorded per task (`alternator/DynamoDbSavepointsManager.scala:56-73`) while output is deleted on failure (`writers/DynamoDBS3Export.scala:179-186`); auto-resume defaults to on (`config/Savepoints.scala:300`) | Code (caveats in Edge Cases) | Document that DynamoDB → S3-export is effectively not resumable and that re-runs need a clean savepoints directory or `autoResume: false`; consider disabling savepoints for this target in code |
| 6 | Output format fidelity | — | Sorted attributes/sets, relative keys, reduced manifest fields, no `AWSDynamoDB/<id>/` prefix (`writers/DynamoDBS3Export.scala:92,115-171,200-247`) | Code | Describe the format as "compatible with the migrator's S3-export source", not as identical to AWS exports |
| 7 | Stream chaining | `stream-changes.rst:179` mentions the case where no S3-export `exportTime` is available | Summary written here has no `exportTime`/`startTime` (`writers/DynamoDBS3Export.scala:138-143`; reader fallback `readers/DynamoDBS3Export.scala:237`) | Code | In `stream-changes.rst`, note that migrator-produced exports need `streamChanges.initialTimestamp` set explicitly |
| 8 | Validator | `validate.rst` does not list this pair | No case for this target (`Validator.scala:26-37`) | Code | Note that S3 exports cannot be validated directly |

Verified unchanged: the single documented fact — that a `dynamodb` source may be combined with a
`dynamodb-s3-export` target (`configuration.rst:261`) — matches the code; everything else is
undocumented and derived from code and tests.
