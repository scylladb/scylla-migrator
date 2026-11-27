# Feature Specification: Migration Validation

**Feature Branch**: `009-migration-validation`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/validate.rst` (whole page: introduction, "MySQL custom join keys", validation properties, "Run the Validator in the Ansible-based Setup", "... in a Manual Spark Setup", "... in AWS EMR", "... in a Docker-based Spark Setup"); `docs/source/configuration.rst` sections "Overview" (`validation` top-level property) and "Validation", and the `preserveCollectionTimestamps` comment in "CQL-Compatible Source" (validation and `copyMissingRows` paragraph); `docs/source/getting-started/ansible.rst` (validator submit scripts); source code at commit 06835d0 (`Validator.scala`, `config/Validation.scala`, `config/RepairWritetimeStrategy.scala`, `config/MigratorConfig.scala`, `scylla/ScyllaValidator.scala`, `scylla/MySQLToScyllaValidator.scala`, `alternator/AlternatorValidator.scala`, `validation/RowComparisonFailure.scala`, `validation/core/*.scala`, `ansible/templates/submit-*-validator.sh`).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Check that the target matches the source after a migration (Priority: P1)

After a migration finishes, an operator submits the same Spark job with the Validator entry point
(`--class com.scylladb.migrator.Validator`) and the same configuration file, extended with a
`validation` section. The Validator compares source and target, logs up to `failuresToFetch`
differences with a summary, and signals the outcome through its exit status.

**Why this priority**: Validation is how operators prove that no data was lost or corrupted
(constitution principle I).

**Independent Test**: Migrate a table, run the Validator and observe exit status 0 and the
"No comparison failures found" log line; then delete or modify a row in the target, re-run, and
observe the difference logged and exit status 1.

**Acceptance Scenarios**:

1. **Given** a completed migration and a configuration with a `validation` section, **When** the
   operator runs `spark-submit --class com.scylladb.migrator.Validator --conf
   spark.scylla.config=<config.yaml> <scylla-migrator-assembly.jar>`, **Then** the Validator
   compares every source row with the target and, when no difference is found, logs "No
   comparison failures found" and exits with status 0.
2. **Given** the target has differences, **When** validation runs with `failuresToFetch: 100`,
   **Then** at most 100 differences are collected, an error line "Found N comparison failure(s) in
   sample: …" with a per-kind breakdown (missing target rows, extra target rows, differing field
   values, numeric type mismatches, mismatched column counts/names) is logged followed by each
   failure, and the process exits with status 1.
3. **Given** the application runs in migration mode, **When** the configuration has no
   `validation` section, **Then** the configuration is still valid.
4. **Given** the application runs in validation mode, **When** the configuration has no
   `validation` section, **Then** validation fails before comparing anything with the error
   "Missing required property 'validation' in the configuration file."
5. **Given** a `validation` section missing one of `compareTimestamps`, `ttlToleranceMillis`,
   `writetimeToleranceMillis`, `failuresToFetch`, `floatingPointTolerance` or
   `timestampMsTolerance`, or with `failuresToFetch ≤ 0`, a negative tolerance, or a non-finite
   `floatingPointTolerance`, **When** the configuration is loaded, **Then** it is rejected with a
   decoding error naming the property.
6. **Given** an unsupported source/target pair (e.g. a Parquet, Aerospike or DynamoDB S3 export
   source), **When** the Validator starts, **Then** it fails with "Unsupported combination of
   source and target".

---

### User Story 2 - Tolerate expected differences (Priority: P2)

An operator configures how strictly values are compared: whether WRITETIMEs and TTLs are compared
at all (`compareTimestamps`) and with which tolerances (`ttlToleranceMillis`,
`writetimeToleranceMillis`), the tolerance for floating-point numbers
(`floatingPointTolerance`) and timestamps (`timestampMsTolerance`), and how numeric type changes
are judged (`numericTypePolicy`).

**Why this priority**: Without tolerances, TTL clock drift during long migrations and
floating-point representation would produce false positives.

**Independent Test**: Migrate a CQL table with TTLs, wait some seconds, validate with
`ttlToleranceMillis: 60000` (no failures) and with `ttlToleranceMillis: 0` (failures).

**Acceptance Scenarios**:

1. **Given** a CQL→CQL validation with `compareTimestamps: true` and `ttlToleranceMillis: 60000`,
   **When** a target TTL differs from the source TTL by less than 60 s, **Then** it is not
   reported.
2. **Given** `writetimeToleranceMillis: 1000`, **When** WRITETIMEs differ by more than 1 s,
   **Then** the row is reported (the difference is printed in microseconds).
3. **Given** `compareTimestamps: false`, **When** validation runs, **Then** WRITETIMEs and TTLs are
   not compared.
4. **Given** `floatingPointTolerance: 0.001`, **When** two floating-point or decimal values differ
   by less than 0.001, **Then** they are considered equal (integral values are always compared
   exactly).
5. **Given** `timestampMsTolerance: 0`, **When** timestamp values differ by any amount, **Then**
   they are reported.
6. **Given** `numericTypePolicy: StrictType`, **When** equal numeric values have different numeric
   types on the two sides, **Then** a numeric type mismatch is reported; with `DetectWiden`, only a
   float↔double change whose values are not exactly equal is reported; with the default
   `Lenient`, numeric type changes are never reported.

---

### User Story 3 - Run the Validator in each supported deployment (Priority: P2)

An operator runs the Validator in whichever Spark setup they used for the migration: Ansible,
manual Spark standalone, AWS EMR, or Docker Compose.

**Why this priority**: Each deployment has a different submission mechanism; all must work.

**Independent Test**: In each deployment, submit the Validator with the documented command and
confirm it runs against the configured tables.

**Acceptance Scenarios**:

1. **Given** the Ansible-based setup, **When** the operator submits `submit-cql-job-validator.sh`
   (or `submit-alternator-validator.sh` for Alternator), **Then** the Validator runs.
2. **Given** AWS EMR, **When** a custom-JAR Cluster Step runs `spark-submit --deploy-mode cluster
   --class com.scylladb.migrator.Validator --conf spark.scylla.config=/mnt1/config.yaml
   /mnt1/scylla-migrator-assembly.jar`, **Then** the Validator runs.
3. **Given** the Docker-based setup, **When** the operator runs `docker compose exec spark-master
   /spark/bin/spark-submit --class com.scylladb.migrator.Validator --master
   spark://spark-master:7077 --conf spark.driver.host=spark-master --conf
   spark.scylla.config=/app/config.yaml /jars/scylla-migrator-assembly.jar`, **Then** the
   Validator runs.

---

### User Story 4 - Validate a MySQL migration with a custom join key (Priority: P3)

An operator who migrated a MySQL table whose ScyllaDB primary key differs from the MySQL declared
primary key sets `source.primaryKey` to the join key, `skipSourcePrimaryKeyValidation: true`, and
one safeguard: `verifySourceJoinKeyUniqueness: true` (scan and reject duplicates/nulls) or
`acknowledgeUnverifiedJoinKey: true` (operator asserts uniqueness).

**Why this priority**: Needed only for MySQL tables with non-matching keys.

**Independent Test**: Configure a MySQL source with `primaryKey: [tenant_id, external_id]`,
`skipSourcePrimaryKeyValidation: true`, `verifySourceJoinKeyUniqueness: true`; insert a duplicate
key combination in MySQL and confirm validation refuses before target validation; remove the
duplicate and confirm validation proceeds.

**Acceptance Scenarios**:

1. **Given** default options, **When** `source.primaryKey` does not match the MySQL table's
   declared primary key (including column order; names compared case-insensitively), or the table
   exposes no primary key, **Then** validation rejects the configuration.
2. **Given** `skipSourcePrimaryKeyValidation: true` with neither safeguard, **When** validation
   starts, **Then** it refuses to use the custom join key.
3. **Given** `skipSourcePrimaryKeyValidation: true` and `verifySourceJoinKeyUniqueness: true`,
   **When** the selected source rows contain a duplicate key combination or a null key component,
   **Then** validation rejects them before target validation, quoting up to 5 offending keys.
4. **Given** both safeguards enabled, **When** validation runs, **Then** uniqueness verification
   takes precedence.
5. **Given** `skipSourcePrimaryKeyValidation: false`, **When** either safeguard is set, **Then**
   the safeguard has no effect.
6. **Given** custom join-key columns that, after `renames`, do not match the ScyllaDB target
   primary key, **When** validation starts, **Then** it is rejected (the override does not bypass
   the target check).
7. **Given** a MySQL source with a `where` filter, **When** uniqueness is verified, **Then** only
   the selected rows are checked.
8. **Given** a MySQL source without `primaryKey`, **When** validation starts, **Then** it fails
   with "Missing required property 'primaryKey' in MySQL source configuration".

---

### User Story 5 - Reduce shuffle volume for large MySQL columns (Priority: P4)

An operator validating a MySQL-to-ScyllaDB migration with large text/blob columns lists them under
`validation.hashColumns` (source-side names) so they are compared by content hash.

**Why this priority**: Performance optimisation for a specific source.

**Independent Test**: Validate with and without `hashColumns` on a table with large blobs;
results are the same and shuffle volume is lower with hashing.

**Acceptance Scenarios**:

1. **Given** `hashColumns: [large_text_column, blob_column]` and a rename of `blob_column`,
   **When** validation runs, **Then** renames are applied automatically and the columns are
   compared via a content hash.
2. **Given** `hashColumns` containing a primary-key column, a blank name, a case-insensitive
   duplicate, or a column absent from either side, **When** validation starts, **Then** it is
   rejected.
3. **Given** `hashColumns` on a CQL or DynamoDB validation, **When** validation runs, **Then** a
   warning says it is ignored.

---

### User Story 6 - Compare per-element collection timestamps and repair missing rows (Priority: P4)

For CQL tables with non-frozen collections migrated with `preserveCollectionTimestamps`, the
Validator compares each element's TTL/WRITETIME when both endpoints can read them, and
`copyMissingRows` can write back rows that are missing from the target.

**Why this priority**: Specialised fidelity check tied to `001-cql-to-cql-migration`.

**Independent Test**: Validate a Cassandra 5.0+ → ScyllaDB 2026.2+ migration of a table with a
non-frozen map and confirm element-level WRITETIME differences are reported; validate against an
older server and confirm only values are compared.

**Acceptance Scenarios**:

1. **Given** both endpoints can read per-element metadata (Cassandra 5.0+, or ScyllaDB 2026.2+
   whose primary key has no frozen tuple, UDT or collection), **When** validation runs, **Then**
   per-element TTL/WRITETIME are compared.
2. **Given** an older server or a ScyllaDB endpoint whose primary key includes a frozen tuple, UDT
   or collection, **When** validation runs, **Then** non-frozen collection columns are compared by
   value only and a warning says so.
3. **Given** `copyMissingRows: true` and a source that can read element metadata, **When** a row is
   missing from the target, **Then** it is written back with its element TTL/WRITETIME restored.
4. **Given** `copyMissingRows: true` and a source that cannot read element metadata for such a
   table, **When** validation starts, **Then** `copyMissingRows` is rejected (the operator re-runs
   the migration instead).

---

### User Story 7 - Repair missing rows and choose the repair WRITETIME (Priority: P4)

An operator sets `validation.copyMissingRows: true` so that rows present in the source but absent
from the target are written to the target during validation (CQL, DynamoDB/Alternator and MySQL
validations), and for CQL migrations with `preserveTimestamps: true` chooses the WRITETIME of the
repair writes with `validation.repairWritetimeStrategy`.

**Why this priority**: Convenience reconciliation for small gaps; risky if misused.

**Independent Test**: Delete a few target rows, validate with `copyMissingRows: true`, check the
log line "N missing row(s) copied", re-validate and observe zero missing rows.

**Acceptance Scenarios**:

1. **Given** `copyMissingRows: true`, **When** some source rows are missing from the target,
   **Then** only those rows are written to the target (rows with differing values are reported,
   never overwritten), the number of copied rows is logged, and the failures reported in this run
   still include the missing rows (they were collected before the copy).
2. **Given** `repairWritetimeStrategy` omitted, **When** missing rows are repaired with
   `preserveTimestamps: true`, **Then** the original source WRITETIME is used (`source`).
3. **Given** `repairWritetimeStrategy: coordinator`, **When** rows are repaired, **Then** the
   current coordinator time is used as WRITETIME.
4. **Given** `repairWritetimeStrategy: config` and `copyMissingRows: true` without
   `target.writeWritetimestampInuS`, **When** validation starts, **Then** it fails with
   "repairWritetimeStrategy=config requires target.writeWritetimestampInuS".
5. **Given** a non-`source` strategy while `preserveTimestamps` is disabled, **When** rows are
   repaired, **Then** a warning says the strategy is ignored.

### Edge Cases

- Live writes during validation: the Validator may scan using multiple statements over time and
  provides no single point-in-time snapshot. Writes MUST be stopped on both source and target
  (documented explicitly for MySQL — also logged as a warning at every MySQL validation — and for
  ScyllaDB per-element collection lookups, where values and metadata are read at slightly
  different times).
- Uniqueness verification itself requires writes to be stopped throughout the scan.
- TTLs are read as remaining seconds; long validations see the clock advance, absorbed by
  `ttlToleranceMillis`.
- ScyllaDB 2026.2+ per-element lookups cost ceil(n / 100) synchronous queries per collection per
  row on each ScyllaDB side; validation is noticeably slower.
- `verifySourceJoinKeyUniqueness` adds a full source scan and shuffle; source rows are persisted in
  Spark memory and disk for reuse, then released on success or failure; an existing caller-owned
  persistence setting is preserved.
- `acknowledgeUnverifiedJoinKey` does not establish uniqueness: duplicate source rows can produce
  field mismatches against one target row, and with `copyMissingRows` duplicates can be written
  under the same target key (last write wins).
- `hashColumns` reduces Spark join/shuffle volume but the original payload columns are still read
  from MySQL and ScyllaDB before hashing.
- Custom join-key options affect validation only; they do not change how the migration reads MySQL.
- Extra rows that exist only in the target are detected only for MySQL→ScyllaDB validations, and
  not when the MySQL source has a `where` filter; CQL and DynamoDB/Alternator validations compare
  source rows against the target and do not report target-only rows.
- `copyMissingRows` cannot distinguish rows that were never migrated from rows deliberately deleted
  on the target only; enabling it can resurrect such rows.
- For non-frozen collections, `copyMissingRows` repairs only rows entirely absent from the target;
  rows whose collections differ are reported but not converged (a warning says so).
- DynamoDB/Alternator validation ignores `compareTimestamps`, TTL/WRITETIME and timestamp
  tolerances (DynamoDB has no such metadata); the target table is scanned with the source's
  `scanSegments`, `maxMapTasks`, `readThroughput` and `throughputReadPercent`.
- Unsupported source/target pairs (see FR-003).

## Requirements *(mandatory)*

### Functional Requirements

**Entry point and invocation**

- **FR-001**: System MUST provide a Validator entry point `com.scylladb.migrator.Validator`,
  submitted as the same Spark job and assembly JAR as the migration with a different `--class`,
  configured through `spark.scylla.config`; it MUST log the loaded configuration with secrets
  redacted.
- **FR-002**: The Validator MUST be runnable in the Ansible setup (`submit-cql-job-validator.sh`,
  and `submit-alternator-validator.sh` per `getting-started/ansible.rst`), manual Spark standalone,
  AWS EMR (custom-JAR Cluster Step, `--deploy-mode cluster`) and Docker Compose setups.
- **FR-003**: System MUST validate exactly these source/target pairs: CQL (`cassandra`/`scylla`)
  → `scylla`; `dynamodb`/`alternator` → `dynamodb`/`alternator`; `mysql` → `scylla`. Any other
  pair — notably `parquet`, `aerospike` and `dynamodb-s3-export` sources, and `parquet` /
  `dynamodb-s3-export` targets — MUST be rejected at Validator start with "Unsupported combination
  of source and target". These gaps MUST be documented (constitution principle I).

**Validation configuration**

- **FR-004**: The top-level `validation` section MUST be optional in migration mode; in validation
  mode its absence MUST fail validation with "Missing required property 'validation' in the
  configuration file." before any comparison.
- **FR-004a**: `compareTimestamps`, `ttlToleranceMillis`, `writetimeToleranceMillis`,
  `failuresToFetch`, `floatingPointTolerance` and `timestampMsTolerance` MUST all be present when
  the section is given (no defaults). At load, System MUST reject `failuresToFetch ≤ 0`, negative
  `ttlToleranceMillis` / `writetimeToleranceMillis` / `timestampMsTolerance`, and a negative, NaN
  or infinite `floatingPointTolerance`, reporting all problems together.
- **FR-005**: `validation.compareTimestamps` MUST control whether WRITETIMEs and TTLs are compared
  (CQL→CQL validation only).
- **FR-006**: `validation.ttlToleranceMillis` MUST set the allowed TTL difference (documented value
  60000).
- **FR-007**: `validation.writetimeToleranceMillis` MUST set the allowed WRITETIME difference
  (documented value 1000); reported WRITETIME differences are expressed in microseconds.
- **FR-008**: `validation.failuresToFetch` MUST bound how many differences are collected and
  printed (documented value 100); counts in the summary refer to this sample.
- **FR-009**: `validation.floatingPointTolerance` MUST set the allowed difference between
  floating-point/decimal values (documented value 0.001); integral values MUST be compared exactly.
- **FR-010**: `validation.timestampMsTolerance` MUST set the allowed difference in milliseconds
  between timestamp values (documented value 0; CQL and MySQL validations).
- **FR-011**: `validation.hashColumns` MUST be optional, apply to MySQL-to-ScyllaDB validation,
  take source-side column names, apply `renames` automatically, and compare those columns via a
  content hash. Blank or case-insensitively duplicated names MUST be rejected at load; names that
  are primary-key columns or missing on either side MUST be rejected at validation start; for
  other validators a warning MUST say it is ignored.
- **FR-012**: The Validator MUST log "No comparison failures found" and exit with status 0 when
  no difference is found. When differences are found it MUST log an error summary "Found N
  comparison failure(s) in sample: …" broken down by kind (missing target row, extra target row,
  differing field values, numeric type mismatch, mismatched column count, mismatched column
  names), log each failure (source row, target row, failure items), and exit with status 1. Any
  configuration or runtime error MUST terminate the job unsuccessfully.
- **FR-012a**: `validation.numericTypePolicy` MUST be optional, accept `Lenient` (default),
  `DetectWiden` or `StrictType` (case-insensitive; other values rejected at load), and control
  whether numeric type changes between equal values are reported as numeric type mismatches
  (never / only float↔double widening that changes the value / any differing numeric type).

**MySQL validation**

- **FR-013**: For MySQL sources, System MUST require `source.primaryKey` and by default check that
  it matches the MySQL table's declared primary key, including column order (column names compared
  case-insensitively); a table that exposes no primary key MUST be rejected unless
  `skipSourcePrimaryKeyValidation` is set.
- **FR-014**: `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness` and
  `acknowledgeUnverifiedJoinKey` MUST default to `false` and affect validation only.
- **FR-015**: Custom join-key columns MUST exist in the source and, after `renames`, MUST match the
  ScyllaDB target primary key; `skipSourcePrimaryKeyValidation` MUST bypass only the check against
  MySQL's declared primary key.
- **FR-016**: When `skipSourcePrimaryKeyValidation` is `true`, System MUST refuse to validate
  unless `verifySourceJoinKeyUniqueness` or `acknowledgeUnverifiedJoinKey` is `true`, and MUST log
  a warning when proceeding on acknowledgement only.
- **FR-017**: With `verifySourceJoinKeyUniqueness: true`, System MUST scan the selected source rows
  (respecting a source `where` filter) and reject duplicate key combinations or null key components
  before target validation.
- **FR-018**: If both safeguards are enabled, verification MUST take precedence; both MUST have no
  effect when `skipSourcePrimaryKeyValidation` is `false`.
- **FR-019**: Source rows persisted for uniqueness verification MUST be released on success or
  failure, preserving any caller-owned persistence setting.
- **FR-019a**: MySQL validation MUST reject renames that map several source columns (or several
  primary-key columns) to the same target name, and renamed source columns that do not exist in
  the target table.
- **FR-019b**: MySQL validation MUST report target rows absent from the source (extra target rows)
  within the `failuresToFetch` budget, except when the source has a `where` filter (then a warning
  says extra-row detection is disabled).

**Collections and repair**

- **FR-020**: For non-frozen CQL collections, System MUST compare per-element TTL/WRITETIME when
  both endpoints can read them (Cassandra 5.0+ in the scan; ScyllaDB 2026.2+ via per-element point
  lookups after the scan), and otherwise compare by value only (with a warning).
- **FR-021**: `validation.copyMissingRows` MUST be optional (default `false`); when `true`, rows
  present in the source but missing from the target MUST be written to the target for CQL,
  DynamoDB/Alternator and MySQL validations, rows with differing values MUST NOT be overwritten,
  and the number of copied rows MUST be logged. For tables with non-frozen collections it MUST
  restore element TTL/WRITETIME when the source can read them and MUST be rejected otherwise.
- **FR-022**: `validation.repairWritetimeStrategy` MUST be optional, accept `source` (default),
  `coordinator` or `config` (case-insensitive; other values rejected at load), and apply only to
  CQL validations with `copyMissingRows: true` and `preserveTimestamps: true`: `source` keeps the
  original WRITETIME, `coordinator` uses the current coordinator time, `config` uses
  `target.writeWritetimestampInuS` and MUST fail at start when that property is absent. A
  non-`source` value without `preserveTimestamps` MUST be ignored with a warning.

### Key Entities

- **Validator**: separate entry point of the migrator application that compares source and target
  and signals the result through logs and exit status.
- **Validation config**: required `compareTimestamps`, `ttlToleranceMillis`,
  `writetimeToleranceMillis`, `failuresToFetch`, `floatingPointTolerance`, `timestampMsTolerance`;
  optional `hashColumns`, `copyMissingRows`, `repairWritetimeStrategy`, `numericTypePolicy`.
- **Difference (comparison failure)**: a source row missing in the target, a target-only row
  (MySQL only), a row with a different column count or names (DynamoDB), or a row whose fields,
  numeric types, TTLs or WRITETIMEs differ beyond tolerance.
- **Join key**: columns used to match source and target rows; by default the source primary key,
  or a custom MySQL `source.primaryKey`.
- **Join-key safeguards**: `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness`,
  `acknowledgeUnverifiedJoinKey`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For a quiesced, correctly migrated table, the Validator reports zero differences and
  exits with status 0.
- **SC-002**: For a target with N injected differences (N ≤ `failuresToFetch`), the Validator
  reports all N and exits with status 1; for N > `failuresToFetch`, it prints exactly
  `failuresToFetch`.
- **SC-003**: Differences within configured tolerances produce zero reported failures.
- **SC-004**: 100% of MySQL custom join-key configurations without a safeguard are refused before
  target validation; 100% of duplicate or null join keys are detected when verification is
  enabled.
- **SC-005**: The Validator can be launched in each of the four documented deployment types using
  only the documented command.
- **SC-006**: 100% of unsupported source/target pairs and invalid `validation` sections are
  rejected before any comparison runs.

## Assumptions

- Writes are stopped on source and target before and during validation (operator
  responsibility).
- The validation configuration reuses the migration's `source`, `target` and `renames` (and, for
  DynamoDB-family sources, the `savepoints` section, which remains mandatory for the
  configuration to load).
- Per-element collection timestamp migration is specified in `001-cql-to-cql-migration`; MySQL
  source configuration in `006-mysql-to-cql-migration`; renames in `007-column-renames`; cluster
  setup and job submission in `010-spark-cluster-and-job-submission`.
- Automatic remediation beyond `copyMissingRows` (e.g. deleting extra target rows) is out of
  scope.

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Supported pairs | Not enumerated | CQL→scylla, DynamoDB/Alternator→DynamoDB/Alternator, MySQL→scylla; everything else "Unsupported combination" (`Validator.scala:23-38`) | Code (FR-003) | List supported pairs; document Parquet, Aerospike, DynamoDB S3 export and Parquet/S3-export targets as unvalidated. |
| 2 | Outcome signalling | Not stated ("print differences") | Exit 0 with "No comparison failures found"; otherwise error summary with per-kind counts, each failure logged, `System.exit(1)` (`Validator.scala:67-104`) | Code (FR-012) | Document exit status and log format for automation. |
| 3 | `copyMissingRows` | Only mentioned in prose | `validation.copyMissingRows`, default `false`; supported by CQL, DynamoDB and MySQL validators; failures are pre-copy snapshot (`config/Validation.scala:58`; `alternator/AlternatorValidator.scala:109-157`; `scylla/MySQLToScyllaValidator.scala:1118-1141`) | Code (FR-021, US7) | Add to the Validation reference with the resurrection warning. |
| 4 | `repairWritetimeStrategy` | Not documented | `source`/`coordinator`/`config`; `config` needs `target.writeWritetimestampInuS` (`config/RepairWritetimeStrategy.scala:28-37`; `scylla/ScyllaValidator.scala:343-352,637-646,714-740`) | Code (FR-022) | Document property and values. |
| 5 | `numericTypePolicy` | Not documented | `Lenient` (default) / `DetectWiden` / `StrictType` (`config/Validation.scala:61`; `validation/core/NumericComparison.scala:3-79`) | Code (FR-012a) | Document property and semantics. |
| 6 | Missing `validation` in validation mode | configuration.rst: section "mandatory" in validation mode (spec previously assumed a decoding error) | Not a decoding error; each validator fails at start with "Missing required property 'validation'…" (`scylla/ScyllaValidator.scala:334`; `alternator/AlternatorValidator.scala:69`; `scylla/MySQLToScyllaValidator.scala:702`) | Code (FR-004, US1 AS4) | Fine as is; optionally quote the message. |
| 7 | Required validation fields and ranges | Example values only | All six base fields required; `failuresToFetch > 0`, non-negative tolerances, finite `floatingPointTolerance`, `hashColumns` non-blank and unique (`config/Validation.scala:51-113`) | Code (FR-004a, FR-011) | Mark required fields and constraints. |
| 8 | "exactly the same items" | validate.rst intro implies full equality check | Target-only rows detected only for MySQL (and not with `where`); CQL and DynamoDB use left joins (`scylla/ScyllaValidator.scala:553`; `alternator/AlternatorValidator.scala:89`; `scylla/MySQLToScyllaValidator.scala:839-842,1100-1110`) | Code (FR-019b, Edge Cases) | State that extra target rows are not reported for CQL/DynamoDB. |
| 9 | `compareTimestamps` / tolerances scope | Generic | Timestamps/TTLs compared only for CQL; DynamoDB validator uses only `floatingPointTolerance` and `numericTypePolicy`; target scan reuses source scan settings (`alternator/AlternatorValidator.scala:51-62,96-104`) | Code (FR-005, Edge Cases) | Note per-validator applicability. |
| 10 | MySQL `primaryKey` and column checks | Custom join-key section only | `primaryKey` required for validation; renamed source columns must exist in target; rename collisions rejected; `hashColumns` must not overlap PK; live-write warning logged (`scylla/MySQLToScyllaValidator.scala:700-858`) | Code (FR-013, FR-019a, FR-011) | Document these start-up checks. |
| 11 | WRITETIME difference units | `writetimeToleranceMillis` in ms | Tolerance converted to µs; differences printed in micros (`validation/RowComparisonFailure.scala:60-71`) | Code (FR-007) | Mention units in output. |

Verified unchanged: entry point class and `spark.scylla.config`, Ansible submit scripts
(`ansible/templates/submit-cql-job-validator.sh`, `submit-alternator-validator.sh`), documented
validation properties and their meanings, `hashColumns` semantics, MySQL join-key safeguards and
their precedence, uniqueness verification respecting `where`, persistence release, and per-element
collection comparison / `copyMissingRows` rejection rules match the docs.
