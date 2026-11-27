# Feature Specification: Migration Validation

**Feature Branch**: `009-migration-validation`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/validate.rst` (whole page: introduction, "MySQL custom join keys", validation properties, "Run the Validator in the Ansible-based Setup", "... in a Manual Spark Setup", "... in AWS EMR", "... in a Docker-based Spark Setup"); `docs/source/configuration.rst` sections "Overview" (`validation` top-level property) and "Validation", and the `preserveCollectionTimestamps` comment in "CQL-Compatible Source" (validation and `copyMissingRows` paragraph); `docs/source/getting-started/ansible.rst` (validator submit scripts).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Check that the target matches the source after a migration (Priority: P1)

After a migration finishes, an operator submits the same Spark job with the Validator entry point
(`--class com.scylladb.migrator.Validator`) and the same configuration file, extended with a
`validation` section. The Validator compares source and target and reports differences, printing
up to `failuresToFetch` of them.

**Why this priority**: Validation is how operators prove that no data was lost or corrupted
(constitution principle I).

**Independent Test**: Migrate a table, run the Validator and observe that no differences are
reported; then delete or modify a row in the target, re-run, and observe the difference reported.

**Acceptance Scenarios**:

1. **Given** a completed migration and a configuration with a `validation` section, **When** the
   operator runs `spark-submit --class com.scylladb.migrator.Validator --conf
   spark.scylla.config=<config.yaml> <scylla-migrator-assembly.jar>`, **Then** the Validator
   compares source and target and reports whether the target contains exactly the same items.
2. **Given** the target has differences, **When** validation runs with `failuresToFetch: 100`,
   **Then** at most 100 differences are fetched and printed.
3. **Given** the application runs in migration mode, **When** the configuration has no
   `validation` section, **Then** the configuration is still valid (the section is mandatory only
   in validation mode).
4. **Given** the application runs in validation mode, **When** the configuration has no
   `validation` section, **Then** startup fails with a decoding error naming the missing property.

---

### User Story 2 - Tolerate expected differences (Priority: P2)

An operator configures how strictly values are compared: whether WRITETIMEs and TTLs are compared
at all (`compareTimestamps`) and with which tolerances (`ttlToleranceMillis`,
`writetimeToleranceMillis`), and the tolerance for floating-point numbers
(`floatingPointTolerance`) and timestamps (`timestampMsTolerance`).

**Why this priority**: Without tolerances, TTL clock drift during long migrations and
floating-point representation would produce false positives.

**Independent Test**: Migrate a table with TTLs, wait some seconds, validate with
`ttlToleranceMillis: 60000` (no failures) and with `ttlToleranceMillis: 0` (failures).

**Acceptance Scenarios**:

1. **Given** `compareTimestamps: true` and `ttlToleranceMillis: 60000`, **When** a target TTL
   differs from the source TTL by less than 60 s, **Then** it is not reported.
2. **Given** `writetimeToleranceMillis: 1000`, **When** WRITETIMEs differ by more than 1 s,
   **Then** the row is reported.
3. **Given** `compareTimestamps: false`, **When** validation runs, **Then** WRITETIMEs and TTLs are
   not compared.
4. **Given** `floatingPointTolerance: 0.001`, **When** two floating-point values differ by less
   than 0.001, **Then** they are considered equal.
5. **Given** `timestampMsTolerance: 0`, **When** timestamp values differ by any amount, **Then**
   they are reported.

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
   declared primary key (including column order), **Then** validation rejects the configuration.
2. **Given** `skipSourcePrimaryKeyValidation: true` with neither safeguard, **When** validation
   starts, **Then** it refuses to use the custom join key.
3. **Given** `skipSourcePrimaryKeyValidation: true` and `verifySourceJoinKeyUniqueness: true`,
   **When** the selected source rows contain a duplicate key combination or a null key component,
   **Then** validation rejects them before target validation.
4. **Given** both safeguards enabled, **When** validation runs, **Then** uniqueness verification
   takes precedence.
5. **Given** `skipSourcePrimaryKeyValidation: false`, **When** either safeguard is set, **Then**
   the safeguard has no effect.
6. **Given** custom join-key columns that, after `renames`, do not match the ScyllaDB target
   primary key, **When** validation starts, **Then** it is rejected (the override does not bypass
   the target check).
7. **Given** a MySQL source with a `where` filter, **When** uniqueness is verified, **Then** only
   the selected rows are checked.

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
   value only.
3. **Given** `copyMissingRows` and a source that can read element metadata, **When** a row is
   missing from the target, **Then** it is written back with its element TTL/WRITETIME restored.
4. **Given** `copyMissingRows` and a source that cannot read element metadata for such a table,
   **When** validation starts, **Then** `copyMissingRows` is rejected (the operator re-runs the
   migration instead).

### Edge Cases

- Live writes during validation: the Validator may scan using multiple statements over time and
  provides no single point-in-time snapshot. Writes MUST be stopped on both source and target
  (documented explicitly for MySQL, and for ScyllaDB per-element collection lookups, where values
  and metadata are read at slightly different times).
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
- Unsupported source/target pairs (see FR-003).

## Requirements *(mandatory)*

### Functional Requirements

**Entry point and invocation**

- **FR-001**: System MUST provide a Validator entry point `com.scylladb.migrator.Validator`,
  submitted as the same Spark job and assembly JAR as the migration with a different `--class`,
  configured through `spark.scylla.config`.
- **FR-002**: The Validator MUST be runnable in the Ansible setup (`submit-cql-job-validator.sh`,
  and `submit-alternator-validator.sh` per `getting-started/ansible.rst`), manual Spark standalone,
  AWS EMR (custom-JAR Cluster Step, `--deploy-mode cluster`) and Docker Compose setups.
- **FR-003**: System MUST validate the source/target pairs it supports and reject unsupported pairs
  at startup. [NEEDS CLARIFICATION: the docs do not enumerate which source/target pairs the
  Validator supports (CQL→CQL, DynamoDB/Alternator→DynamoDB/Alternator, MySQL→ScyllaDB are implied;
  Parquet, DynamoDB S3 export and Aerospike sources are not mentioned); which pairs are supported,
  and should unsupported ones be documented as gaps per constitution principle I?]

**Validation configuration**

- **FR-004**: The top-level `validation` section MUST be mandatory only when running in validation
  mode.
- **FR-005**: `validation.compareTimestamps` MUST control whether WRITETIMEs and TTLs are compared.
- **FR-006**: `validation.ttlToleranceMillis` MUST set the allowed TTL difference (documented value
  60000).
- **FR-007**: `validation.writetimeToleranceMillis` MUST set the allowed WRITETIME difference
  (documented value 1000).
- **FR-008**: `validation.failuresToFetch` MUST bound how many differences are fetched and printed
  (documented value 100).
- **FR-009**: `validation.floatingPointTolerance` MUST set the allowed difference between
  floating-point numbers (documented value 0.001).
- **FR-010**: `validation.timestampMsTolerance` MUST set the allowed difference in milliseconds
  between timestamp values (documented value 0).
- **FR-011**: `validation.hashColumns` MUST be optional, apply to MySQL-to-ScyllaDB validation,
  take source-side column names, apply `renames` automatically, and compare those columns via a
  content hash.
- **FR-012**: The Validator MUST report whether the target contains exactly the same items as the
  source and print the fetched differences. [NEEDS CLARIFICATION: the docs do not state how the
  outcome is signalled (e.g. process exit status on differences, summary counts of missing/extra
  rows), which automation needs to gate cut-over on validation.]

**MySQL validation**

- **FR-013**: For MySQL sources, by default System MUST check that `source.primaryKey` matches the
  MySQL table's declared primary key, including column order.
- **FR-014**: `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness` and
  `acknowledgeUnverifiedJoinKey` MUST default to `false` and affect validation only.
- **FR-015**: Custom join-key columns MUST exist in the source and, after `renames`, MUST match the
  ScyllaDB target primary key; `skipSourcePrimaryKeyValidation` MUST bypass only the check against
  MySQL's declared primary key.
- **FR-016**: When `skipSourcePrimaryKeyValidation` is `true`, System MUST refuse to validate
  unless `verifySourceJoinKeyUniqueness` or `acknowledgeUnverifiedJoinKey` is `true`.
- **FR-017**: With `verifySourceJoinKeyUniqueness: true`, System MUST scan the selected source rows
  (respecting a source `where` filter) and reject duplicate key combinations or null key components
  before target validation.
- **FR-018**: If both safeguards are enabled, verification MUST take precedence; both MUST have no
  effect when `skipSourcePrimaryKeyValidation` is `false`.
- **FR-019**: Source rows persisted for uniqueness verification MUST be released on success or
  failure, preserving any caller-owned persistence setting.

**Collections and repair**

- **FR-020**: For non-frozen CQL collections, System MUST compare per-element TTL/WRITETIME when
  both endpoints can read them (Cassandra 5.0+ in the scan; ScyllaDB 2026.2+ via per-element point
  lookups after the scan), and otherwise compare by value only.
- **FR-021**: `copyMissingRows` MUST write rows present in the source but missing from the target
  back to the target; for tables with non-frozen collections it MUST restore element TTL/WRITETIME
  when the source can read them and MUST be rejected otherwise. The property's location and default
  are not listed in the documented `validation` properties (see Assumptions).

### Key Entities

- **Validator**: separate entry point of the migrator application that compares source and target.
- **Validation config**: `compareTimestamps`, `ttlToleranceMillis`, `writetimeToleranceMillis`,
  `failuresToFetch`, `floatingPointTolerance`, `timestampMsTolerance`, optional `hashColumns`,
  `copyMissingRows`.
- **Difference (comparison failure)**: a source row missing in the target, or a row whose fields,
  TTLs or WRITETIMEs differ beyond tolerance.
- **Join key**: columns used to match source and target rows; by default the source primary key,
  or a custom MySQL `source.primaryKey`.
- **Join-key safeguards**: `skipSourcePrimaryKeyValidation`, `verifySourceJoinKeyUniqueness`,
  `acknowledgeUnverifiedJoinKey`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For a quiesced, correctly migrated table, the Validator reports zero differences.
- **SC-002**: For a target with N injected differences (N ≤ `failuresToFetch`), the Validator
  reports all N; for N > `failuresToFetch`, it prints exactly `failuresToFetch`.
- **SC-003**: Differences within configured tolerances produce zero reported failures.
- **SC-004**: 100% of MySQL custom join-key configurations without a safeguard are refused before
  target validation; 100% of duplicate or null join keys are detected when verification is
  enabled.
- **SC-005**: The Validator can be launched in each of the four documented deployment types using
  only the documented command.

## Assumptions

- Writes are stopped on source and target before and during validation (operator
  responsibility).
- The validation configuration reuses the migration's `source`, `target` and `renames`.
- `copyMissingRows` appears in the docs only in prose (MySQL join keys, collection timestamps);
  its exact placement in the `validation` section and its default (assumed `false`, opt-in) are
  taken as given here and should be added to the configuration reference.
- Per-element collection timestamp migration is specified in `001-cql-to-cql-migration`; MySQL
  source configuration in `006-mysql-to-cql-migration`; renames in `007-column-renames`; cluster
  setup and job submission in `010-spark-cluster-and-job-submission`.
- Automatic remediation beyond `copyMissingRows` (e.g. deleting extra target rows) is out of
  scope.
