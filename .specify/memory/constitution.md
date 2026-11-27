<!--
Sync Impact Report
==================
Version change: (template) → 1.0.0
Ratification: initial constitution derived from the existing user documentation in
`docs/source/` and the repository instructions in `AGENTS.md` / `CONTRIBUTING.md`.
Principles added:
  I.   Data Correctness Is Non-Negotiable
  II.  Resumable, Idempotent Migrations
  III. Configuration Is the Contract
  IV.  Fail Fast, Explain Clearly
  V.   Scale Out, Never Saturate
  VI.  Secrets Stay Secret
  VII. Documentation and Tests Ship With the Change
Sections added: Technology & Compatibility Constraints, Development Workflow & Quality Gates,
Governance.
Templates reviewed:
  ✅ .specify/templates/spec-template.md   — no change needed (generic; principles apply via Constitution Check)
  ✅ .specify/templates/plan-template.md   — no change needed (Constitution Check gate reads this file)
  ✅ .specify/templates/tasks-template.md  — no change needed
Follow-up TODOs: none.
-->

# ScyllaDB Migrator (Hydra) Constitution

The ScyllaDB Migrator is an Apache Spark application that copies data from Apache Cassandra,
ScyllaDB, DataStax Astra, Parquet, DynamoDB, ScyllaDB Alternator, DynamoDB S3 exports,
Aerospike and MySQL into ScyllaDB (CQL or Alternator) or DynamoDB, optionally keeps the target
in sync by streaming changes, and validates the result. Every feature specified in `specs/`
MUST comply with the principles below.

## Core Principles

### I. Data Correctness Is Non-Negotiable

The migrator exists to move data without loss or silent corruption.

- Every row/item read from the source MUST either be written to the target, or be skipped
  by an explicit, documented, user-selected policy (e.g. `invalidRecordHandling: skip`) that
  is logged.
- Value fidelity MUST be preserved wherever the target can represent it: types, TTLs and
  WRITETIMEs (when `preserveTimestamps` is requested), collection element metadata (when
  `preserveCollectionTimestamps` is requested), decimal values, binary keys.
- Lossy conversions (numeric overflow, rounding, truncation, padding) MUST be treated as
  invalid values or be opt-in (e.g. `stripTrailingZerosForDecimals`), never applied silently.
- Known residual-risk windows (DynamoDB Streams 24 h retention, driver clock skew for
  Kinesis `AT_TIMESTAMP`, source writes during collection-timestamp migration) MUST be
  documented with their failure mode and a mitigation.
- A Validator MUST exist for every supported source/target pair, or the gap MUST be
  documented.

### II. Resumable, Idempotent Migrations

Large migrations get interrupted; restarts MUST be cheap and safe.

- Resumable sources (CQL, DynamoDB/Alternator, Parquet) MUST persist progress in savepoints
  at a configurable interval, and re-running with the **same launch configuration** MUST
  continue where the previous run left off (automatic resume, on by default).
- Writes MUST be idempotent on the target primary key so that replays (resume, at-least-once
  scans, stream replays after clock skew) only cost extra load, never wrong data.
- Sources that cannot resume (Aerospike, MySQL, DynamoDB S3 export) MUST say so in the
  documentation and MUST NOT pretend to honour a `savepoints` section.
- Savepoint formats and configuration identity MUST stay backward compatible across
  releases unless a breaking change is explicitly called out with an upgrade path.

### III. Configuration Is the Contract

The YAML configuration file is the migrator's public API.

- Every user-visible behaviour MUST be controlled through a documented configuration
  property, `spark-submit` argument, or documented environment variable.
- Existing configuration files MUST keep working after an upgrade (e.g. boolean
  `streamChanges` remains valid alongside the object form). Aliases (`scylla`/`cassandra`,
  `target-table`/`target-database`, `kinesis`/`kinesis-data-streams`) MUST stay accepted.
- Defaults MUST be safe and documented; optional properties are marked "Optional" in the
  configuration reference.
- Supported source/target combinations MUST be enumerated, and unsupported combinations
  MUST be rejected at startup.

### IV. Fail Fast, Explain Clearly

- Invalid or contradictory configuration (missing required field, `cloud` together with
  `host`, ARN/region mismatch, `initialTimestamp` far in the future, `streamChanges` without
  `streamSource`, unsupported collection types) MUST be rejected **before** any data is
  read or written, with a message that names the offending property.
- Runtime errors that cannot be handled per-record (connection, exhausted retries, write
  failures) MUST be fatal rather than silently dropping data.
- Per-record problems MUST be logged with enough context to locate the record, and the
  number of skipped/dropped items MUST be reported.

### V. Scale Out, Never Saturate

- Throughput MUST scale horizontally with the Spark cluster (splits, scan segments,
  executors) and MUST be bounded by user-controllable throttles (`connections`,
  `fetchSize`, `throughputReadPercent`/`throughputWritePercent`, `spark.cores.max`).
- The migrator MUST NOT, by default, overwhelm the source or target database; default
  rates are conservative (e.g. 0.5 of DynamoDB capacity).
- Memory- or throughput-heavy modes (collection timestamp preservation, uniqueness
  verification) MUST document their cost and offer batching/tuning knobs.

### VI. Secrets Stay Secret

- Credentials (CQL passwords, AWS keys, Aerospike credentials, GCS key files) MUST NEVER be
  written to logs, savepoints or metadata tables in clear text; savepoints store `<redacted>`.
- Resume MUST take credentials from the live launch configuration, never from a savepoint.
- Any derived value that can leak information (e.g. the unsalted SHA-256 of a `where`
  filter) MUST be documented with its exposure.
- TLS and AssumeRole options MUST be available wherever the underlying protocol supports
  them.

### VII. Documentation and Tests Ship With the Change

- Any change in configuration or user-visible behaviour MUST update `docs/source/`,
  `config.yaml.example` and the affected YAML examples in the same change.
- Every feature MUST have unit tests (`make test-unit`) and, when it touches a migration
  path, integration tests against Dockerised services (`make test`) and the E2E sanity
  suite (`make test-benchmark-e2e-sanity`). AWS-only behaviour MUST have `AWS`-category
  tests.
- Specs in `specs/` MUST stay consistent with `docs/source/`; when they diverge, the
  discrepancy MUST be recorded as a `[NEEDS CLARIFICATION]` and resolved.

## Technology & Compatibility Constraints

- Language/runtime: Scala 2.13, Java 17+ (JDK 17 APIs are used), sbt build producing
  `scylla-migrator-assembly.jar`.
- Runtime platform: Apache Spark — Migrator 2.x requires Spark 4.0.x; 1.x and 0.9.x require
  Spark 3.5.x. The compatibility matrix in `docs/source/index.rst` MUST be kept current.
- Spark, Hadoop and AWS SDK versions in `build.sbt` MUST remain mutually compatible.
- Entry points are `com.scylladb.migrator.Migrator` and `com.scylladb.migrator.Validator`,
  configured through `spark.scylla.config`.
- Supported deployments: Docker Compose (single machine), manual Spark standalone, Ansible
  (Ubuntu 22.04/24.04), and managed Spark (e.g. AWS EMR).
- Code style: `.scalafmt.conf`, two-space indentation, 100-column lines; `make lint` MUST
  pass.

## Development Workflow & Quality Gates

1. Feature work follows Spec Kit: `/speckit-specify` → (`/speckit-clarify`) →
   `/speckit-plan` → `/speckit-tasks` → `/speckit-implement`.
2. Every `plan.md` MUST include a Constitution Check against principles I–VII; deviations
   are listed in its Complexity Tracking table with a justification.
3. Before every push: `make lint-fix`, `make lint`, `make build`, then the relevant tests
   (`make test-unit`, `make test`, and `make test-benchmark-e2e-sanity` for migration
   changes). Report the checks run, failed, and skipped (with reasons).
4. Pull requests are opened from a personal fork against the upstream repository and go
   through the automated review/resolver bots described in `AGENTS.md`.

## Governance

- This constitution supersedes ad-hoc practice for anything developed through Spec Kit in
  this repository. `AGENTS.md` and `CONTRIBUTING.md` remain the runtime development
  guidance and MUST NOT contradict it.
- Amendments are made via `/speckit-constitution`, recorded in the Sync Impact Report at
  the top of this file, and versioned with semantic versioning: MAJOR for removing or
  redefining a principle, MINOR for adding a principle or section, PATCH for wording.
- Reviewers MUST verify compliance with principles I–VII on every PR that carries a spec;
  any added complexity MUST be justified in the plan.

**Version**: 1.0.0 | **Ratified**: 2026-11-27 | **Last Amended**: 2026-11-27
