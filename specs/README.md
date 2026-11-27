# Specifications

This directory holds [Spec Kit](https://github.com/github/spec-kit) feature specifications for the
ScyllaDB Migrator. Project principles live in
[`.specify/memory/constitution.md`](../.specify/memory/constitution.md).

## Baseline (as-built) specs

Specs `001`–`012` describe behaviour that already exists, so they have no `plan.md` or
`tasks.md`. Specs `001`–`010` were first derived from the user documentation in
[`docs/source/`](../docs/source/), then reviewed against the source code at commit `06835d0`.
Where docs and code disagree, the spec follows the code. Specs `011` and `012` cover migration
paths that exist only in code and are not documented at all.

Every spec ends with a **Docs vs Code Review** table that lists each discrepancy, cites the
code, and suggests a docs fix. Each spec has a `checklists/requirements.md` quality checklist.

| Spec | Scope | Derived from |
|------|-------|--------------|
| [001-cql-to-cql-migration](001-cql-to-cql-migration/spec.md) | Cassandra / ScyllaDB / DataStax Astra → CQL | `migrate-from-cassandra-or-parquet.rst`, `configuration.rst`, code |
| [002-parquet-to-cql-migration](002-parquet-to-cql-migration/spec.md) | Parquet (local / S3) → CQL | `migrate-from-cassandra-or-parquet.rst`, `configuration.rst`, code |
| [003-dynamodb-family-migration](003-dynamodb-family-migration/spec.md) | DynamoDB / Alternator / S3 export → DynamoDB / Alternator | `migrate-from-dynamodb.rst`, `configuration.rst`, code |
| [004-dynamodb-change-streaming](004-dynamodb-change-streaming/spec.md) | DynamoDB Streams & Kinesis change replication | `stream-changes.rst`, code |
| [005-aerospike-to-cql-migration](005-aerospike-to-cql-migration/spec.md) | Aerospike → CQL | `configuration.rst`, code |
| [006-mysql-to-cql-migration](006-mysql-to-cql-migration/spec.md) | MySQL → CQL | `configuration.rst`, `validate.rst`, `resume-interrupted-migration.rst`, code |
| [007-column-renames](007-column-renames/spec.md) | Renaming columns during migration | `rename-columns.rst`, `configuration.rst`, code |
| [008-savepoints-and-resume](008-savepoints-and-resume/spec.md) | Savepoints, backends, automatic resume | `resume-interrupted-migration.rst`, `configuration.rst`, code |
| [009-migration-validation](009-migration-validation/spec.md) | Validator | `validate.rst`, `configuration.rst`, code |
| [010-spark-cluster-and-job-submission](010-spark-cluster-and-job-submission/spec.md) | Spark cluster setup, job submission, Spark runtime defaults | `getting-started/`, `run-the-migration.rst`, `index.rst`, `tutorials/`, `ansible/`, code |
| [011-cql-to-parquet-export](011-cql-to-parquet-export/spec.md) | CQL → Parquet export (**undocumented**) | code only |
| [012-dynamodb-to-s3-export](012-dynamodb-to-s3-export/spec.md) | DynamoDB → DynamoDB S3 export format (**undocumented**) | code only |

## New features

Start new work with `/speckit-specify <description>` in Claude Code; it creates the next
numbered directory (`013-…`). Then `/speckit-clarify` (optional) → `/speckit-plan` →
`/speckit-tasks` → `/speckit-implement`. Reference the relevant baseline spec(s) above in the
new spec's Input/Assumptions so the delta is explicit.

When a change alters documented behaviour, update `docs/source/` **and** the matching
baseline spec in the same PR (constitution principle VII).
