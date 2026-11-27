# Specifications

This directory holds [Spec Kit](https://github.com/github/spec-kit) feature specifications for the
ScyllaDB Migrator. Project principles live in
[`.specify/memory/constitution.md`](../.specify/memory/constitution.md).

## Baseline (as-built) specs

Specs `001`–`010` were derived from the user documentation in [`docs/source/`](../docs/source/)
on 2026-11-27. They describe behaviour that already exists, so they have no `plan.md` or
`tasks.md`. Each has a `checklists/requirements.md` quality checklist; remaining
`[NEEDS CLARIFICATION]` markers flag gaps or contradictions in the docs (resolve them with
`/speckit-clarify`).

| Spec | Scope | Derived from |
|------|-------|--------------|
| [001-cql-to-cql-migration](001-cql-to-cql-migration/spec.md) | Cassandra / ScyllaDB / DataStax Astra → CQL | `migrate-from-cassandra-or-parquet.rst`, `configuration.rst` |
| [002-parquet-to-cql-migration](002-parquet-to-cql-migration/spec.md) | Parquet (local / S3) → CQL | `migrate-from-cassandra-or-parquet.rst`, `configuration.rst` |
| [003-dynamodb-family-migration](003-dynamodb-family-migration/spec.md) | DynamoDB / Alternator / S3 export → DynamoDB / Alternator | `migrate-from-dynamodb.rst`, `configuration.rst` |
| [004-dynamodb-change-streaming](004-dynamodb-change-streaming/spec.md) | DynamoDB Streams & Kinesis change replication | `stream-changes.rst` |
| [005-aerospike-to-cql-migration](005-aerospike-to-cql-migration/spec.md) | Aerospike → CQL | `configuration.rst` |
| [006-mysql-to-cql-migration](006-mysql-to-cql-migration/spec.md) | MySQL → CQL (sparsely documented) | `configuration.rst`, `validate.rst`, `resume-interrupted-migration.rst` |
| [007-column-renames](007-column-renames/spec.md) | Renaming columns during migration | `rename-columns.rst`, `configuration.rst` |
| [008-savepoints-and-resume](008-savepoints-and-resume/spec.md) | Savepoints, backends, automatic resume | `resume-interrupted-migration.rst`, `configuration.rst` |
| [009-migration-validation](009-migration-validation/spec.md) | Validator | `validate.rst`, `configuration.rst` |
| [010-spark-cluster-and-job-submission](010-spark-cluster-and-job-submission/spec.md) | Spark cluster setup & job submission | `getting-started/`, `run-the-migration.rst`, `index.rst`, `tutorials/` |

## New features

Start new work with `/speckit-specify <description>` in Claude Code; it creates the next
numbered directory (`011-…`). Then `/speckit-clarify` (optional) → `/speckit-plan` →
`/speckit-tasks` → `/speckit-implement`. Reference the relevant baseline spec(s) above in the
new spec's Input/Assumptions so the delta is explicit.

When a change alters documented behaviour, update `docs/source/` **and** the matching
baseline spec in the same PR (constitution principle VII).
