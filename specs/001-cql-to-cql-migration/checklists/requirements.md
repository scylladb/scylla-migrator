# Specification Quality Checklist: CQL-to-CQL Migration (Cassandra, ScyllaDB, DataStax Astra)

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-11-27
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — YAML property names, CQL
  consistency levels and `spark-submit` flags are kept because they are the user-facing contract;
  no internal classes or libraries are described.
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders — as far as a database-migration tool allows; CQL
  terms (TTL, WRITETIME, frozen collection) are domain vocabulary for the operators who use it.
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain — the `writeTTLInS`/`writeWritetimestampInuS` vs
  `preserveTimestamps: true` marker was resolved from code (forced values are silently ignored when
  timestamps are preserved; FR-027a).
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details) — they refer to the
  Validator and to user-visible configuration only.
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded — renames, savepoints and validation are delegated to sibling specs.
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

- Items marked incomplete require spec updates before `/speckit-clarify` or `/speckit-plan`
- Reviewed against source code 2026-11-27 (commit 06835d0). Code-derived corrections: CQL target
  tables are never created (FR-028), `scylla` source alias (FR-001), Parquet target allowed
  (FR-003), consistency-level range and late validation (FR-010/010a), optional
  `splitCount`/`connections`, host/port/bundle validation, extra `preserveTimestamps` rejections,
  collection-size limits, `dropNullPrimaryKeys`. Docs/code disagreements are tabulated in the
  spec's "Docs vs Code Review" section.
- Baseline spec originally derived from existing docs.
- Docs discrepancy noted in Assumptions: `migrate-from-cassandra-or-parquet.rst` says
  `preserveTimestamps` is incompatible with all collections, while `configuration.rst` supports frozen
  collections and opt-in non-frozen set/map support; the spec follows the configuration reference.
