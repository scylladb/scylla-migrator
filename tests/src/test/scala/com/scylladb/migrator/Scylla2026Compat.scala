package com.scylladb.migrator

/** Tag for tests that require ScyllaDB >= 2026.2 (per-element non-frozen collection
  * WRITETIME()/TTL() via the subscript form).
  *
  * These run in a dedicated CI job that starts the pinned `scylla2026` service. They are excluded
  * from the main Scylla integration job, whose shared `scylla` service tracks SCYLLA_VERSION and
  * may be older (and uses SimpleStrategy fixtures that 2026.2 rejects).
  */
class Scylla2026Compat extends munit.Tag("Scylla2026Compat")
