package com.scylladb.migrator

/** Tag for Cassandra compatibility tests (non-default versions: 2.x, 3.x, 5.x).
  *
  * These tests are run by the dedicated cassandra-compat CI matrix job. Excluded from the main
  * Scylla integration test job to avoid duplication.
  */
class CassandraCompat extends munit.Tag("CassandraCompat")
