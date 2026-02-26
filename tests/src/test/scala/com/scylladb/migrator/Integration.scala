package com.scylladb.migrator

/** Used for tagging test suites that require external services (Scylla, Cassandra, Spark, etc.) */
class Integration extends munit.Tag("Integration")
