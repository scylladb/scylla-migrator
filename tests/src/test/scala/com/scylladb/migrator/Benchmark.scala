package com.scylladb.migrator

/** Used for tagging benchmark test suites that should not run in regular CI */
class Benchmark extends munit.Tag("Benchmark")
