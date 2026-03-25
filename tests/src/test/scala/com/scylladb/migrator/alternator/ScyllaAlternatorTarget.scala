package com.scylladb.migrator.alternator

/** Describes a Scylla Alternator target used in integration tests.
  *
  * @param label
  *   Human-readable label (e.g. "new Scylla with vnodes")
  * @param alternatorPort
  *   Host port mapped to the Docker container's Alternator port (8000)
  * @param cqlPort
  *   Host port mapped to the Docker container's CQL port (9042), used for readiness checks
  * @param dockerHost
  *   Docker Compose service name used as the hostname in Spark config files
  */
case class ScyllaAlternatorTarget(
  label: String,
  alternatorPort: Int,
  cqlPort: Int,
  dockerHost: String)

object ScyllaAlternatorTarget {

  /** New Scylla (> 2025.1.4) with vnodes keyspace (tablets disabled). */
  val VnodesNewScylla =
    ScyllaAlternatorTarget("new Scylla (vnodes)", 8000, 9042, "scylla")

  /** New Scylla (> 2025.1.4) with tablets keyspace (tablets enabled). */
  val TabletsNewScylla =
    ScyllaAlternatorTarget("new Scylla (tablets)", 8004, 9049, "scylla-tablets")

  /** Old Scylla (< 2025.1.4) that does not reject INDEXES consumed capacity. */
  val OldScylla =
    ScyllaAlternatorTarget("old Scylla", 8003, 9048, "scylla-old")

}
