package com.scylladb.migrator.scylla

import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.TargetSettings

/** Shared construction of Spark Cassandra Connector config options from [[TargetSettings.Scylla]].
  * This ensures consistent defaults (SSL store types, protocol, enabled cipher suites, etc.)
  * regardless of whether the options are used by the migrator, the Cassandra-to-Scylla validator,
  * or the MySQL-to-Scylla validator.
  *
  * The defaults here mirror those in [[com.scylladb.migrator.Connectors]] which builds
  * `CassandraConnectorConf` objects with the same values.
  */
object ScyllaSparkConnectionOptions {
  val MySQLValidatorCluster = "mysql_validator_target"

  private val InputConsistencyLevel = "spark.cassandra.input.consistency.level"

  private def scopeToCluster(cluster: String, options: Map[String, String]): Map[String, String] =
    options.map { case (key, value) => s"$cluster/$key" -> value }

  /** Build connector session configuration for a dedicated Spark Cassandra cluster alias.
    *
    * These settings intentionally exclude table-selection properties so that credentials and SSL
    * secrets do not need to be passed through DataFrame reader options.
    *
    * '''Cloud mode is not supported''' through this helper — cloud targets use
    * [[Connectors.buildTargetConf]] which directly constructs a `CloudBasedContactInfo`.
    *
    * @throws IllegalArgumentException
    *   if `t.cloud` is defined
    */
  def sessionConfFromTargetSettings(
    t: TargetSettings.Scylla,
    cluster: String = MySQLValidatorCluster
  ): Map[String, String] = {
    require(
      t.cloud.isEmpty,
      "ScyllaSparkConnectionOptions does not support cloud targets; " +
        "use Connectors.buildTargetConf instead."
    )
    val sessionConf =
      Connectors.sparkSessionOptions(Connectors.targetSessionOptions(t)) +
        (InputConsistencyLevel -> t.consistencyLevel)
    scopeToCluster(cluster, sessionConf)
  }

  /** Build the non-sensitive DataFrame reader options for the validator's Cassandra scan. */
  def readerOptionsFromTargetSettings(
    t: TargetSettings.Scylla,
    cluster: String = MySQLValidatorCluster
  ): Map[String, String] =
    Map(
      "keyspace" -> t.keyspace,
      "table"    -> t.table,
      "cluster"  -> cluster
    )
}
