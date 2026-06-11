package com.scylladb.migrator.readers.jdbc

/** Standard warning text emitted before a partitioned JDBC read.
  *
  * Spark's JDBC source opens one connection per partition. Because each connection runs an
  * independent SELECT with its own transactional view, concurrent writes on the source table can
  * yield results that mix data from different points in time. Callers should surface this warning
  * at INFO/WARN level so operators can plan to quiesce the source before correctness-sensitive
  * migrations.
  */
object JdbcConsistencyWarning {

  /** Build the partitioned-read consistency warning. `backendName` is the human-readable backend
    * label (e.g. `"MySQL"`). `qualifiedTable` is the database-qualified table identifier (e.g.
    * `"app.user_events"`).
    */
  def partitionedReadWarning(backendName: String, qualifiedTable: String): String =
    s"Partitioned $backendName reads for $qualifiedTable use multiple independent " +
      "JDBC statements/connections and do not provide a single global snapshot across partitions. " +
      "Concurrent source writes can yield mixed-time results. For correctness-sensitive runs, " +
      "quiesce or otherwise freeze the source table before starting the migration or validation."
}
