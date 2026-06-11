package com.scylladb.migrator.readers.jdbc

class JdbcConsistencyWarningTest extends munit.FunSuite {

  test("partitionedReadWarning embeds the backend name and qualified table") {
    val warning =
      JdbcConsistencyWarning.partitionedReadWarning("Postgres", "public.user_events")

    assert(warning.contains("Partitioned Postgres reads for public.user_events"))
    assert(warning.contains("do not provide a single global snapshot"))
    assert(warning.contains("Concurrent source writes"))
    assert(warning.contains("quiesce"))
  }

  test("partitionedReadWarning is identical to legacy MySQL phrasing for backendName=MySQL") {
    val warning = JdbcConsistencyWarning.partitionedReadWarning("MySQL", "app.events")
    assertEquals(
      warning,
      "Partitioned MySQL reads for app.events use multiple independent " +
        "JDBC statements/connections and do not provide a single global snapshot across partitions. " +
        "Concurrent source writes can yield mixed-time results. For correctness-sensitive runs, " +
        "quiesce or otherwise freeze the source table before starting the migration or validation."
    )
  }
}
