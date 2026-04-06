package com.scylladb.migrator

import com.scylladb.migrator.config.{
  Credentials,
  MigratorConfig,
  Savepoints,
  SourceSettings,
  TargetSettings
}

class ValidatorLoggingTest extends munit.FunSuite {

  test("loadedConfigLogMessage redacts source and target secrets") {
    val config = MigratorConfig(
      source = SourceSettings.MySQL(
        host                 = "mysql.example.com",
        port                 = 3306,
        database             = "app",
        table                = "users",
        credentials          = Credentials("mysql-user", "mysql-secret"),
        primaryKey           = Some(List("id")),
        partitionColumn      = None,
        numPartitions        = None,
        lowerBound           = None,
        upperBound           = None,
        fetchSize            = 1000,
        where                = None,
        connectionProperties = Some(Map("trustStorePassword" -> "tls-secret"))
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = Some(Credentials("scylla-user", "scylla-secret")),
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "users",
        connections                   = Some(4),
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(300, "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )

    val logMessage = Validator.loadedConfigLogMessage(config)

    assert(logMessage.startsWith("Loaded config:\n"))
    assert(logMessage.contains("<redacted>"))
    assert(!logMessage.contains("mysql-secret"))
    assert(!logMessage.contains("scylla-secret"))
    assert(!logMessage.contains("tls-secret"))
  }
}
