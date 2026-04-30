package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.{
  Credentials,
  MigratorConfig,
  Savepoints,
  SourceSettings,
  TargetSettings
}
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

class MySQLSavepointsTest extends munit.FunSuite {

  test("MySQL source does not create savepoint manager even when savepoints are configured") {
    val tempDir = Files.createTempDirectory("mysql-savepoints-test")
    val savepointsDir = tempDir.resolve("savepoints")

    try {
      val config = MigratorConfig(
        source = SourceSettings.MySQL(
          host                 = "mysql.example.com",
          port                 = 3306,
          database             = "mydb",
          table                = "users",
          credentials          = Credentials("mysql-user", "mysql-secret"),
          primaryKey           = None,
          partitionColumn      = None,
          numPartitions        = None,
          lowerBound           = None,
          upperBound           = None,
          zeroDateTimeBehavior = SourceSettings.MySQL.ZeroDateTimeBehavior.Exception,
          fetchSize            = SourceSettings.MySQL.DefaultFetchSize,
          where                = None,
          connectionProperties = None
        ),
        target = TargetSettings.Scylla(
          host                          = "scylla.example.com",
          port                          = 9042,
          localDC                       = Some("datacenter1"),
          credentials                   = None,
          sslOptions                    = None,
          keyspace                      = "ks",
          table                         = "users",
          connections                   = Some(1),
          stripTrailingZerosForDecimals = false,
          writeTTLInS                   = None,
          writeWritetimestampInuS       = None,
          consistencyLevel              = "LOCAL_QUORUM"
        ),
        renames          = None,
        savepoints       = Savepoints(300, savepointsDir.toString),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      )
      val mysqlSourceDF = SourceDataFrame(
        dataFrame           = null,
        timestampColumns    = None,
        savepointsSupported = false
      )

      val manager = ScyllaMigrator.savepointsManagerForSource(config, mysqlSourceDF)(
        spark = null.asInstanceOf[SparkSession]
      )

      manager.foreach(_.close())
      assertEquals(manager, None)
      assert(!Files.exists(savepointsDir), "savepoints directory should not be created for MySQL")
    } finally {
      if (Files.exists(savepointsDir))
        Files
          .walk(savepointsDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.delete)
      Files.deleteIfExists(tempDir)
    }
  }
}
