package com.scylladb.migrator

import com.scylladb.migrator.config.{
  MigratorConfig,
  SavepointTarget,
  Savepoints,
  SourceSettings,
  TargetSettings
}

class SparkUtilsTest extends munit.FunSuite {

  test("shared Spark sessions reuse a live context and reacquire after another suite stops it") {
    val originalContext = SparkUtils.withSparkSession() { spark =>
      assertEquals(spark.range(1L).count(), 1L)
      spark.sparkContext
    }
    SparkUtils.withSparkSession() { spark =>
      assert(spark.sparkContext eq originalContext)
      assertEquals(spark.range(2L).count(), 2L)
      // Simulate teardown of a suite that borrowed this process-wide context.
      spark.stop()
    }
    assert(originalContext.isStopped)
    SparkUtils.withSparkSession() { spark =>
      assert(spark.sparkContext ne originalContext)
      assert(!spark.sparkContext.isStopped)
      assertEquals(spark.range(3L).count(), 3L)
    }
  }

  test("local execution remaps nested filesystem savepoint target path") {
    val config = baseConfig.copy(
      savepoints = Savepoints(
        intervalSeconds = 300,
        path            = "/app/savepoints/legacy",
        target          = Some(SavepointTarget.Filesystem("/app/savepoints/nested"))
      )
    )

    val remapped = SparkUtils.remapForLocalExecution(config)

    assertEquals(remapped.savepoints.path, "docker/spark-master/legacy")
    assertEquals(
      remapped.savepoints.resolvedTarget,
      SavepointTarget.Filesystem("docker/spark-master/nested")
    )
  }

  private val baseConfig: MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = "/app/parquet/data",
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla",
        port                          = 9042,
        localDC                       = Some("datacenter1"),
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "test",
        table                         = "tbl",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 300, path = "/app/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
}
