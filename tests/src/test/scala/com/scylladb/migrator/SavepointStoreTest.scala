package com.scylladb.migrator

import com.scylladb.migrator.config.{ MigratorConfig, Savepoints, SourceSettings, TargetSettings }

class SavepointStoreTest extends munit.FunSuite {

  test("target store newestValidConfig returns newest valid savepoint") {
    val older = config(skipFiles = Set("older")).render
    val newer = config(skipFiles = Set("newer")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, older),
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 2L, newer)
      ),
      warnInvalidRow = (_, _) => ()
    )

    assertEquals(
      selected.flatMap(_.skipParquetFiles),
      Some(Set("newer"))
    )
  }

  test("target store newestValidConfig skips corrupt latest rows") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    val valid = config(skipFiles = Set("valid")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepoint(3000L, 1L, "not: [valid"),
        ScyllaTargetSavepointStore.StoredSavepoint(2000L, 1L, valid)
      ),
      warnInvalidRow = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(selected.flatMap(_.skipParquetFiles), Some(Set("valid")))
    assertEquals(warnings.toSeq, Seq((3000L, 1L)))
  }

  private def config(skipFiles: Set[String]): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = "s3a://bucket/data",
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "tbl",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = Some(skipFiles),
      validation       = None
    )
}
