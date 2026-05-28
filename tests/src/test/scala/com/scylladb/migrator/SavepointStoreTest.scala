package com.scylladb.migrator

import com.scylladb.migrator.config.{ MigratorConfig, Savepoints, SourceSettings, TargetSettings }

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.FileTime

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

  test("target store newestValidConfig ignores hostile future coordinates") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    val real = config(skipFiles = Set("real")).render
    val hostile = config(skipFiles = Set("hostile")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, real),
        ScyllaTargetSavepointStore.StoredSavepoint(
          SavepointsManager.MaxReasonableSeedValue,
          1L,
          hostile
        )
      ),
      warnInvalidRow = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(
      selected.flatMap(_.skipParquetFiles),
      Some(Set("real")),
      "target-backed resume must not let an unreasonable future row dominate ordering"
    )
    assertEquals(warnings.toSeq, Seq((SavepointsManager.MaxReasonableSeedValue, 1L)))
  }

  test("file store latestConfig does not let hostile future coordinates beat a real savepoint") {
    val dir = Files.createTempDirectory("savepoint-store-latest-hostile")
    try {
      val hostilePath =
        dir.resolve(
          f"savepoint_${SavepointsManager.MaxReasonableSeedValue}%013d_${1L}%010d.yaml"
        )
      val realPath =
        dir.resolve(f"savepoint_${System.currentTimeMillis()}%013d_${2L}%010d.yaml")

      Files.write(
        hostilePath,
        config(skipFiles = Set("hostile")).render.getBytes(StandardCharsets.UTF_8)
      )
      Files.setLastModifiedTime(hostilePath, FileTime.fromMillis(1L))
      Files.write(
        realPath,
        config(skipFiles = Set("real")).render.getBytes(StandardCharsets.UTF_8)
      )

      val selected = SavepointStore.file(dir.toString).latestConfig()

      assertEquals(
        selected.skipParquetFiles,
        Some(Set("real")),
        "a savepoint with hostile coordinates must not dominate resumeFromLatest ordering"
      )
    } finally
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
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
