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

  test("target store newestValidConfig skips rows from different config identities") {
    val requested = config(sourcePath = "s3a://bucket/requested", skipFiles = Set("requested"))
    val unrelated = config(sourcePath = "s3a://bucket/unrelated", skipFiles = Set("unrelated"))
    val requestedSha = SavepointStore.configIdentitySha256(requested)
    val unrelatedSha = SavepointStore.configIdentitySha256(unrelated)

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepoint(
          3000L,
          1L,
          unrelated.render,
          Some(requestedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepoint(
          2000L,
          1L,
          unrelated.render,
          Some(unrelatedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepoint(
          1000L,
          1L,
          requested.render,
          Some(requestedSha)
        )
      ),
      expectedConfigSha256 = requestedSha,
      warnInvalidRow       = (_, _) => ()
    )

    assertEquals(
      selected.map(SavepointStore.configIdentitySha256),
      Some(SavepointStore.configIdentitySha256(requested)),
      "target-backed resume must not use a newer row that belongs to a different migration identity"
    )
  }

  test("target store newestCoordinates skips rows from different config identities") {
    val requested = config(sourcePath = "s3a://bucket/requested", skipFiles = Set("requested"))
    val unrelated = config(sourcePath = "s3a://bucket/unrelated", skipFiles = Set("unrelated"))
    val requestedSha = SavepointStore.configIdentitySha256(requested)
    val unrelatedSha = SavepointStore.configIdentitySha256(unrelated)

    val selected = ScyllaTargetSavepointStore.newestCoordinates(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = 3000L,
          sequence     = 1L,
          configSha256 = Some(unrelatedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = 2000L,
          sequence     = 1L,
          configSha256 = Some(requestedSha)
        )
      ),
      expectedConfigSha256 = Some(requestedSha),
      warnInvalidRow       = (_, _) => ()
    )

    assertEquals(
      selected,
      Some(SavepointCoordinates(2000L, 1L)),
      "target-backed seed state must ignore coordinates from another migration identity"
    )
  }

  test("file store latestConfig ignores hostile future coordinates even with newer mtime") {
    val dir = Files.createTempDirectory("savepoint-store-latest-hostile")
    try {
      val realMillis = System.currentTimeMillis()
      val hostilePath =
        dir.resolve(
          f"savepoint_${SavepointsManager.MaxReasonableSeedValue}%013d_${1L}%010d.yaml"
        )
      val realPath =
        dir.resolve(f"savepoint_${realMillis}%013d_${2L}%010d.yaml")

      Files.write(
        realPath,
        config(skipFiles = Set("real")).render.getBytes(StandardCharsets.UTF_8)
      )
      Files.write(
        hostilePath,
        config(skipFiles = Set("hostile")).render.getBytes(StandardCharsets.UTF_8)
      )
      Files.setLastModifiedTime(hostilePath, FileTime.fromMillis(realMillis + 10_000L))

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

  private def config(
    skipFiles: Set[String],
    sourcePath: String = "s3a://bucket/data"
  ): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = sourcePath,
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
