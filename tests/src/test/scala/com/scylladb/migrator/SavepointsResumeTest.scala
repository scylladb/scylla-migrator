package com.scylladb.migrator

import com.scylladb.migrator.config.{
  Credentials,
  MigratorConfig,
  Savepoints,
  SourceSettings,
  TargetSettings
}

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Unit tests for the startup auto-resume resolver ([[SavepointsResume]]) and for the secret
  * redaction applied when savepoints are written. None of these require Spark or external services.
  */
class SavepointsResumeTest extends munit.FunSuite {

  private def baseConfig(
    savepointsPath: String,
    targetCredentials: Option[Credentials] = None
  ): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = "dummy",
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "localhost",
        port                          = 9042,
        localDC                       = None,
        credentials                   = targetCredentials,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "t",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 3600, path = savepointsPath),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )

  private def savepointFileName(millis: Long, seq: Long): String =
    f"savepoint_${millis}%013d_${seq}%010d.yaml"

  /** Render a savepoint with the given processed files under the standard filename. */
  private def writeSavepoint(dir: Path, millis: Long, seq: Long, skip: Set[String]): Unit = {
    val cfg = baseConfig(dir.toString).copy(skipParquetFiles = Some(skip))
    Files.write(
      dir.resolve(savepointFileName(millis, seq)),
      cfg.renderRedacted.getBytes(StandardCharsets.UTF_8)
    )
  }

  private def deleteRecursively(dir: Path): Unit =
    if (Files.exists(dir))
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)

  private def withTempDir(name: String)(f: Path => Unit): Unit = {
    val dir = Files.createTempDirectory(name)
    try f(dir)
    finally deleteRecursively(dir)
  }

  test("resume merges skip-state from the latest savepoint into a fresh config") {
    withTempDir("resume-merge") { dir =>
      writeSavepoint(dir, millis = 1000L, seq = 1L, skip = Set("a.parquet"))
      writeSavepoint(dir, millis = 1000L, seq = 2L, skip = Set("a.parquet", "b.parquet"))

      val merged = SavepointsResume.resume(baseConfig(dir.toString), None, None)

      assertEquals(merged.skipParquetFiles, Some(Set("a.parquet", "b.parquet")))
    }
  }

  test("resume picks the newest savepoint by (millis, seq), not an older one") {
    withTempDir("resume-newest") { dir =>
      // Older file lists a file that must NOT leak into the result if the newest is chosen.
      writeSavepoint(dir, millis = 5000L, seq = 1L, skip = Set("stale.parquet"))
      writeSavepoint(
        dir,
        millis = 5000L,
        seq    = 2L,
        skip   = Set("fresh-1.parquet", "fresh-2.parquet")
      )

      val merged = SavepointsResume.resume(baseConfig(dir.toString), None, None)

      assertEquals(merged.skipParquetFiles, Some(Set("fresh-1.parquet", "fresh-2.parquet")))
    }
  }

  test("resume skips a corrupt newest savepoint and falls back to an older valid one") {
    withTempDir("resume-corrupt") { dir =>
      writeSavepoint(dir, millis = 7000L, seq = 1L, skip = Set("older-good.parquet"))
      // Newest filename by (millis, seq), but contents do not decode to a MigratorConfig.
      Files.write(
        dir.resolve(savepointFileName(7000L, 2L)),
        "definitely not a valid migrator config".getBytes(StandardCharsets.UTF_8)
      )

      val merged = SavepointsResume.resume(baseConfig(dir.toString), None, None)

      assertEquals(merged.skipParquetFiles, Some(Set("older-good.parquet")))
    }
  }

  test("resume unions the savepoint skip-state with any skip-state already in the config") {
    withTempDir("resume-union") { dir =>
      writeSavepoint(dir, millis = 2000L, seq = 1L, skip = Set("from-savepoint.parquet"))

      val configWithExisting =
        baseConfig(dir.toString).copy(skipParquetFiles = Some(Set("from-config.parquet")))
      val merged = SavepointsResume.resume(configWithExisting, None, None)

      assertEquals(
        merged.skipParquetFiles,
        Some(Set("from-config.parquet", "from-savepoint.parquet"))
      )
    }
  }

  test("resume preserves the live source and target (including credentials)") {
    withTempDir("resume-preserve-creds") { dir =>
      writeSavepoint(dir, millis = 3000L, seq = 1L, skip = Set("done.parquet"))

      val liveConfig =
        baseConfig(dir.toString, targetCredentials = Some(Credentials("migrator", "live-secret")))
      val merged = SavepointsResume.resume(liveConfig, None, None)

      assertEquals(merged.target, liveConfig.target)
      assertEquals(merged.source, liveConfig.source)
      assertEquals(merged.skipParquetFiles, Some(Set("done.parquet")))
    }
  }

  test("resume is a no-op when autoResume is disabled") {
    withTempDir("resume-disabled") { dir =>
      writeSavepoint(dir, millis = 4000L, seq = 1L, skip = Set("ignored.parquet"))

      val base = baseConfig(dir.toString)
      val config = base.copy(savepoints = base.savepoints.copy(autoResume = false))
      val merged = SavepointsResume.resume(config, None, None)

      assertEquals(merged.skipParquetFiles, None)
    }
  }

  test("resume is a no-op when the savepoints directory has no savepoints") {
    withTempDir("resume-empty") { dir =>
      val merged = SavepointsResume.resume(baseConfig(dir.toString), None, None)
      assertEquals(merged.skipParquetFiles, None)
    }
  }

  test("resume is a no-op when the savepoints directory does not exist") {
    withTempDir("resume-missing") { dir =>
      val missing = dir.resolve("does-not-exist")
      val merged = SavepointsResume.resume(baseConfig(missing.toString), None, None)
      assertEquals(merged.skipParquetFiles, None)
    }
  }

  private class WritingManager(cfg: MigratorConfig, processed: Set[String])
      extends SavepointsManager(cfg) {
    override def describeMigrationState(): String = s"Processed: ${processed.size}"
    override def updateConfigWithMigrationState(): MigratorConfig =
      cfg.copy(skipParquetFiles = Some(processed))
  }

  test("written savepoints redact credentials but preserve skip-state") {
    withTempDir("savepoint-redaction") { dir =>
      val secret = "TOP-SECRET-PASSWORD-123"
      val cfg =
        baseConfig(dir.toString, targetCredentials = Some(Credentials("migrator", secret)))
      val manager = new WritingManager(cfg, Set("file-a.parquet"))
      try manager.dumpMigrationState("test")
      finally manager.close()

      val names =
        Using.resource(Files.list(dir)) { stream =>
          stream.iterator().asScala.map(_.getFileName.toString).toList
        }
      val savepointFile = SavepointsManager
        .latestSavepointName(names)
        .map(dir.resolve)
        .getOrElse(fail("no savepoint file was written"))

      val text = new String(Files.readAllBytes(savepointFile), StandardCharsets.UTF_8)
      assert(!text.contains(secret), s"savepoint leaked the credential in cleartext:\n$text")
      assert(text.contains("<redacted>"), s"savepoint did not redact the credential:\n$text")

      // Skip-state must survive redaction so that auto-resume can recover it.
      val parsed = MigratorConfig.loadFrom(savepointFile.toString)
      assertEquals(parsed.skipParquetFiles, Some(Set("file-a.parquet")))
    }
  }
}
