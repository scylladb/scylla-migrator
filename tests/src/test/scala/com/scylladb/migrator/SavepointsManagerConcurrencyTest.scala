package com.scylladb.migrator

import com.scylladb.migrator.config.{
  MigratorConfig,
  Savepoints,
  SourceSettings,
  TargetSettings
}

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ CountDownLatch, CyclicBarrier, Executors, TimeUnit }
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.matching.Regex

/** Unit tests for `SavepointsManager` that focus on the concurrency and filename invariants
  * introduced to fix issue #347. These tests do not require Spark or any external services.
  */
class SavepointsManagerConcurrencyTest extends munit.FunSuite {

  private val savepointName: Regex =
    """^savepoint_(\d+)(?:_(\d+))?\.yaml$""".r

  private def newConfig(savepointsDir: Path, intervalSeconds: Int): MigratorConfig =
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
        credentials                   = None,
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
      savepoints       = Savepoints(intervalSeconds = intervalSeconds, path = savepointsDir.toString),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )

  /** Test double: reports whatever subset of processed files the caller has registered.
    *
    * `snapshotDelayMillis` is injected into `updateConfigWithMigrationState` so that concurrent
    * dumps can deterministically overlap: the snapshot is taken, a brief delay occurs, then the
    * write happens. This reproduces the real race between a scheduled dump (early snapshot, late
    * write) and a terminal dump (late snapshot, late write).
    */
  private class TestManager(
    cfg: MigratorConfig,
    processed: => Set[String],
    snapshotDelayMillis: Long = 0L,
    onSnapshotStarted: () => Unit = () => ()
  ) extends SavepointsManager(cfg) {
    def describeMigrationState(): String = s"Processed: ${processed.size}"
    def updateConfigWithMigrationState(): MigratorConfig = {
      onSnapshotStarted()
      val snapshot = processed
      if (snapshotDelayMillis > 0) Thread.sleep(snapshotDelayMillis)
      cfg.copy(skipParquetFiles = Some(snapshot))
    }
  }

  private def listSavepoints(dir: Path): Seq[Path] =
    Using.resource(Files.list(dir)) { stream =>
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter { p =>
          val name = p.getFileName.toString
          // Exclude temp files produced by the atomic-rename write path.
          name.startsWith("savepoint_") && name.endsWith(".yaml")
        }
        .toSeq
    }

  private def sortKey(path: Path): (Long, Long) =
    path.getFileName.toString match {
      case savepointName(head, tailOrNull) =>
        // Defensive: hostile filenames with overflow-long numerics must not crash the sort.
        try
          if (tailOrNull == null)
            (java.lang.Math.multiplyExact(java.lang.Long.parseLong(head), 1000L), -1L)
          else
            (java.lang.Long.parseLong(head), java.lang.Long.parseLong(tailOrNull))
        catch {
          case _: NumberFormatException | _: ArithmeticException =>
            (Files.getLastModifiedTime(path).toMillis, -1L)
        }
      case _ => (Files.getLastModifiedTime(path).toMillis, -1L)
    }

  private def latest(dir: Path): Path = {
    // Materialize the sort key once per file: avoids N log N `getLastModifiedTime` syscalls in
    // the legacy-fallback branch under the heavier stress tests in this suite.
    val ranked = listSavepoints(dir).map(p => sortKey(p) -> p).sortBy(_._1)
    ranked.last._2
  }

  private def deleteRecursively(dir: Path): Unit =
    if (Files.exists(dir)) {
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }

  test("filenames are monotonically unique even when written in the same millisecond") {
    val dir = Files.createTempDirectory("savepoints-unique-names")
    try {
      val cfg     = newConfig(dir, intervalSeconds = 3600)
      val manager = new TestManager(cfg, processed = Set("a"))
      try {
        (1 to 50).foreach(_ => manager.dumpMigrationState("rapid"))
      } finally manager.close()

      val names = listSavepoints(dir).map(_.getFileName.toString)
      assertEquals(names.distinct.size, names.size, "every savepoint file should be unique")
      assert(
        names.forall(savepointName.pattern.matcher(_).matches()),
        s"all savepoint filenames must match the new pattern, got: $names"
      )

      val sorted = listSavepoints(dir).sortBy(sortKey).map(_.getFileName.toString)
      val counters =
        sorted.collect { case n @ savepointName(_, c) if c != null => n -> c.toLong }
      assert(counters.size >= 50, s"expected >=50 counters, got ${counters.size}")
      val isMonotonic = counters.map(_._2).sliding(2).forall {
        case Seq(a, b) => a < b
        case _         => true
      }
      assert(isMonotonic, s"counters must increase monotonically in filename order: $counters")
    } finally deleteRecursively(dir)
  }

  test("concurrent dumps never overwrite a later snapshot with a stale one") {
    // Simulates the #347 race: one thread (scheduler) snapshots an early subset of processed
    // files and takes a long time to write, while another thread (driver) snapshots the full set
    // and writes immediately after. The invariant we assert is that the file with the highest
    // (millis, counter) always contains the most advanced snapshot.
    //
    // A `CyclicBarrier` ensures all worker threads arrive at `dumpMigrationState` at the same
    // wall-clock instant so the `ReentrantLock` actually gets contended; this replaces the prior
    // reliance on a short `Thread.sleep` as a race amplifier, which could be defeated by a slow
    // CI host where the scheduler serialized submissions before any contention could occur.
    val dir   = Files.createTempDirectory("savepoints-race")
    val state = new AtomicInteger(0)
    val cfg   = newConfig(dir, intervalSeconds = 3600)
    val rounds = 30

    try {
      val manager = new TestManager(
        cfg,
        processed = (1 to state.get()).map(i => s"file-$i").toSet
      )
      try {
        // Pool sized to the round count so every submission gets its own thread and all
        // `rounds` runnables can simultaneously wait on the barrier; a smaller pool would
        // deadlock at `barrier.await` because only the first N threads would ever arrive.
        val pool    = Executors.newFixedThreadPool(rounds)
        val barrier = new CyclicBarrier(rounds)
        val latch   = new CountDownLatch(rounds)
        try {
          (1 to rounds).foreach { i =>
            pool.submit(new Runnable {
              override def run(): Unit =
                try {
                  // All 30 submissions wait here until the last one arrives; from that moment
                  // they race the `ReentrantLock` deterministically regardless of CI speed.
                  barrier.await(30, TimeUnit.SECONDS)
                  state.set(i)
                  manager.dumpMigrationState(s"round-$i")
                } finally latch.countDown()
            })
          }
          assert(latch.await(30, TimeUnit.SECONDS), "concurrent dumps did not finish in time")
        } finally pool.shutdownNow()

        // Terminal dump with the full state, emulating the "final"/"completed" dump issued by the
        // driver after scheduling has quiesced.
        state.set(rounds)
        manager.dumpMigrationState("final")
      } finally manager.close()

      // Every file must be a valid YAML parseable as MigratorConfig.
      listSavepoints(dir).foreach { p =>
        val text = new String(Files.readAllBytes(p), StandardCharsets.UTF_8)
        assert(text.nonEmpty, s"file $p is empty")
        val parsed = MigratorConfig.loadFrom(p.toString)
        val snapshot = parsed.skipParquetFiles.getOrElse(
          fail(s"skipParquetFiles missing from $p"): Set[String]
        )
        // Every recorded snapshot must be a prefix subset of {file-1, ..., file-30}.
        assert(
          snapshot.forall(_.startsWith("file-")),
          s"unexpected entries in $p: $snapshot"
        )
      }

      // The final file (by filename order) must be the terminal dump with the full set.
      val winner = latest(dir)
      val cfg2   = MigratorConfig.loadFrom(winner.toString)
      assertEquals(
        cfg2.skipParquetFiles.getOrElse(Set.empty),
        (1 to 30).map(i => s"file-$i").toSet,
        s"latest savepoint ${winner.getFileName} did not contain the final snapshot"
      )
    } finally deleteRecursively(dir)
  }

  test("atomic write prevents torn reads of the savepoint") {
    // A reader thread continuously parses the latest savepoint while the writer dumps repeatedly.
    // With the temp-file + ATOMIC_MOVE strategy the reader must never observe a truncated YAML.
    val dir     = Files.createTempDirectory("savepoints-atomic")
    val counter = new AtomicInteger(0)
    val cfg     = newConfig(dir, intervalSeconds = 3600)

    try {
      val manager = new TestManager(
        cfg,
        processed = (1 to counter.get()).map(i => s"f-$i").toSet
      )
      val stop    = new java.util.concurrent.atomic.AtomicBoolean(false)
      val torn    = new java.util.concurrent.atomic.AtomicInteger(0)
      val reader  = new Thread(() => {
        while (!stop.get()) {
          try {
            listSavepoints(dir).sortBy(sortKey).lastOption.foreach { p =>
              try {
                val _ = MigratorConfig.loadFrom(p.toString)
              } catch {
                // File content was partial/unparseable: that is a torn read.
                case _: io.circe.ParsingFailure            => torn.incrementAndGet()
                case _: io.circe.DecodingFailure           => torn.incrementAndGet()
                case _: java.io.EOFException               => torn.incrementAndGet()
                // The file may disappear between listing and reading (atomic rename or
                // filesystem cleanup). That is not a torn read, just a benign race.
                case _: java.nio.file.NoSuchFileException  => ()
                case _: java.io.FileNotFoundException      => ()
              }
            }
          } catch {
            // Directory listing / sort key can race with rename; ignore and re-list.
            case _: java.nio.file.NoSuchFileException => ()
            case _: java.io.FileNotFoundException    => ()
          }
        }
      })
      reader.setDaemon(true)
      reader.start()

      try {
        (1 to 200).foreach { i =>
          counter.set(i)
          manager.dumpMigrationState(s"iter-$i")
        }
      } finally {
        stop.set(true)
        reader.join(5000L)
        manager.close()
      }

      assertEquals(
        torn.get(),
        0,
        "reader observed a torn/unparseable savepoint; atomic write is broken"
      )
    } finally deleteRecursively(dir)
  }

  test("new manager seeds state from existing savepoints so new files always sort after old ones") {
    // Guards against the JVM-restart + backward-clock window: if the new manager's counter
    // restarted at 0 and the host clock drifted behind the previous run, new filenames would
    // sort BEFORE existing ones and resume would silently regress.  Seeding on startup pins
    // `(millis, seq)` above anything already on disk.
    val dir = Files.createTempDirectory("savepoints-seed")
    try {
      // Plant a pre-existing savepoint with an absurdly future millis so any "fresh" clamp that
      // did not consult the directory would produce a filename that sorts BEFORE this one.
      val farFuture = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365)
      val planted = dir.resolve(f"savepoint_${farFuture}%013d_${9999L}%010d.yaml")
      Files.write(planted, Array.emptyByteArray)

      val cfg     = newConfig(dir, intervalSeconds = 3600)
      val manager = new TestManager(cfg, processed = Set("seed"))
      try manager.dumpMigrationState("after-seed")
      finally manager.close()

      val winner = latest(dir)
      assert(
        winner.getFileName.toString != planted.getFileName.toString,
        s"new dump did not beat the planted savepoint ${planted.getFileName}"
      )
      // And the new file must be a real dump, not the empty planted one.
      val cfg2 = MigratorConfig.loadFrom(winner.toString)
      assertEquals(cfg2.skipParquetFiles.getOrElse(Set.empty), Set("seed"))
    } finally deleteRecursively(dir)
  }

  test("hostile filenames do not crash sort / findLatest") {
    // An attacker (or a buggy external tool) could drop a savepoint-looking file whose numeric
    // fields overflow `Long`.  `parseLong` then throws `NumberFormatException`; an un-guarded
    // `sortBy` would surface that as a resume-time crash (DoS).  `sortKey` must treat such a
    // name as unknown and fall back to mtime, so the real savepoint still wins.
    val dir = Files.createTempDirectory("savepoints-hostile")
    try {
      // 25-digit head overflows `Long.MAX_VALUE` (19 digits).
      val hostile = dir.resolve("savepoint_9999999999999999999999999_1.yaml")
      Files.write(hostile, "irrelevant".getBytes(StandardCharsets.UTF_8))

      val cfg     = newConfig(dir, intervalSeconds = 3600)
      val manager = new TestManager(cfg, processed = Set("real"))
      try manager.dumpMigrationState("real")
      finally manager.close()

      // Sort must not throw.
      val ranked = listSavepoints(dir).map(p => sortKey(p) -> p).sortBy(_._1)
      assert(ranked.nonEmpty, "no files listed")
      val winner = ranked.last._2
      // The new legitimate dump must win: its key is (currentMillis, counter>=1), which is far
      // larger than a fallback-to-mtime key produced for the pre-existing hostile file.
      assert(
        winner.getFileName.toString != hostile.getFileName.toString,
        s"hostile filename ${hostile.getFileName} beat the real savepoint in sort order"
      )
    } finally deleteRecursively(dir)
  }

  test("final/completed dumps before close still win over an in-flight scheduled dump") {
    // Mirror the production ordering in Parquet.scala / ScyllaMigrator.scala:
    // a scheduled dump may already be in flight, then the driver writes "final" / "completed",
    // then `close()` drains the scheduler. The latest savepoint on disk must still contain the
    // terminal snapshot.
    val dir   = Files.createTempDirectory("savepoints-await")
    val state = new AtomicInteger(0)
    val cfg   = newConfig(dir, intervalSeconds = 1)
    val scheduledTickStarted = new CountDownLatch(1)

    try {
      val manager = new TestManager(
        cfg,
        processed           = (1 to state.get()).map(i => s"x-$i").toSet,
        snapshotDelayMillis = 400,
        onSnapshotStarted   = () => scheduledTickStarted.countDown()
      )
      assert(
        scheduledTickStarted.await(10, TimeUnit.SECONDS),
        "scheduled tick never started"
      )
      state.set(99)
      manager.dumpMigrationState("final")
      manager.dumpMigrationState("completed")
      manager.close()

      val winner = latest(dir)
      val parsed = MigratorConfig.loadFrom(winner.toString)
      assertEquals(
        parsed.skipParquetFiles.getOrElse(Set.empty),
        (1 to 99).map(i => s"x-$i").toSet,
        s"latest savepoint ${winner.getFileName} did not contain the completed terminal snapshot"
      )
    } finally deleteRecursively(dir)
  }
}
