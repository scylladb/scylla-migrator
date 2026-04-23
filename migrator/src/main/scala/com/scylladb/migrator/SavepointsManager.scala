package com.scylladb.migrator

import com.scylladb.migrator.config.MigratorConfig
import org.apache.logging.log4j.LogManager
import sun.misc.{ Signal, SignalHandler }

import java.nio.charset.StandardCharsets
import java.nio.file.{
  AccessDeniedException,
  AtomicMoveNotSupportedException,
  Files,
  Path,
  Paths,
  StandardCopyOption
}
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

/** A component that manages savepoints. Savepoints provide a way to resume an interrupted
  * migration.
  *
  * This component periodically stores savepoints according to the schedule defined in the
  * configuration. It also automatically stores a savepoint in case of early termination (e.g. due
  * to a SIGTERM signal).
  *
  * Internally, it works by writing modified copies of the original migration configuration. These
  * copies specify which parts of the source dataset have already been migrated and can safely be
  * skipped when restarting the migration.
  *
  * Make sure to call the method `close` when you don’t need the savepoints manager anymore so that
  * it releases the resources it was using.
  *
  * This class is abstract. Subclasses are responsible for implementing how to track the migration
  * progress, and for communicating the updated state of the migration via the method
  * `updateConfigWithMigrationState`.
  *
  * Concurrency and ordering guarantees:
  *   - `dumpMigrationState` is serialized so that the accumulator snapshot and the on-disk write
  *     happen atomically with respect to other dumps. This prevents a scheduled dump from
  *     overwriting a later terminal dump with a stale snapshot.
  *   - Savepoint filenames embed a zero-padded millisecond timestamp and a zero-padded per-instance
  *     monotonic counter (`savepoint_<epochMillis>_<counter>.yaml`). The timestamp is clamped to a
  *     monotonic non-decreasing value within the manager instance, so a backward wall-clock step
  *     cannot make a later savepoint sort earlier than an older one. Zero-padding keeps
  *     lexicographical order consistent with chronological order, so `ls | tail -n 1` also returns
  *     the newest savepoint.
  *   - `close()` awaits the scheduler so no scheduled tick races the final dump issued by the
  *     caller after `close()`. The wait is bounded by `MaxCloseAwaitMillis` so a stuck filesystem
  *     cannot hang shutdown indefinitely.
  *   - The first SIGINT/SIGTERM/USR2 still writes a savepoint before exit. A second signal acts as
  *     a force-quit escape hatch, so operators can terminate even if the first dump is stuck on
  *     slow or unhealthy storage.
  */
abstract class SavepointsManager(migratorConfig: MigratorConfig) extends AutoCloseable {

  import SavepointsManager._

  val log = LogManager.getLogger(this.getClass.getName)
  private val scheduler = new ScheduledThreadPoolExecutor(1)
  private var oldUsr2Handler: SignalHandler = _
  private var oldTermHandler: SignalHandler = _
  private var oldIntHandler: SignalHandler = _

  // Serializes `dumpMigrationState` across the scheduler, the driver thread, and signal handlers.
  // `ReentrantLock` (rather than a bare `synchronized`) lets the signal handler use `tryLock` with
  // a bounded timeout, which avoids turning a stuck write into an unkillable process.
  private val dumpLock = new ReentrantLock()
  // Per-instance monotonic counter: disambiguates dumps that share the same millisecond timestamp
  // and makes the filename order consistent with the logical order of dumps.
  private val savepointSequence = new AtomicLong(0L)
  // First signal writes a savepoint before exit; the second skips dumping and exits immediately.
  private val signalDumpInProgress = new AtomicBoolean(false)
  // Guarded by `dumpLock`: prevents a backward wall-clock step from making a later dump sort
  // earlier than an older one.
  private var lastSavepointMillis = 0L

  createSavepointsDirectory()
  seedStateFromExistingSavepoints()
  addUSR2Handler()
  startSavepointSchedule()

  private def createSavepointsDirectory(): Unit = {
    val savepointsDirectory = Paths.get(migratorConfig.savepoints.path)
    if (!Files.exists(savepointsDirectory)) {
      log.debug(
        s"Directory ${savepointsDirectory.normalize().toString} does not exist. Creating it..."
      )
      Files.createDirectories(savepointsDirectory)
    }
  }

  /** Scan the savepoints directory once at startup and seed `lastSavepointMillis` /
    * `savepointSequence` so that filenames written by this instance are strictly greater than any
    * filename already on disk.
    *
    * Without this seed, a JVM restart on a host whose wall clock has drifted backwards (NTP
    * step-back, VM migration, docker host time skew) would make new savepoints sort *earlier* than
    * savepoints written by the previous run still present in the same directory. Resume would then
    * silently pick a stale savepoint and lose progress. Seeding closes that window by ensuring the
    * `(millis, seq)` key monotonically increases across process boundaries as long as the directory
    * is not wiped.
    */
  private def seedStateFromExistingSavepoints(): Unit = {
    val savepointsDirectory = Paths.get(migratorConfig.savepoints.path)
    if (!Files.exists(savepointsDirectory)) return

    var maxMillis = 0L
    var maxSeq = 0L
    try
      Using.resource(Files.list(savepointsDirectory)) { stream =>
        stream.iterator().asScala.foreach { path =>
          val name = path.getFileName.toString
          name match {
            case SavepointsManager.SavepointName(head, tailOrNull) =>
              val millis =
                try
                  if (tailOrNull == null) java.lang.Long.parseLong(head) * 1000L
                  else java.lang.Long.parseLong(head)
                catch { case _: NumberFormatException => 0L }
              val seq =
                if (tailOrNull == null) 0L
                else
                  try java.lang.Long.parseLong(tailOrNull)
                  catch { case _: NumberFormatException => 0L }
              if (millis > maxMillis || (millis == maxMillis && seq > maxSeq)) {
                maxMillis = millis
                maxSeq    = seq
              }
            case _ => ()
          }
        }
      }
    catch {
      case NonFatal(e) =>
        log.warn(
          s"Could not scan ${savepointsDirectory} to seed savepoint state; " +
            s"falling back to clock-only ordering: ${e.getMessage}"
        )
        return
    }

    if (maxMillis > 0L) {
      lastSavepointMillis = maxMillis
      savepointSequence.set(maxSeq)
      log.info(
        s"Seeded savepoint state from existing files: " +
          s"lastSavepointMillis=${maxMillis}, nextSequence=${maxSeq + 1}"
      )
    }
  }

  private def savepointFilename(path: String): String = {
    val millis = math.max(lastSavepointMillis, System.currentTimeMillis())
    lastSavepointMillis = millis
    val seq = savepointSequence.incrementAndGet()
    // Zero-padded so that lexicographical order matches chronological order (handy for `ls`).
    // `Locale.ROOT` pins the numeric formatter to ASCII digits; the JVM default locale on some
    // hosts (e.g. `ar-SA`, `th-TH`) would otherwise emit non-ASCII numerals that break the
    // filename regex and chronological sort used for resume.
    String.format(
      Locale.ROOT,
      "%s/savepoint_%013d_%010d.yaml",
      path,
      java.lang.Long.valueOf(millis),
      java.lang.Long.valueOf(seq)
    )
  }

  private def addUSR2Handler(): Unit = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint."
    )

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit = {
        val reason = signal.toString
        if (!signalDumpInProgress.compareAndSet(false, true)) {
          log.warn(
            s"Received ${reason} while another signal-triggered savepoint dump is already in " +
              s"progress; forcing exit immediately."
          )
          sys.exit(0)
        }

        // The first signal preserves the historical contract: write a savepoint before exiting.
        // If the dump itself fails (disk full, permission denied, subclass bug), we still need
        // to honour "first signal -> exit": swallow the error, log it, reset the in-progress
        // flag so the JVM is not left in a half-terminated state, and proceed to `sys.exit(0)`
        // from the `finally` block. A second signal while the dump is in flight still takes the
        // fast-path above.
        try dumpMigrationState(reason)
        catch {
          case NonFatal(t) =>
            log.error(
              s"Signal-triggered savepoint dump for ${reason} failed; exiting anyway.",
              t
            )
        } finally {
          signalDumpInProgress.set(false)
          sys.exit(0)
        }
      }
    }

    oldUsr2Handler = Signal.handle(new Signal("USR2"), handler)
    oldTermHandler = Signal.handle(new Signal("TERM"), handler)
    oldIntHandler  = Signal.handle(new Signal("INT"), handler)
  }

  private def startSavepointSchedule(): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpMigrationState("schedule")
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${migratorConfig.savepoints.intervalSeconds} seconds"
    )

    scheduler.scheduleAtFixedRate(
      runnable,
      migratorConfig.savepoints.intervalSeconds,
      migratorConfig.savepoints.intervalSeconds,
      TimeUnit.SECONDS
    )
  }

  /** Dump the current state of the migration into a configuration file that can be used to resume
    * the migration.
    *
    * Snapshotting the current state and writing it to disk are performed under a lock so that
    * concurrent callers (scheduler, driver, signal handler) cannot interleave and overwrite each
    * other with stale snapshots. The file is first written to a temporary path and then atomically
    * renamed, so readers never observe a truncated YAML file. The temp file is deleted if the
    * rename does not succeed, so failures do not leak sibling `*.yaml.tmp` files on disk.
    *
    * @param reason
    *   Human-readable, informal, event that caused the dump.
    */
  final def dumpMigrationState(reason: String): Unit = {
    dumpLock.lock()
    try doDump(reason)
    finally dumpLock.unlock()
  }

  private def doDump(reason: String): Unit = {
    val finalPath =
      Paths.get(savepointFilename(migratorConfig.savepoints.path)).normalize
    val tempPath = Paths.get(finalPath.toString + ".tmp").normalize

    val modifiedConfig = updateConfigWithMigrationState()
    val payload = modifiedConfig.render.getBytes(StandardCharsets.UTF_8)

    var moved = false
    try {
      Files.write(tempPath, payload)
      atomicReplace(tempPath, finalPath)
      moved = true
    } finally
      if (!moved) {
        // Best-effort cleanup so a failed rename does not leak `.yaml.tmp` siblings that would
        // otherwise accumulate on a flaky filesystem.
        try Files.deleteIfExists(tempPath)
        catch {
          case cleanupErr: Throwable =>
            log.warn(s"Failed to clean up temp savepoint ${tempPath}: ${cleanupErr.getMessage}")
        }
      }

    log.info(
      s"Created a savepoint config at ${finalPath} due to ${reason}. ${describeMigrationState()}"
    )
  }

  private def atomicReplace(source: Path, target: Path): Unit =
    try
      Files.move(
        source,
        target,
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING
      )
    catch {
      case _: AtomicMoveNotSupportedException =>
        // Fallback for filesystems that do not support atomic rename (e.g. certain object-store
        // mounts). Semantics degrade to "replace" but all other guarantees are preserved.
        log.warn(
          s"Atomic rename not supported on the filesystem backing ${target}; " +
            s"falling back to non-atomic replace."
        )
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
      case e: AccessDeniedException =>
        // On Windows, ATOMIC_MOVE can throw AccessDeniedException if a reader holds the target
        // file open. Fall back to a non-atomic replace rather than failing the dump.
        log.warn(
          s"Atomic rename denied on ${target} (likely a concurrent reader on Windows); " +
            s"falling back to non-atomic replace: ${e.getMessage}"
        )
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
    }

  /** Stop the periodic creation of savepoints and release the associated resources.
    *
    * Blocks briefly for in-flight scheduled ticks to finish so that no scheduled dump can race with
    * a terminal dump issued by the caller after `close()`. The wait is bounded on both ends
    * (`MinCloseAwaitMillis` / `MaxCloseAwaitMillis`) so `close()` never hangs indefinitely on a
    * stuck filesystem nor returns before a reasonable in-flight dump can finish; if the scheduler
    * fails to terminate within the deadline the method logs a warning and forces shutdown.
    */
  def close(): Unit = {
    scheduler.shutdown()
    val awaitMillis =
      math.min(
        MaxCloseAwaitMillis,
        math.max(MinCloseAwaitMillis, 2L * migratorConfig.savepoints.intervalSeconds * 1000L)
      )
    val terminated =
      try scheduler.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          false
      }
    if (!terminated) {
      log.warn(
        s"Savepoint scheduler did not terminate within ${awaitMillis} ms; forcing shutdown."
      )
      scheduler.shutdownNow()
    }
    Signal.handle(new Signal("USR2"), oldUsr2Handler)
    Signal.handle(new Signal("TERM"), oldTermHandler)
    Signal.handle(new Signal("INT"), oldIntHandler)
  }

  /** Provide readable logs by describing which parts of the migration have been completed already.
    */
  def describeMigrationState(): String

  /** A copy of the original migration configuration, updated to describe which parts of the
    * migration have been completed already.
    */
  def updateConfigWithMigrationState(): MigratorConfig

}

object SavepointsManager {
  // Floor for the `awaitTermination` deadline in `close()`. Gives the in-flight scheduled dump
  // time to finish even when `intervalSeconds` is tiny (e.g. 1s in integration tests).
  private val MinCloseAwaitMillis: Long = 5_000L

  // Ceiling for the same deadline. Prevents `close()` from blocking for multiple minutes when
  // `intervalSeconds` is large (e.g. 3600) and the filesystem is stuck.
  private val MaxCloseAwaitMillis: Long = 30_000L

  // Filename grammar for savepoints. Two groups:
  //   - new format: `savepoint_<epochMillis>_<counter>.yaml` (tail is the counter)
  //   - legacy:     `savepoint_<epochSeconds>.yaml`          (tail is null)
  // This regex is the single source of truth for parsing filenames produced by this manager.
  // Test helpers and the startup seed both reuse it to stay in sync with what `savepointFilename`
  // writes.
  private[migrator] val SavepointName: scala.util.matching.Regex =
    """^savepoint_(\d+)(?:_(\d+))?\.yaml$""".r
}
