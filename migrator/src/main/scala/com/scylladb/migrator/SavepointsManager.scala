package com.scylladb.migrator

import com.scylladb.migrator.config.MigratorConfig
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager
import sun.misc.{ Signal, SignalHandler }

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
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
  *   - `dumpMigrationState` is serialized so that the accumulator snapshot and the store write
  *     happen atomically with respect to other dumps. This prevents a scheduled dump from writing
  *     after a later terminal dump with a stale snapshot.
  *   - Savepoint versions embed a millisecond timestamp and a per-instance monotonic counter. The
  *     timestamp is clamped to a monotonic non-decreasing value within the manager instance, so a
  *     backward wall-clock step cannot make a later savepoint sort earlier than an older one.
  *   - `close()` awaits the scheduler so no scheduled tick races the final dump issued by the
  *     caller after `close()`. The wait is bounded by `MaxCloseAwaitMillis` so a stuck filesystem
  *     cannot hang shutdown indefinitely.
  *   - The first SIGINT/SIGTERM/USR2 attempts to write a savepoint before exit, but the wait for
  *     `dumpLock` is bounded by `SignalDumpLockTimeoutMillis`. If a previous dump is stuck past
  *     that deadline, the signal still triggers `sys.exit(0)` without writing — preserving "first
  *     signal exits" over "first signal writes". A second signal acts as a force-quit escape hatch,
  *     so operators can terminate even if the first dump is stuck on slow or unhealthy storage.
  */
abstract class SavepointsManager(
  migratorConfig: MigratorConfig,
  maybeSavepointStore: Option[SavepointStore] = None
) extends AutoCloseable {

  def this(
    migratorConfig: MigratorConfig,
    hadoopConfiguration: Option[Configuration],
    redactionRegex: Option[String]
  ) =
    this(
      migratorConfig,
      Some(
        SavepointStore.forConfig(
          migratorConfig,
          sparkContext        = None,
          redactionRegex      = redactionRegex,
          hadoopConfiguration = hadoopConfiguration
        )
      )
    )

  import SavepointsManager._

  val log = LogManager.getLogger(this.getClass.getName)
  private val savepointStore = maybeSavepointStore.getOrElse(SavepointStore.file(migratorConfig))
  private val scheduler = new ScheduledThreadPoolExecutor(1)
  private var oldUsr2Handler: SignalHandler = _
  private var oldTermHandler: SignalHandler = _
  private var oldIntHandler: SignalHandler = _

  // Serializes `dumpMigrationState` across the scheduler, the driver thread, and signal handlers.
  // `ReentrantLock` (rather than a bare `synchronized`) is used so that signal-triggered dumps
  // can call `tryLock(SignalDumpLockTimeoutMillis, …)` via `tryDumpMigrationState` and avoid
  // waiting forever on a wedged scheduled dump: a stuck write can otherwise turn the first
  // SIGTERM/SIGINT into an unkillable process when the orchestrator only emits one graceful
  // signal before SIGKILL. The driver and scheduler paths still acquire the lock unconditionally
  // because they have no deadline pressure.
  private val dumpLock = new ReentrantLock()
  // Per-instance monotonic counter: disambiguates dumps that share the same millisecond timestamp
  // and makes the filename order consistent with the logical order of dumps.
  private val savepointSequence = new AtomicLong(0L)
  // First signal writes a savepoint before exit; the second skips dumping and exits immediately.
  private val signalDumpInProgress = new AtomicBoolean(false)
  // Guarded by `dumpLock`: prevents a backward wall-clock step from making a later dump sort
  // earlier than an older one.
  private var lastSavepointMillis = 0L

  savepointStore.prepare()
  seedStateFromExistingSavepoints()
  addUSR2Handler()
  startSavepointSchedule()

  /** Scan the savepoint store once at startup and seed `lastSavepointMillis` / `savepointSequence`
    * so that versions written by this instance are strictly greater than any savepoint version
    * already in the configured store.
    *
    * Without this seed, a JVM restart on a host whose wall clock has drifted backwards (NTP
    * step-back, VM migration, docker host time skew) could make new savepoints sort *earlier* than
    * savepoints written by the previous run still present in the same store. Resume would then
    * silently pick a stale savepoint and lose progress. Seeding closes that window by ensuring the
    * `(millis, seq)` key monotonically increases across process boundaries as long as previous
    * savepoints are still present.
    */
  private def seedStateFromExistingSavepoints(): Unit =
    savepointStore.seedState().foreach {
      case coordinates if isReasonableSeed(coordinates) =>
        lastSavepointMillis = coordinates.epochMillis
        savepointSequence.set(coordinates.sequence)
        log.info(
          s"Seeded savepoint state from existing savepoints: " +
            s"lastSavepointMillis=${coordinates.epochMillis}, nextSequence=${coordinates.sequence + 1}"
        )
      case coordinates =>
        log.warn(
          s"Ignoring hostile/corrupted savepoint seed from ${savepointStore.getClass.getSimpleName}: " +
            s"epoch_millis=${coordinates.epochMillis}, sequence=${coordinates.sequence} outside " +
            s"the accepted range (0 < epoch_millis < ${MaxReasonableSeedValue}, " +
            s"0 <= sequence <= ${MaxSavepointSequenceValue}, and max-sequence seeds must " +
            s"leave room for millisecond rollover)."
        )
    }

  // A max-sequence seed makes the next dump advance epoch_millis by one. That rollover must
  // still stay below MaxReasonableSeedValue, because resume lookup rejects the boundary value.
  private def isReasonableSeed(coordinates: SavepointCoordinates): Boolean =
    isResumeSafeCoordinate(coordinates.epochMillis, coordinates.sequence)

  private def nextCoordinates(): SavepointCoordinates = {
    val millis = math.max(lastSavepointMillis, System.currentTimeMillis())
    val seq = savepointSequence.incrementAndGet()
    if (seq <= MaxSavepointSequenceValue) {
      lastSavepointMillis = millis
      SavepointCoordinates(millis, seq)
    } else {
      val nextMillis = java.lang.Math.addExact(millis, 1L)
      lastSavepointMillis = nextMillis
      savepointSequence.set(1L)
      SavepointCoordinates(nextMillis, 1L)
    }
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
        // Crucially, the lock acquisition is bounded by `SignalDumpLockTimeoutMillis` so a
        // wedged scheduled dump cannot indefinitely stall a graceful shutdown — orchestrators
        // like k8s typically deliver only one SIGTERM before promoting to SIGKILL, so blocking
        // here would defeat the shutdown deadline. If the lock cannot be acquired in time, or
        // the dump itself fails (disk full, permission denied, subclass bug), we still honour
        // "first signal -> exit": log the cause, reset the in-progress flag, and `sys.exit(0)`
        // from the `finally` block. A second signal during an in-flight dump still takes the
        // fast-path above.
        try {
          val wrote = tryDumpMigrationState(reason, SignalDumpLockTimeoutMillis)
          if (!wrote) {
            log.warn(
              s"Did not write a savepoint for ${reason}: dumpLock was contended or the wait " +
                s"was interrupted within ${SignalDumpLockTimeoutMillis} ms. See prior warnings " +
                s"for the specific cause. Exiting anyway to preserve the first-signal-exits contract."
            )
          }
        } catch {
          case NonFatal(t) =>
            log.error(
              s"Signal-triggered savepoint dump for ${reason} failed; exiting anyway.",
              t
            )
        } finally
          // The JVM is already on its way out via `sys.exit(0)`. Resetting the in-progress
          // flag here serves no purpose (no future signal can observe the reset before the
          // JVM halts) and opens a tiny re-entry window where a third signal could pass the
          // CAS gate and start a redundant dump. Leave the flag set and exit.
          sys.exit(0)
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

  /** Dump the current state of the migration into a configuration payload that can be used to
    * resume the migration.
    *
    * Snapshotting the current state and writing it to the configured store are performed under a
    * lock so that concurrent callers (scheduler, driver, signal handler) cannot interleave with
    * stale snapshots.
    *
    * @param reason
    *   Human-readable, informal, event that caused the dump.
    */
  final def dumpMigrationState(reason: String): Unit = {
    dumpLock.lock()
    try doDump(reason)
    finally dumpLock.unlock()
  }

  /** Bounded-wait variant of `dumpMigrationState` for the signal-handler fail-safe path.
    *
    * Returns `true` if `dumpLock` was acquired within `timeoutMillis` and the dump was attempted
    * (any exception from `doDump` propagates so the caller can log it). Returns `false` if the lock
    * could not be acquired in time, so the caller can give up the dump and continue exiting.
    *
    * Visible to `private[migrator]` so unit tests can exercise the timeout branch without going
    * through `sys.exit`.
    */
  private[migrator] def tryDumpMigrationState(reason: String, timeoutMillis: Long): Boolean = {
    val acquired =
      try dumpLock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException =>
          // Distinguish interrupt from timeout: the handler's generic log would otherwise
          // misattribute the cause to "lock contention", masking the real reason (shutdown
          // hook or framework-initiated interrupt) in incident review.
          log.warn(
            s"tryDumpMigrationState(${reason}) was interrupted while waiting for dumpLock; " +
              s"not writing a savepoint."
          )
          Thread.currentThread().interrupt()
          false
      }
    if (!acquired) false
    else
      try {
        doDump(reason)
        true
      } finally dumpLock.unlock()
  }

  private def doDump(reason: String): Unit = {
    val coordinates = nextCoordinates()
    val modifiedConfig = updateConfigWithMigrationState()
    // Savepoint payloads should be directly runnable; `resumeFromLatest` is only a
    // launch-time lookup flag.
    val savepointConfig = modifiedConfig.copy(
      savepoints = modifiedConfig.savepoints.copy(resumeFromLatest = false)
    )
    val location = savepointStore.save(
      SavepointRecord(
        coordinates = coordinates,
        reason      = reason,
        yaml        = savepointConfig.render
      )
    )

    log.info(
      s"Created a savepoint config at ${location} due to ${reason}. ${describeMigrationState()}"
    )
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

  // How long a signal-triggered dump waits for `dumpLock` before giving up and exiting without
  // writing a savepoint. Chosen well below the typical orchestrator grace period (k8s default is
  // 30 s before SIGKILL) so the JVM still has headroom to flush logs and run shutdown hooks
  // after the bounded wait, even if a scheduled dump is wedged on slow/unhealthy storage.
  private[migrator] val SignalDumpLockTimeoutMillis: Long = 5_000L

  // Sanity ceiling for epoch-millis values read from savepoint stores during seed. Values at or
  // above this threshold are treated as hostile / corrupted. Chosen as half of `Long.MAX_VALUE`
  // (~year 146,135,510 AD for millis) so legitimate values have effectively infinite headroom.
  private[migrator] val MaxReasonableSeedValue: Long = java.lang.Long.MAX_VALUE / 2L

  // Highest counter value that still fits the documented ten-digit savepoint filename component.
  // If a restored seed is already at this value, the next dump advances the millisecond component
  // and starts the counter at 1 so filename ordering remains stable.
  private[migrator] val MaxSavepointSequenceValue: Long = 9_999_999_999L

  private[migrator] def isResumeSafeCoordinate(epochMillis: Long, sequence: Long): Boolean =
    epochMillis > 0L &&
      epochMillis < MaxReasonableSeedValue &&
      sequence >= 0L &&
      sequence <= MaxSavepointSequenceValue &&
      (
        sequence < MaxSavepointSequenceValue ||
          epochMillis < MaxReasonableSeedValue - 1L
      )

  private[migrator] def isResumeSafeSortKey(epochMillis: Long, sequence: Long): Boolean =
    if (sequence == -1L)
      epochMillis > 0L && epochMillis < MaxReasonableSeedValue
    else
      isResumeSafeCoordinate(epochMillis, sequence)

  // Filename grammar for savepoints. Two groups:
  //   - new format: `savepoint_<epochMillis>_<counter>.yaml` (tail is the counter)
  //   - legacy:     `savepoint_<epochSeconds>.yaml`          (tail is null)
  // This regex is the single source of truth for parsing filenames produced by this manager.
  // Test helpers and the startup seed both reuse it to stay in sync with what `savepointFilename`
  // writes.
  private[migrator] val SavepointName: scala.util.matching.Regex =
    """^savepoint_(\d+)(?:_(\d+))?\.yaml$""".r
}
