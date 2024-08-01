package com.scylladb.migrator

import com.scylladb.migrator.config.MigratorConfig
import org.apache.log4j.LogManager
import sun.misc.{ Signal, SignalHandler }

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }

/**
  * A component that manages savepoints. Savepoints provide a way to resume an interrupted migration.
  *
  * This component periodically stores savepoints according to the schedule defined in the configuration.
  * It also automatically stores a savepoint in case of early termination (e.g. due to a SIGTERM signal).
  *
  * Internally, it works by writing modified copies of the original migration configuration. These copies
  * specify which parts of the source dataset have already been migrated and can safely be skipped when
  * restarting the migration.
  *
  * Make sure to call the method `close` when you don’t need the savepoints manager anymore so that it
  * releases the resources it was using.
  *
  * This class is abstract. Subclasses are responsible for implementing how to track the migration progress,
  * and for communicating the updated state of the migration via the method `updateConfigWithMigrationState`.
  */
abstract class SavepointsManager(migratorConfig: MigratorConfig) extends AutoCloseable {

  val log = LogManager.getLogger(this.getClass.getName)
  private val scheduler = new ScheduledThreadPoolExecutor(1)

  createSavepointsDirectory()
  addUSR2Handler()
  startSavepointSchedule()

  private def createSavepointsDirectory(): Unit = {
    val savepointsDirectory = Paths.get(migratorConfig.savepoints.path)
    if (!Files.exists(savepointsDirectory)) {
      log.debug(
        s"Directory ${savepointsDirectory.normalize().toString} does not exist. Creating it...")
      Files.createDirectories(savepointsDirectory)
    }
  }

  private def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  private def addUSR2Handler(): Unit = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpMigrationState(signal.toString)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
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
      s"Starting savepoint schedule; will write a savepoint every ${migratorConfig.savepoints.intervalSeconds} seconds")

    scheduler.scheduleAtFixedRate(
      runnable,
      migratorConfig.savepoints.intervalSeconds,
      migratorConfig.savepoints.intervalSeconds,
      TimeUnit.SECONDS)
  }

  /**
    * Dump the current state of the migration into a configuration file that can be
    * used to resume the migration.
    * @param reason Human-readable, informal, event that caused the dump.
    */
  final def dumpMigrationState(reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(migratorConfig.savepoints.path)).normalize

    val modifiedConfig = updateConfigWithMigrationState()

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))

    log.info(
      s"Created a savepoint config at ${filename} due to ${reason}. ${describeMigrationState()}")
  }

  /**
    * Stop the periodic creation of savepoints and release the associated resources.
    */
  final def close(): Unit =
    scheduler.shutdown()

  /**
    * Provide readable logs by describing which parts of the migration have been completed already.
    */
  def describeMigrationState(): String

  /**
    * A copy of the original migration configuration, updated to describe which parts of the migration
    * have been completed already.
    */
  def updateConfigWithMigrationState(): MigratorConfig

}
