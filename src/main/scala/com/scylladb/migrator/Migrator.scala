package com.scylladb.migrator

import com.scylladb.migrator.config._
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import sun.misc.{ Signal, SignalHandler }

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import scala.util.control.NonFatal

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    val sourceDF =
      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          readers.Cassandra.readDataframe(
            spark,
            cassandraSource,
            cassandraSource.preserveTimestamps,
            migratorConfig.skipTokenRanges)
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    log.info("Starting write...")

    try {
      migratorConfig.target match {
        case target: TargetSettings.Scylla =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns)
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      scheduler.shutdown()
      spark.stop()
    }
  }

  def dumpAccumulatorState(config: MigratorConfig, reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges
    )

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor, config: MigratorConfig): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpAccumulatorState(config, "schedule")
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")

    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
  }

  def addUSR2Handler(config: MigratorConfig) = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpAccumulatorState(config, signal.toString)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
  }
}
