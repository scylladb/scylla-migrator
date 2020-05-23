package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }

import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.config._
import com.scylladb.migrator.writer.Writer
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._
import sun.misc.{ Signal, SignalHandler }

import scala.util.control.NonFatal

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.cassandra.dev.customFromDriver", "com.scylladb.migrator.CustomUUIDConverter")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.INFO)

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val (sourceDF, timestampColumns) =
      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          readers.Cassandra.readDataframe(
            cassandraSource,
            migratorConfig.preserveTimestamps,
            migratorConfig.skipTokenRanges)
        case otherwise => ???
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.printSchema()

    log.info("Starting write...")

    val tokenRangeAccumulator = TokenRangeAccumulator.empty
    spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    addUSR2Handler(migratorConfig, tokenRangeAccumulator)
    startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator)

    try {
      Writer.writeDataframe(
        migratorConfig.target,
        migratorConfig.renames,
        sourceDF,
        timestampColumns,
        Some(tokenRangeAccumulator))
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      dumpAccumulatorState(migratorConfig, tokenRangeAccumulator, "final")
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def dumpAccumulatorState(config: MigratorConfig,
                           accumulator: TokenRangeAccumulator,
                           reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize
    val rangesToSkip = accumulator.value.get.map(range =>
      (range.range.start.asInstanceOf[Token[_]], range.range.end.asInstanceOf[Token[_]]))

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges ++ rangesToSkip
    )

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))

    log.info(
      s"Created a savepoint config at ${filename} due to ${reason}. Ranges added: ${rangesToSkip}")
  }

  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor,
                             config: MigratorConfig,
                             acc: TokenRangeAccumulator): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpAccumulatorState(config, acc, "schedule")
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")

    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
  }

  def addUSR2Handler(config: MigratorConfig, acc: TokenRangeAccumulator) = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpAccumulatorState(config, acc, signal.toString)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
  }
}
