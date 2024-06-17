package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import com.scylladb.migrator.writers
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }
import sun.misc.{ Signal, SignalHandler }

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import scala.util.control.NonFatal

case class SourceDataFrame(dataFrame: DataFrame,
                           timestampColumns: Option[TimestampColumns],
                           savepointsSupported: Boolean)

object ScyllaMigrator {
  val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  def migrate(migratorConfig: MigratorConfig,
              target: TargetSettings.Scylla,
              sourceDF: SourceDataFrame)(implicit spark: SparkSession): Unit = {

    val scheduler = new ScheduledThreadPoolExecutor(1)

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    val tokenRangeAccumulator =
      if (!sourceDF.savepointsSupported) None
      else {
        val tokenRangeAccumulator = TokenRangeAccumulator.empty
        spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

        addUSR2Handler(migratorConfig, tokenRangeAccumulator)
        startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator)

        Some(tokenRangeAccumulator)
      }

    log.info(
      "We need to transfer: " + sourceDF.dataFrame.rdd.getNumPartitions + " partitions in total")

    if (migratorConfig.source.isInstanceOf[SourceSettings.Cassandra]) {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => {
        p.asInstanceOf[CassandraPartition[_, _]]
      })
      val allTokenRangesBuilder = Set.newBuilder[(Token[_], Token[_])]
      cassandraPartitions.foreach(p => {
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach(tr => {
            val range: (Token[_], Token[_]) =
              (tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]])
            allTokenRangesBuilder += range
          })

      })
      val allTokenRanges = allTokenRangesBuilder.result()

      log.info("All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (migratorConfig.skipTokenRanges != None) {
        log.info(
          "Savepoints array defined, size of the array: " + migratorConfig.skipTokenRanges.size)

        val diff = allTokenRanges.diff(migratorConfig.skipTokenRanges)
        log.info("Diff ... total diff of full ranges to savepoints is: " + diff.size)
        log.debug("Dump of the missing tokens: ")
        log.debug(diff)
      }
    }

    log.info("Starting write...")

    try {
      writers.Scylla.writeDataframe(
        target,
        migratorConfig.renames,
        sourceDF.dataFrame,
        sourceDF.timestampColumns,
        tokenRangeAccumulator)
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      tokenRangeAccumulator.foreach(dumpAccumulatorState(migratorConfig, _, "final"))
      scheduler.shutdown()
    }
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

}
