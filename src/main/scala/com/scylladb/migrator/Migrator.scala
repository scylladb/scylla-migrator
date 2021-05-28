package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }

//import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.config._
import com.scylladb.migrator.readers.Cassandra
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
//import org.apache.spark.streaming.kinesis.{ KinesisInputDStream, SparkAWSCredentials }
import sun.misc.{ Signal, SignalHandler }

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
//        case parquetSource: SourceSettings.Parquet =>
//          readers.Parquet.readDataFrame(spark, parquetSource)
//        case dynamoSource: SourceSettings.DynamoDB =>
//          val tableDesc = DynamoUtils
//            .buildDynamoClient(dynamoSource.endpoint, dynamoSource.credentials, dynamoSource.region)
//            .describeTable(dynamoSource.table)
//            .getTable
//
//          readers.DynamoDB.readDataFrame(spark, dynamoSource, tableDesc)
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

//    val tokenRangeAccumulator =
//      if (!sourceDF.savepointsSupported) None
//      else {
//        val tokenRangeAccumulator = TokenRangeAccumulator.empty
//        spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")
//
//        addUSR2Handler(migratorConfig, tokenRangeAccumulator)
//        startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator)
//
//        Some(tokenRangeAccumulator)
//      }

    log.info("Starting write...")

    try {
      migratorConfig.target match {
        case target: TargetSettings.Scylla =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns)
//            tokenRangeAccumulator)
//        case target: TargetSettings.DynamoDB =>
//          val sourceAndDescriptions = migratorConfig.source match {
//            case source: SourceSettings.DynamoDB =>
//              if (target.streamChanges) {
//                log.info(
//                  "Source is a Dynamo table and change streaming requested; enabling Dynamo Stream")
//                DynamoUtils.enableDynamoStream(source)
//              }
//              val sourceDesc =
//                DynamoUtils
//                  .buildDynamoClient(source.endpoint, source.credentials, source.region)
//                  .describeTable(source.table)
//                  .getTable
//
//              Some(
//                (
//                  source,
//                  sourceDesc,
//                  DynamoUtils.replicateTableDefinition(
//                    sourceDesc,
//                    target
//                  )
//                ))
//
//            case _ =>
//              None
//          }
//
//          writers.DynamoDB.writeDataframe(
//            target,
//            migratorConfig.renames,
//            sourceDF.dataFrame,
//            sourceAndDescriptions.map(_._3))
//
//          sourceAndDescriptions.foreach {
//            case (source, sourceDesc, targetDesc) =>
//              log.info("Done transferring table snapshot. Starting to transfer changes")
//
//              DynamoStreamReplication.createDStream(
//                spark,
//                streamingContext,
//                source,
//                target,
//                sourceDF.dataFrame.schema,
//                sourceDesc,
//                targetDesc,
//                migratorConfig.renames)
//
//              streamingContext.start()
//              streamingContext.awaitTermination()
//          }
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
//      tokenRangeAccumulator.foreach(dumpAccumulatorState(migratorConfig, _, "final"))
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def dumpAccumulatorState(config: MigratorConfig,
//                           accumulator: TokenRangeAccumulator,
                           reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize
//    val rangesToSkip = accumulator.value.get.map(range =>
//      (range.range.start.asInstanceOf[Token[_]], range.range.end.asInstanceOf[Token[_]]))

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges
//        ++ rangesToSkip
    )

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))

//    log.info(
//      s"Created a savepoint config at ${filename} due to ${reason}. Ranges added: ${rangesToSkip}")
  }

//  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor,
//                             config: MigratorConfig//,
////                             acc: TokenRangeAccumulator
//                            ): Unit = {
//    val runnable = new Runnable {
//      override def run(): Unit =
//        try dumpAccumulatorState(config,"schedule")
//        catch {
//          case e: Throwable =>
//            log.error("Could not create the savepoint. This will be retried.", e)
//        }
//    }
//
//    log.info(
//      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")
//
//    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
//  }

//  def addUSR2Handler(config: MigratorConfig, acc: TokenRangeAccumulator) = {
//    log.info(
//      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")
//
//    val handler = new SignalHandler {
//      override def handle(signal: Signal): Unit =
//        dumpAccumulatorState(config, acc, signal.toString)
//    }
//
//    Signal.handle(new Signal("USR2"), handler)
//    Signal.handle(new Signal("TERM"), handler)
//    Signal.handle(new Signal("INT"), handler)
//  }
}
