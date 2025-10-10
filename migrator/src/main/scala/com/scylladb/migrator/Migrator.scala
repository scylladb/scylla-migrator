package com.scylladb.migrator

import com.scylladb.migrator.alternator.AlternatorMigrator
import com.scylladb.migrator.config._
import com.scylladb.migrator.scylla.ScyllaMigrator
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

    log.info(s"ScyllaDB Migrator ${BuildInfo.version}")

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    try {
      (migratorConfig.source, migratorConfig.target) match {
        case (cassandraSource: SourceSettings.Cassandra, scyllaTarget: TargetSettings.Scylla) =>
          val sourceDF = readers.Cassandra.readDataframe(
            spark,
            cassandraSource,
            cassandraSource.preserveTimestamps,
            migratorConfig.getSkipTokenRangesOrEmptySet)
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
          val preparedReader = readers.Parquet.prepareParquetReader(
            spark,
            parquetSource,
            migratorConfig.getSkipParquetFilesOrEmptySet)

          val savepointsManager = readers.ParquetSavepointsManager(
            migratorConfig,
            spark.sparkContext
          )

          scala.util.Using.resource(savepointsManager) { manager =>
            preparedReader.configureHadoop(spark)

            val filesToProcess = preparedReader.filesToProcess
            val totalFiles = filesToProcess.size

            if (totalFiles == 0) {
              log.info(
                "No Parquet files to process. Migration may be complete or all files already processed.")
            } else {
              log.info(s"Starting Parquet migration: processing $totalFiles files")

              filesToProcess.zipWithIndex.foreach {
                case (filePath, index) =>
                  val fileNum = index + 1
                  log.info(s"Processing file $fileNum/$totalFiles: $filePath")

                  val singleFileDF = spark.read.parquet(filePath)
                  val sourceDF = scylla.SourceDataFrame(singleFileDF, None, false)

                  scylla.ScyllaParquetMigrator.migrate(
                    migratorConfig,
                    scyllaTarget,
                    sourceDF,
                    manager
                  )

                  manager.markFileAsProcessed(filePath)
                  log.info(s"Successfully processed file $fileNum/$totalFiles: $filePath")
              }
              
              manager.dumpMigrationState("completed")
              log.info(s"Parquet migration completed successfully: $totalFiles files processed")
            }
          }
        case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromDynamoDB(dynamoSource, alternatorTarget, migratorConfig)
        case (
            s3Source: SourceSettings.DynamoDBS3Export,
            alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromS3Export(s3Source, alternatorTarget, migratorConfig)
        case _ =>
          sys.error("Unsupported combination of source and target.")
      }
    } finally {
      spark.stop()
    }
  }

}
