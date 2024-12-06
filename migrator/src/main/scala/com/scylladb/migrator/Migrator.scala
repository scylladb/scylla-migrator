package com.scylladb.migrator

import com.scylladb.migrator.config._
import com.scylladb.migrator.scylla.ScyllaMigrator
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql._

import scala.util.Properties

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

    log.info(s"Spark Version: ${SPARK_VERSION}")
    log.info(s"Scala Version: ${Properties.versionNumberString}")
    log.info(s"ScyllaDB Migrator ${BuildInfo.version}")

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    try {
      (migratorConfig.source, migratorConfig.target) match {
        case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
          val sourceDF = readers.Parquet.readDataFrame(spark, parquetSource)
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case _ =>
          sys.error("Unsupported combination of source and target.")
      }
    } finally {
      log.info("Successfully migrated data from Spark to Scylla")
    }
  }

}
