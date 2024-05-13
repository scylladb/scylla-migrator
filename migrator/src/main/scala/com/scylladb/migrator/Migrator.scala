package com.scylladb.migrator

import com.scylladb.migrator.alternator.AlternatorMigrator
import com.scylladb.migrator.config._
import com.scylladb.migrator.scylla.ScyllaMigrator
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

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
            migratorConfig.skipTokenRanges)
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
          val sourceDF = readers.Parquet.readDataFrame(spark, parquetSource)
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromDynamoDB(
            dynamoSource,
            alternatorTarget,
            migratorConfig.renames)
        case (
            s3Source: SourceSettings.DynamoDBS3Export,
            alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromS3Export(s3Source, alternatorTarget, migratorConfig.renames)
        case _ =>
          sys.error("Unsupported combination of source and target.")
      }
    } finally {
      spark.stop()
    }
  }

}
