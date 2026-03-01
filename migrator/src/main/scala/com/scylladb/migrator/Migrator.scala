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
        case (cqlSource: SourceSettings.Cassandra, scyllaTarget: TargetSettings.Scylla) =>
          val sourceDF = readers.Cassandra.readDataframe(
            spark,
            cqlSource,
            cqlSource.preserveTimestamps,
            migratorConfig.getSkipTokenRangesOrEmptySet
          )
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
          readers.Parquet.migrateToScylla(migratorConfig, parquetSource, scyllaTarget)(spark)
        case (cqlSource: SourceSettings.Cassandra, parquetTarget: TargetSettings.Parquet) =>
          if (cqlSource.preserveTimestamps)
            log.warn(
              "preserveTimestamps is enabled but Parquet target does not support CQL TTL/writetime metadata. " +
                "The additional timestamp columns will be included in the Parquet schema but may not be useful."
            )
          if (migratorConfig.skipTokenRanges.exists(_.nonEmpty))
            log.warn(
              "skipTokenRanges is set but may not behave as expected with a Parquet target, " +
                "since Parquet output is not token-range-aware."
            )
          val sourceDF = readers.Cassandra.readDataframe(
            spark,
            cqlSource,
            cqlSource.preserveTimestamps,
            migratorConfig.getSkipTokenRangesOrEmptySet
          )
          writers.Parquet.writeDataframe(parquetTarget, sourceDF.dataFrame)
        case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromDynamoDB(dynamoSource, alternatorTarget, migratorConfig)
        case (
              dynamoSource: SourceSettings.DynamoDB,
              s3ExportTarget: TargetSettings.DynamoDBS3Export
            ) =>
          val (sourceRDD, sourceTableDesc) =
            readers.DynamoDB.readRDD(spark, dynamoSource, migratorConfig.skipSegments)
          writers.DynamoDBS3Export.writeRDD(s3ExportTarget, sourceRDD, Some(sourceTableDesc))
        case (
              s3Source: SourceSettings.DynamoDBS3Export,
              alternatorTarget: TargetSettings.DynamoDB
            ) =>
          AlternatorMigrator.migrateFromS3Export(s3Source, alternatorTarget, migratorConfig)
        case (source, target) =>
          sys.error(
            s"Unsupported combination of source and target: " +
              s"${source.getClass.getSimpleName} -> ${target.getClass.getSimpleName}"
          )
      }
    } finally
      spark.stop()
  }

}
