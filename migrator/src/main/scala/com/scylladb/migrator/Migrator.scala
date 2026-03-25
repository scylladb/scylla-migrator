package com.scylladb.migrator

import com.scylladb.migrator.alternator.AlternatorMigrator
import com.scylladb.migrator.config._
import com.scylladb.migrator.scylla.ScyllaMigrator
import org.apache.logging.log4j.{ Level, LogManager }
import org.apache.logging.log4j.core.config.Configurator
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

    Configurator.setRootLevel(Level.WARN)
    Configurator.setLevel("com.scylladb.migrator", Level.INFO)
    Configurator.setLevel("org.apache.spark.scheduler.TaskSetManager", Level.WARN)
    Configurator.setLevel("com.datastax.spark.connector.cql.CassandraConnector", Level.WARN)

    log.info(s"ScyllaDB Migrator ${BuildInfo.version}")

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    try migrate(migratorConfig)
    finally spark.stop()
  }

  /** Run a migration with the given configuration.
    *
    * This is the programmatic entry point used by integration tests (in-process Spark) and by
    * `main` (spark-submit). The caller is responsible for creating and stopping the SparkSession.
    */
  def migrate(config: MigratorConfig)(implicit spark: SparkSession): Unit =
    (config.source, config.target) match {
      case (cqlSource: SourceSettings.Cassandra, scyllaTarget: TargetSettings.Scylla) =>
        val sourceDF = readers.Cassandra.readDataframe(
          spark,
          cqlSource,
          cqlSource.preserveTimestamps,
          config.getSkipTokenRangesOrEmptySet
        )
        ScyllaMigrator.migrate(config, scyllaTarget, sourceDF)
      case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
        readers.Parquet.migrateToScylla(config, parquetSource, scyllaTarget)
      case (cqlSource: SourceSettings.Cassandra, parquetTarget: TargetSettings.Parquet) =>
        ScyllaMigrator.migrateToParquet(cqlSource, parquetTarget, config)
      case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
        AlternatorMigrator.migrateFromDynamoDB(dynamoSource, alternatorTarget, config)
      case (
            dynamoSource: SourceSettings.DynamoDB,
            s3ExportTarget: TargetSettings.DynamoDBS3Export
          ) =>
        AlternatorMigrator.migrateToS3Export(dynamoSource, s3ExportTarget, config)
      case (
            s3Source: SourceSettings.DynamoDBS3Export,
            alternatorTarget: TargetSettings.DynamoDB
          ) =>
        AlternatorMigrator.migrateFromS3Export(s3Source, alternatorTarget, config)
      case (source, target) =>
        sys.error(
          s"Unsupported combination of source and target: " +
            s"${source.getClass.getSimpleName} -> ${target.getClass.getSimpleName}"
        )
    }

}
