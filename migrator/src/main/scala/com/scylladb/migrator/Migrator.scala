package com.scylladb.migrator

import com.scylladb.migrator.alternator.AlternatorMigrator
import com.scylladb.migrator.config._
import com.scylladb.migrator.scylla.ScyllaMigrator
import org.apache.logging.log4j.{ Level, LogManager }
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    SparkSecretRedaction.ensureMigratorRedactionRegex(sparkConf)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
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
      MigratorConfig.loadFrom(
        spark.conf.get("spark.scylla.config"),
        spark.sparkContext.hadoopConfiguration
      )

    log.info(s"Loaded config:\n${migratorConfig.renderRedacted}")

    try migrate(migratorConfig)
    finally spark.stop()
  }

  /** Run a migration with the given configuration.
    *
    * This is the programmatic entry point used by integration tests (in-process Spark) and by
    * `main` (spark-submit). The caller is responsible for creating and stopping the SparkSession.
    */
  def migrate(config: MigratorConfig)(implicit spark: SparkSession): Unit = {
    // Backend-neutral warning. Any source whose `supportsSavepoints` is `false` (MySQL, DynamoDB
    // S3 export, future non-resumable backends) gets the same up-front notice without per-source
    // `case` branches. Source-specific reader caveats (e.g. MySQL JDBC partitioning semantics)
    // are emitted from the reader itself.
    if (!config.source.supportsSavepoints) {
      log.warn(
        "Source does not support savepoints; any configured savepoints settings are ignored. " +
          "If this migration is interrupted, it must be restarted from scratch. " +
          "Ensure the target table supports idempotent writes."
      )
    }

    (config.source, config.target) match {
      case (cqlSource: SourceSettings.Cassandra, scyllaTarget: TargetSettings.Scylla) =>
        val sourceDF = readers.Cassandra.readDataframe(
          spark,
          cqlSource,
          cqlSource.preserveTimestamps,
          config.getSkipTokenRangesOrEmptySet
        )
        ScyllaMigrator.migrate(config, scyllaTarget, sourceDF)
      case (mysqlSource: SourceSettings.MySQL, scyllaTarget: TargetSettings.Scylla) =>
        log.info("Starting MySQL to ScyllaDB migration")
        val sourceDF = readers.MySQL.readDataframe(spark, mysqlSource)
        ScyllaMigrator.migrate(config, scyllaTarget, sourceDF)
      case (aerospikeSource: SourceSettings.Aerospike, scyllaTarget: TargetSettings.Scylla) =>
        log.info("Starting Aerospike to ScyllaDB migration")
        val sourceDF = readers.Aerospike.readDataframe(spark, aerospikeSource)
        ScyllaMigrator.migrate(config, scyllaTarget, sourceDF)
      case (parquetSource: SourceSettings.Parquet, scyllaTarget: TargetSettings.Scylla) =>
        readers.Parquet.migrateToScylla(config, parquetSource, scyllaTarget)
      case (cqlSource: SourceSettings.Cassandra, parquetTarget: TargetSettings.Parquet) =>
        ScyllaMigrator.migrateToParquet(cqlSource, parquetTarget, config)
      case (dynamoSource: SourceSettings.DynamoDBLike, dynamoTarget: TargetSettings.DynamoDBLike) =>
        AlternatorMigrator.migrateFromDynamoDB(dynamoSource, dynamoTarget, config)
      case (
            dynamoSource: SourceSettings.DynamoDB,
            s3ExportTarget: TargetSettings.DynamoDBS3Export
          ) =>
        AlternatorMigrator.migrateToS3Export(dynamoSource, s3ExportTarget, config)
      case (
            s3Source: SourceSettings.DynamoDBS3Export,
            dynamoTarget: TargetSettings.DynamoDBLike
          ) =>
        AlternatorMigrator.migrateFromS3Export(s3Source, dynamoTarget, config)
      case (source, target) =>
        sys.error(
          s"Unsupported combination of source and target: " +
            s"${source.getClass.getSimpleName} -> ${target.getClass.getSimpleName}"
        )
    }
  }

}
