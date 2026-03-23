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
          ScyllaMigrator.migrateToParquet(cqlSource, parquetTarget, migratorConfig)
        case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
          AlternatorMigrator.migrateFromDynamoDB(dynamoSource, alternatorTarget, migratorConfig)
        case (
              dynamoSource: SourceSettings.DynamoDB,
              s3ExportTarget: TargetSettings.DynamoDBS3Export
            ) =>
          AlternatorMigrator.migrateToS3Export(dynamoSource, s3ExportTarget, migratorConfig)
        case (
              s3Source: SourceSettings.DynamoDBS3Export,
              alternatorTarget: TargetSettings.DynamoDB
            ) =>
          AlternatorMigrator.migrateFromS3Export(s3Source, alternatorTarget, migratorConfig)
        case (
              aerospikeSource: SourceSettings.Aerospike,
              scyllaTarget: TargetSettings.Scylla
            ) =>
          val sourceDF = readers.Aerospike.readDataframe(spark, aerospikeSource)
          ScyllaMigrator.migrate(migratorConfig, scyllaTarget, sourceDF)
        case (source, target) =>
          val validCombinations =
            """Supported source -> target combinations:
              |  Cassandra/Scylla -> Scylla
              |  Cassandra/Scylla -> Parquet
              |  Parquet          -> Scylla
              |  DynamoDB         -> DynamoDB/Alternator
              |  DynamoDB         -> S3 Export
              |  S3 Export        -> DynamoDB/Alternator
              |  Aerospike        -> Scylla""".stripMargin
          sys.error(
            s"Unsupported combination of source and target: " +
              s"${source.getClass.getSimpleName} -> ${target.getClass.getSimpleName}\n$validCombinations"
          )
      }
    } finally
      spark.stop()
  }

}
