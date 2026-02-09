package com.scylladb.migrator

import com.scylladb.migrator.config._

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Main entry point for the ScyllaDB Migrator.
 * 
 * This class extends the existing Migrator to add support for MariaDB sources.
 * 
 * Usage:
 *   spark-submit --class com.scylladb.migrator.Migrator \
 *     --master spark://master:7077 \
 *     --conf spark.scylla.config=/path/to/config.yaml \
 *     scylla-migrator-assembly.jar
 */
object Migrator {
  private val log = LoggerFactory.getLogger(classOf[Migrator.type])
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .getOrCreate()
    
    try {
      val config = MigratorConfig.loadFromSpark(spark.sparkContext.getConf) match {
        case Right(c) => c
        case Left(err) => 
          log.error(s"Failed to load configuration: $err")
          sys.exit(1)
      }
      
      log.info("Configuration loaded successfully")
      
      config.source match {
        case MigratorConfig.SourceConfig.MariaDBSource(settings) =>
          log.info("Starting MariaDB to ScyllaDB migration")
          migrateFromMariaDB(spark, settings, config.target, config.renames)
          
        case MigratorConfig.SourceConfig.CassandraSource(settings) =>
          log.info("Starting Cassandra to ScyllaDB migration")
          migrateFromCassandra(spark, settings, config.target, config.renames)
          
        // Add other source types as needed
      }
      
      log.info("Migration completed successfully")
      
    } catch {
      case e: Exception =>
        log.error(s"Migration failed: ${e.getMessage}", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Migrate from MariaDB to ScyllaDB
   */
  private def migrateFromMariaDB(
    spark: SparkSession,
    sourceSettings: SourceSettings.MariaDB,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): Unit = {
    // Convert to internal settings format
    val internalSettings = SourceSettings.MariaDB.toSourceSettings(sourceSettings)
    
    // Run the migration
    MariaDBMigrator.migrate(spark, internalSettings, targetSettings, renames)
  }
  
  /**
   * Migrate from Cassandra to ScyllaDB
   * This is a placeholder - in the actual codebase, this would call the existing
   * Cassandra migration logic.
   */
  private def migrateFromCassandra(
    spark: SparkSession,
    sourceSettings: CassandraSourceSettings,
    targetSettings: TargetSettings.Scylla,
    renames: Option[Renames]
  ): Unit = {
    // This would call the existing Cassandra migration logic
    // For now, just log a message
    log.info("Cassandra migration would be handled by existing CassandraConnectorMigrator")
    throw new UnsupportedOperationException(
      "Cassandra source should use the existing scylla-migrator implementation"
    )
  }
}

class Migrator
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
          readers.Parquet.migrateToScylla(migratorConfig, parquetSource, scyllaTarget)(spark)
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
