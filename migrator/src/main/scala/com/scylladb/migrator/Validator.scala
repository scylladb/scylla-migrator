package com.scylladb.migrator

import com.scylladb.migrator.alternator.AlternatorValidator
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql.SparkSession
import com.scylladb.migrator.scylla.{ MySQLToScyllaValidator, ScyllaValidator }

object Validator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def runValidation(
    config: MigratorConfig
  )(implicit spark: SparkSession): List[RowComparisonFailure] =
    (config.source, config.target) match {
      case (cassandraSource: SourceSettings.Cassandra, scyllaTarget: TargetSettings.Scylla) =>
        ScyllaValidator.runValidation(cassandraSource, scyllaTarget, config)
      case (dynamoSource: SourceSettings.DynamoDB, alternatorTarget: TargetSettings.DynamoDB) =>
        AlternatorValidator.runValidation(dynamoSource, alternatorTarget, config)
      case (mysqlSource: SourceSettings.MySQL, scyllaTarget: TargetSettings.Scylla) =>
        MySQLToScyllaValidator.runValidation(mysqlSource, scyllaTarget, config)
      case _ =>
        sys.error(
          "Unsupported combination of source and target " +
            s"(found ${config.source.getClass.getSimpleName} and ${config.target.getClass.getSimpleName} settings)"
        )
    }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-validator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.INFO)

    log.info(s"ScyllaDB Migrator Validator ${BuildInfo.version}")

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val failures = runValidation(migratorConfig)

    if (failures.isEmpty) log.info("No comparison failures found - enjoy your day!")
    else {
      val missingCount = failures.count(_.items.exists {
        case RowComparisonFailure.Item.MissingTargetRow => true
        case _                                          => false
      })
      val differingCount = failures.count(_.items.exists {
        case _: RowComparisonFailure.Item.DifferingFieldValues => true
        case _                                                 => false
      })
      val mismatchedColumnCount = failures.count(_.items.exists {
        case RowComparisonFailure.Item.MismatchedColumnCount => true
        case _                                               => false
      })
      val mismatchedColumnNames = failures.count(_.items.exists {
        case RowComparisonFailure.Item.MismatchedColumnNames => true
        case _                                               => false
      })

      val breakdown = List(
        if (missingCount > 0) Some(s"$missingCount missing target row(s)") else None,
        if (differingCount > 0) Some(s"$differingCount differing field value(s)") else None,
        if (mismatchedColumnCount > 0) Some(s"$mismatchedColumnCount mismatched column count(s)")
        else None,
        if (mismatchedColumnNames > 0) Some(s"$mismatchedColumnNames mismatched column name(s)")
        else None
      ).flatten.mkString(", ")

      log.error(s"Found ${failures.size} comparison failure(s): $breakdown")
      log.error(failures.mkString("\n"))
      System.exit(1)
    }
  }
}
