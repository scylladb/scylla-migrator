package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql.SparkSession

object Validator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def runValidation(config: MigratorConfig)(
    implicit spark: SparkSession): List[RowComparisonFailure] = {
    val sourceSettings = config.source match {
      case s: SourceSettings.Cassandra => s
      case otherwise =>
        throw new RuntimeException(
          s"Validation only supports validating against Cassandra/Scylla " +
            s"(found ${otherwise.getClass.getSimpleName} settings)")
    }

    val sourceConnector: CassandraConnector =
      Connectors.sourceConnector(spark.sparkContext.getConf, sourceSettings)
    val targetConnector: CassandraConnector =
      Connectors.targetConnector(spark.sparkContext.getConf, config.target)

    val renameMap = config.renames.map(rename => rename.from -> rename.to).toMap
    val sourceTableDef =
      Schema.tableFromCassandra(sourceConnector, sourceSettings.keyspace, sourceSettings.table)

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = renameMap.getOrElse(colDef.columnName, colDef.columnName)

          if (sourceSettings.preserveTimestamps)
            List(
              ColumnName(colDef.columnName, Some(alias)),
              WriteTime(colDef.columnName, Some(alias + "_writetime")),
              TTL(colDef.columnName, Some(alias + "_ttl"))
            )
          else List(ColumnName(colDef.columnName, Some(alias)))
        }

      val primaryKeyProjection =
        (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          .map(colDef => ColumnName(colDef.columnName, renameMap.get(colDef.columnName)))

      spark.sparkContext
        .cassandraTable(sourceSettings.keyspace, sourceSettings.table)
        .withConnector(sourceConnector)
        .withReadConf(
          ReadConf
            .fromSparkConf(spark.sparkContext.getConf)
            .copy(
              splitCount      = sourceSettings.splitCount,
              fetchSizeInRows = sourceSettings.fetchSize
            )
        )
        .select(primaryKeyProjection ++ regularColumnsProjection: _*)
    }

    val joined = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val renamedColName = renameMap.getOrElse(colDef.columnName, colDef.columnName)

          if (sourceSettings.preserveTimestamps)
            List(
              ColumnName(renamedColName),
              WriteTime(renamedColName, Some(renamedColName + "_writetime")),
              TTL(renamedColName, Some(renamedColName + "_ttl"))
            )
          else List(ColumnName(renamedColName))
        }

      val primaryKeyProjection =
        (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      val joinKey = (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
        .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      source
        .leftJoinWithCassandraTable(
          config.target.keyspace,
          config.target.table,
          SomeColumns(primaryKeyProjection ++ regularColumnsProjection: _*),
          SomeColumns(joinKey: _*))
        .withConnector(targetConnector)
    }

    joined
      .flatMap {
        case (l, r) =>
          RowComparisonFailure.compareRows(
            l,
            r,
            config.validation.floatingPointTolerance,
            config.validation.ttlToleranceMillis,
            config.validation.writetimeToleranceMillis,
            config.validation.compareTimestamps
          )
      }
      .take(config.validation.failuresToFetch)
      .toList
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-validator")
      .config("spark.cassandra.dev.customFromDriver", "com.scylladb.migrator.CustomUUIDConverter")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.INFO)

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val failures = runValidation(migratorConfig)

    if (failures.isEmpty) log.info("No comparison failures found - enjoy your day!")
    else {
      log.error("Found the following comparison failures:")
      log.error(failures.mkString("\n"))
    }
  }
}
