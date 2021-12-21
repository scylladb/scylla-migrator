package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.{ CassandraRowWriter, TokenRangeAccumulator, WriteConf }
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.{ DateTime, DateTimeZone }

object Validator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def runValidation(config: MigratorConfig)(
    implicit spark: SparkSession): RDD[RowComparisonFailure] = {
    val sourceSettings = config.source match {
      case s: SourceSettings.Cassandra => s
      case otherwise =>
        throw new RuntimeException(
          s"Validation only supports validating against Cassandra/Scylla " +
            s"(found ${otherwise.getClass.getSimpleName} settings)")
    }

    val targetSettings = config.target match {
      case s: TargetSettings.Scylla => s
      case otherwise =>
        throw new RuntimeException(
          s"Validation only supports validating against Cassandra/Scylla " +
            s"(found ${otherwise.getClass.getSimpleName} settings)")

    }

    val sourceConnector: CassandraConnector =
      Connectors.sourceConnector(spark.sparkContext.getConf, sourceSettings)
    val targetConnector: CassandraConnector =
      Connectors.targetConnector(spark.sparkContext.getConf, targetSettings)

    val writetimeCutoff = DateTime.now(DateTimeZone.UTC).getMillis() * 1000;

    val renameMap = config.renames.map(rename => rename.from -> rename.to).toMap
    val sourceTableDef =
      sourceConnector.withSessionDo(
        Schema.tableFromCassandra(_, sourceSettings.keyspace, sourceSettings.table))

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
          else if (!colDef.isCollection)
            List(
              ColumnName(colDef.columnName, Some(alias)),
              WriteTime(colDef.columnName, Some(alias + "_writetime")),
              TTL(colDef.columnName, Some(alias + "_ttl"))
            )
          else List(ColumnName(colDef.columnName))
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
          else if (!colDef.isCollection)
            List(
              ColumnName(renamedColName),
              WriteTime(renamedColName, Some(renamedColName + "_writetime")),
              TTL(colDef.columnName, Some(renamedColName + "_ttl"))
            )
          else
            List(ColumnName(renamedColName))
        }

      val primaryKeyProjection =
        (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      val joinKey = (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
        .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      source
        .leftJoinWithCassandraTable(
          targetSettings.keyspace,
          targetSettings.table,
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
            writetimeCutoff,
            config.validation.floatingPointTolerance,
            config.validation.ttlToleranceMillis,
            config.validation.writetimeToleranceMillis,
            config.validation.compareTimestamps
          )
      }
  }

//  def remediateValidation(migratorConfig: MigratorConfig, rdd: RDD[RowComparisonFailure])(
//    implicit spark: SparkSession): RDD[CassandraRow] = {
//    // get all repairable rows
//    val newRDD: RDD[CassandraRow] = rdd
//      .flatMap { failure =>
//        (failure) match {
//          case RowComparisonFailure(row, _, List(RowComparisonFailure.Item.MissingTargetRow)) =>
//            Some(row) //new CassandraSQLRow(row.metaData, row.columnValues))
//          case RowComparisonFailure(
//              row,
//              _,
//              List(RowComparisonFailure.Item.DifferingFieldValues(_))) =>
//            Some(row) //new CassandraSQLRow(row.metaData, row.columnValues))
//          case default => {
//            log.error("Unrepairable comparison failure:\n${default.mkString()}")
//            None
//          }
//        }
//      }
//
//    val sourceDF =
//      migratorConfig.source match {
//        case cassandraSource: SourceSettings.Cassandra =>
//          readers.Cassandra.readDataframe(
//            spark,
//            cassandraSource,
//            cassandraSource.preserveTimestamps,
//            migratorConfig.skipTokenRanges)
//        case parquetSource: SourceSettings.Parquet =>
//          readers.Parquet.readDataFrame(spark, parquetSource)
//        case dynamoSource: SourceSettings.DynamoDB =>
//          val tableDesc = DynamoUtils
//            .buildDynamoClient(dynamoSource.endpoint, dynamoSource.credentials, dynamoSource.region)
//            .describeTable(dynamoSource.table)
//            .getTable
//
//          readers.DynamoDB.readDataFrame(spark, dynamoSource, tableDesc)
//      }
//
//    val tokenRangeAccumulator = TokenRangeAccumulator.empty
//    spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")
//
//    migratorConfig.target match {
//      case target: TargetSettings.Scylla =>
//        writers.Scylla.writeDataframe(
//          target,
//          migratorConfig.renames,
//          sourceDF.dataFrame,
//          sourceDF.timestampColumns,
//          Some(tokenRangeAccumulator));
//      case target_ => {}
//    }
//
//  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-validator")
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

    val failures = runValidation(migratorConfig).cache

    val failureCount = failures.count()
    if (failureCount <= 0) {
      log.info("No comparison failures found - enjoy your day!")
    } else {
      log.error(s"Found ${failureCount} comparison failures")
      val timestamp = DateTime.now(DateTimeZone.UTC).getMillis();

      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          failures
            .coalesce(1)
            .saveAsTextFile(
              s"gs://dataproc-7290e922-fdf8-4832-a421-dd157b235d2d-us-east1/output/${cassandraSource.keyspace}/${cassandraSource.table}/${timestamp}/")
        case _ => {}
      }

//      remediateValidation(migratorConfig, failures)
    }
  }
}
