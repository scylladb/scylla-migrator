package com.scylladb.migrator.scylla

import com.datastax.spark.connector.{
  toRDDFunctions,
  toSparkContextFunctions,
  ColumnName,
  SomeColumns,
  TTL,
  WriteTime
}
import com.datastax.spark.connector.cql.{ CassandraConnector, Schema }
import com.datastax.spark.connector.writer.{ CassandraRowWriter, WriteConf }
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import com.scylladb.migrator.ConsistencyLevelUtils
import com.datastax.spark.connector.rdd.ReadConf

/** The C* to Scylla migration validator */
object ScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  /** Validates that the target Scylla database contains the same data as the source Cassandra
    * database.
    *
    * When `copyMissingRows` is enabled in the validation config, rows that exist in the source but
    * are missing in the target are copied to the target. Note that the returned failure list is a
    * snapshot taken ''before'' the copy, so it may contain `MissingTargetRow` entries for rows that
    * have since been written. A subsequent re-validation can be used to confirm convergence.
    *
    * @return
    *   A list of comparison failures (which is empty if the data are the same in both databases).
    */
  def runValidation(
    sourceSettings: SourceSettings.Cassandra,
    targetSettings: TargetSettings.Scylla,
    config: MigratorConfig
  )(implicit spark: SparkSession): List[RowComparisonFailure] = {

    val validationConfig =
      config.validation.getOrElse(
        sys.error("Missing required property 'validation' in the configuration file.")
      )

    val sourceConnector: CassandraConnector =
      Connectors.sourceConnector(spark.sparkContext.getConf, sourceSettings)
    val targetConnector: CassandraConnector =
      Connectors.targetConnector(spark.sparkContext.getConf, targetSettings)

    val sourceTableDef =
      sourceConnector.withSessionDo(
        Schema.tableFromCassandra(_, sourceSettings.keyspace, sourceSettings.table)
      )

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = config.renamesMap(colDef.columnName)

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
          .map(colDef => ColumnName(colDef.columnName, config.renamesMap.get(colDef.columnName)))

      val consistencyLevel =
        ConsistencyLevelUtils.parseConsistencyLevel(sourceSettings.consistencyLevel)
      log.info(
        s"Using consistencyLevel [${consistencyLevel}] for VALIDATOR SOURCE based on validator source config [${sourceSettings.consistencyLevel}]"
      )
      val consistencyLevel =
        ConsistencyLevelUtils.parseConsistencyLevel(sourceSettings.consistencyLevel)
      log.info(
        s"Using consistencyLevel [${consistencyLevel}] for VALIDATOR SOURCE based on validator source config [${sourceSettings.consistencyLevel}]"
      )

      spark.sparkContext
        .cassandraTable(sourceSettings.keyspace, sourceSettings.table)
        .withConnector(sourceConnector)
        .withReadConf(
          ReadConf
            .fromSparkConf(spark.sparkContext.getConf)
            .copy(
              splitCount       = sourceSettings.splitCount,
              fetchSizeInRows  = sourceSettings.fetchSize,
              consistencyLevel = consistencyLevel
            )
        )
        .select(primaryKeyProjection ++ regularColumnsProjection: _*)
    }

    val joined = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val renamedColName = config.renamesMap(colDef.columnName)

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
          .map(colDef => ColumnName(config.renamesMap(colDef.columnName)))

      val joinKey = (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
        .map(colDef => ColumnName(config.renamesMap(colDef.columnName)))

      source
        .leftJoinWithCassandraTable(
          targetSettings.keyspace,
          targetSettings.table,
          SomeColumns(primaryKeyProjection ++ regularColumnsProjection: _*),
          SomeColumns(joinKey: _*)
        )
        .withConnector(targetConnector)
    }

    val cachedJoined =
      if (validationConfig.copyMissingRows) joined.persist(StorageLevel.MEMORY_AND_DISK)
      else joined

    try {
      val failures = cachedJoined
        .flatMap { case (l, r) =>
          RowComparisonFailure.compareCassandraRows(
            l,
            r,
            validationConfig.floatingPointTolerance,
            validationConfig.timestampMsTolerance,
            validationConfig.ttlToleranceMillis,
            validationConfig.writetimeToleranceMillis,
            validationConfig.compareTimestamps
          )
        }
        .take(validationConfig.failuresToFetch)
        .toList

      if (validationConfig.copyMissingRows) {
        log.info("Copying missing rows from source to target")

        val writeColumnSelector = {
          val regularColumns = sourceTableDef.regularColumns.map { colDef =>
            ColumnName(config.renamesMap(colDef.columnName))
          }
          val primaryKeyColumns =
            (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
              .map(colDef => ColumnName(config.renamesMap(colDef.columnName)))
          SomeColumns(primaryKeyColumns ++ regularColumns: _*)
        }

        val targetConsistencyLevel =
          ConsistencyLevelUtils.parseConsistencyLevel(targetSettings.consistencyLevel)

        val writeConf = WriteConf
          .fromSparkConf(spark.sparkContext.getConf)
          .copy(consistencyLevel = targetConsistencyLevel)

        cachedJoined
          .filter { case (_, r) => r.isEmpty }
          .map { case (sourceRow, _) => sourceRow }
          .saveToCassandra(
            targetSettings.keyspace,
            targetSettings.table,
            writeColumnSelector,
            writeConf
          )(targetConnector, CassandraRowWriter.Factory)

        log.info("Finished copying missing rows to target")
      }

      failures
    } finally
      if (validationConfig.copyMissingRows) cachedJoined.unpersist()
  }

}
