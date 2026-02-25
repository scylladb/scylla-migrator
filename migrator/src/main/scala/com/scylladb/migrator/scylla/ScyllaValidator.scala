package com.scylladb.migrator.scylla

import com.datastax.spark.connector.{
  toSparkContextFunctions,
  ColumnName,
  SomeColumns,
  TTL,
  WriteTime
}
import com.datastax.spark.connector.cql.{ CassandraConnector, Schema }
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.spark.connector.rdd.ReadConf

/** The C* to Scylla migration validator */
object ScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla")

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

      val consistencyLevel = sourceSettings.consistencyLevel match {
        case "LOCAL_QUORUM" => ConsistencyLevel.LOCAL_QUORUM
        case "QUORUM"       => ConsistencyLevel.QUORUM
        case "LOCAL_ONE"    => ConsistencyLevel.LOCAL_ONE
        case "ONE"          => ConsistencyLevel.ONE
        case _              => ConsistencyLevel.LOCAL_QUORUM
      }
      if (consistencyLevel.toString == sourceSettings.consistencyLevel) {
        log.info(
          s"Using consistencyLevel [${consistencyLevel}] for VALIDATOR SOURCE based on validator source config [${sourceSettings.consistencyLevel}]"
        )
      } else {
        log.info(
          s"Using DEFAULT consistencyLevel [${consistencyLevel}] for VALIDATOR SOURCE based on unrecognized validator source config [${sourceSettings.consistencyLevel}]"
        )
      }

      spark.sparkContext
        .cassandraTable(sourceSettings.keyspace, sourceSettings.table)
        .withConnector(sourceConnector)
        .withReadConf(
          ReadConf
            .fromSparkConf(spark.sparkContext.getConf)
            .copy(
              splitCount = sourceSettings.splitCount,
              fetchSizeInRows = sourceSettings.fetchSize,
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

    joined
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

  }

}
