package com.scylladb.migrator.scylla

import com.datastax.spark.connector.{
  toRDDFunctions,
  toSparkContextFunctions,
  ColumnName,
  SomeColumns,
  TTL,
  WriteTime
}
import com.datastax.spark.connector.cql.{ CassandraConnector, Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.{ readers, writers, Connectors, ConsistencyLevelUtils }
import com.scylladb.migrator.config.{ CopyType, MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.cassandra.DataTypeConverter
import org.apache.spark.sql.types.{ IntegerType, LongType, StructField, StructType }
import org.apache.spark.storage.StorageLevel

/** The C* to Scylla migration validator */
object ScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  private def buildRepairSchema(
    sourceTableDef: TableDef,
    renameColumn: String => String,
    includePerColumnMetadata: Boolean
  ): StructType = {
    val primaryKeyFields =
      (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns).map { colDef =>
        DataTypeConverter.toStructField(colDef).copy(name = renameColumn(colDef.columnName))
      }

    val regularFields =
      sourceTableDef.regularColumns.flatMap { colDef =>
        val renamedColumn = renameColumn(colDef.columnName)
        val field = DataTypeConverter.toStructField(colDef).copy(name = renamedColumn)

        if (includePerColumnMetadata)
          Seq(
            field,
            StructField(s"${renamedColumn}_ttl", IntegerType, true),
            StructField(s"${renamedColumn}_writetime", LongType, true)
          )
        else Seq(field)
      }

    StructType(primaryKeyFields ++ regularFields)
  }

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

    val includePerColumnMetadata =
      readers.Cassandra
        .determineCopyType(sourceTableDef, sourceSettings.preserveTimestamps)
        .fold(
          err => throw err,
          copyType => copyType == CopyType.WithTimestampPreservation
        )

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = config.renamesMap(colDef.columnName)

          if (includePerColumnMetadata)
            List(
              ColumnName(colDef.columnName, Some(alias)),
              TTL(colDef.columnName, Some(alias + "_ttl")),
              WriteTime(colDef.columnName, Some(alias + "_writetime"))
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

          if (includePerColumnMetadata)
            List(
              ColumnName(renamedColName),
              TTL(renamedColName, Some(renamedColName + "_ttl")),
              WriteTime(renamedColName, Some(renamedColName + "_writetime"))
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

        val repairSchema =
          buildRepairSchema(sourceTableDef, config.renamesMap, includePerColumnMetadata)
        val repairFieldNames = repairSchema.fieldNames.toIndexedSeq

        val missingRowsRdd =
          cachedJoined.filter { case (_, r) => r.isEmpty }
        val missingSourceRowCount = missingRowsRdd.count()

        val rawRepairDf = spark.createDataFrame(
          missingRowsRdd.map { case (sourceRow, _) =>
            Row.fromSeq(
              repairFieldNames.map { fieldName =>
                readers.Cassandra.convertValue(sourceRow.getRaw(fieldName))
              }
            )
          },
          repairSchema
        )

        val (repairDf, timestampColumns) =
          if (includePerColumnMetadata) {
            val (df, cols) = readers.Cassandra.explodeDataframeFromPerColumnMeta(spark, rawRepairDf)
            (df, Some(cols))
          } else {
            (rawRepairDf, None)
          }

        writers.Scylla.writeDataframe(
          targetSettings,
          Nil,
          repairDf,
          timestampColumns,
          None,
          sourceSettings
        )

        log.info(
          s"Finished copying missing rows to target: $missingSourceRowCount missing row(s) copied"
        )
      }

      failures
    } finally
      if (validationConfig.copyMissingRows) cachedJoined.unpersist()
  }

}
