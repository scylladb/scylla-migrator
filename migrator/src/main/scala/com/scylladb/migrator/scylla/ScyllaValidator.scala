package com.scylladb.migrator.scylla

import com.datastax.spark.connector.{
  toSparkContextFunctions,
  ColumnName,
  SomeColumns,
  TTL,
  WriteTime
}
import com.datastax.spark.connector.cql.{ CassandraConnector, Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.{ readers, writers, Connectors, ConsistencyLevelUtils }
import com.scylladb.migrator.config.{
  CopyType,
  MigratorConfig,
  RepairWritetimeStrategy,
  SourceSettings,
  TargetSettings
}
import com.scylladb.migrator.readers.TimestampColumns
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.cassandra.DataTypeConverter
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  IntegerType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ArraySeq

/** The C* to Scylla migration validator */
object ScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla")

  private def buildRepairSchema(
    sourceTableDef: TableDef,
    renameColumn: String => String,
    includePerColumnMetadata: Boolean,
    nonFrozenCollectionNames: Set[String] = Set.empty
  ): StructType = {
    val primaryKeyFields =
      (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns).map { colDef =>
        val field =
          DataTypeConverter.toStructField(colDef).copy(name = renameColumn(colDef.columnName))
        field.copy(dataType = readers.Cassandra.widenCqlTimestamps(field.dataType))
      }

    val regularFields =
      sourceTableDef.regularColumns.flatMap { colDef =>
        val renamedColumn = renameColumn(colDef.columnName)
        val field = DataTypeConverter.toStructField(colDef).copy(name = renamedColumn)
        val widenedField =
          field.copy(dataType = readers.Cassandra.widenCqlTimestamps(field.dataType))

        if (includePerColumnMetadata) {
          // Non-frozen collections carry one TTL/WRITETIME per element, so their sidecars are
          // arrays; the collection-aware explode re-hydrates them into collection-append passes.
          val isPerElement = nonFrozenCollectionNames.contains(colDef.columnName)
          val ttlType = if (isPerElement) ArrayType(IntegerType) else IntegerType
          val writetimeType = if (isPerElement) ArrayType(LongType) else LongType
          // Stamp the same collection-kind marker the Parquet export writes, so the shared
          // collection-aware explode accepts this trusted, migrator-built repair schema.
          val baseField =
            if (isPerElement)
              readers.Cassandra.taggedCollectionField(widenedField, colDef.columnType)
            else widenedField
          Seq(
            baseField,
            StructField(s"${renamedColumn}_ttl", ttlType, true),
            StructField(s"${renamedColumn}_writetime", writetimeType, true)
          )
        } else Seq(widenedField)
      }

    StructType(primaryKeyFields ++ regularFields)
  }

  /** Spark's `RowEncoder` expects a `Seq` for `ArrayType` columns, but a CQL set read from a plain
    * `CassandraRow` (the validator join uses these, unlike the migration read which uses the
    * connector's already-`Seq` `CassandraSQLRow`) decodes to a Scala `Set`. Coerce sets to an
    * ordered `Seq` so `createDataFrame` can encode the repair rows; the collection-aware explode
    * re-sorts elements by their ordering afterwards, so the interim order is irrelevant.
    */
  private def coerceRepairValue(dataType: DataType, value: Any): Any =
    (dataType, value) match {
      case (_: ArrayType, s: scala.collection.Set[_]) => s.toIndexedSeq
      case _                                          => value
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

    validationConfig.hashColumns.foreach { _ =>
      log.warn(
        "hashColumns is only supported for MySQL-to-ScyllaDB validation and will be ignored."
      )
    }

    if (
      validationConfig.copyMissingRows &&
      validationConfig.repairWritetimeStrategy == RepairWritetimeStrategy.Config &&
      targetSettings.writeWritetimestampInuS.isEmpty
    ) {
      sys.error(
        "repairWritetimeStrategy=config requires target.writeWritetimestampInuS " +
          "to be set in the configuration."
      )
    }

    val sourceConnector: CassandraConnector =
      Connectors.sourceConnector(spark.sparkContext.getConf, sourceSettings)
    val targetConnector: CassandraConnector =
      Connectors.targetConnector(spark.sparkContext.getConf, targetSettings)

    val sourceTableDef =
      sourceConnector.withSessionDo(
        Schema.tableFromCassandra(_, sourceSettings.keyspace, sourceSettings.table)
      )

    val nonFrozenCollectionNames =
      readers.Cassandra.nonFrozenCollectionColumns(sourceTableDef).map(_.columnName).toSet
    val hasNonFrozenCollections = nonFrozenCollectionNames.nonEmpty

    // Whether TTL()/WRITETIME() metadata is selected and compared. For scalar columns and frozen
    // collections these are scalars; under `preserveCollectionTimestamps` non-frozen collection
    // columns select them as per-element (array) lists. `compareCassandraRows` handles both.
    val includePerColumnMetadata =
      readers.Cassandra
        .determineCopyType(
          sourceTableDef,
          sourceSettings.preserveTimestamps,
          sourceSettings.preserveCollectionTimestamps
        )
        .fold(
          err => throw err,
          copyType => copyType == CopyType.WithTimestampPreservation
        )

    // Per-element collection metadata is array-typed. The scalar repair explosion
    // (`explodeRowsFromPerColumnMeta`) cannot represent it, so copy-missing-rows repair for such
    // tables uses the collection-aware explosion (`explodeRowsFromPerColumnMetaCollectionAware`):
    // a scalar/frozen base write plus per-element collection-append passes.
    val perElementCollectionMetadata =
      includePerColumnMetadata &&
        sourceSettings.preserveCollectionTimestamps &&
        hasNonFrozenCollections

    // Repair uses the scalar explosion only when there are no per-element collections.
    val repairWithScalarMetadata = includePerColumnMetadata && !perElementCollectionMetadata

    // The source SELECT and target JOIN below project the collection-wide `WRITETIME(col)`/`TTL(col)`
    // form for per-element metadata. ScyllaDB 2026.2+ only accepts the element-subscript form on
    // non-frozen collections and REJECTS the collection-wide form, so emitting it against such an
    // endpoint fails mid distributed-read. Detect it up front (same probe the migrate path uses):
    // if EITHER endpoint is subscript-only, drop per-element metadata for non-frozen collection
    // columns on BOTH projections and compare those columns by VALUE only (all scalar/frozen
    // metadata is still validated). Comparison must stay symmetric, hence "both".
    val collectionsValueOnly: Boolean =
      if (!perElementCollectionMetadata) false
      else {
        val nonFrozenSource = readers.Cassandra.nonFrozenCollectionColumns(sourceTableDef)
        val sourceStrategy =
          readers.Cassandra.detectMetadataReadStrategy(
            sourceConnector,
            sourceSettings.keyspace,
            sourceSettings.table,
            nonFrozenSource
          )
        val targetTableDef =
          targetConnector.withSessionDo(
            Schema.tableFromCassandra(_, targetSettings.keyspace, targetSettings.table)
          )
        val targetStrategy =
          readers.Cassandra.detectMetadataReadStrategy(
            targetConnector,
            targetSettings.keyspace,
            targetSettings.table,
            readers.Cassandra.nonFrozenCollectionColumns(targetTableDef)
          )
        sourceStrategy == readers.Cassandra.SubscriptMetadataRead ||
        targetStrategy == readers.Cassandra.SubscriptMetadataRead
      }

    if (collectionsValueOnly) {
      log.warn(
        "Validation source/target exposes per-element collection WRITETIME()/TTL() only via the " +
          "subscript form (ScyllaDB 2026.2+). The validator compares non-frozen collection columns " +
          s"by VALUE only (${nonFrozenCollectionNames.mkString(", ")}); their per-element " +
          "TTL/WRITETIME are NOT validated. Scalar and frozen-collection metadata are still compared."
      )
      // copyMissingRows repair for these tables reconstructs per-element collection TTL/WRITETIME
      // from the source metadata read — which is unavailable in value-only mode — so it cannot run.
      if (validationConfig.copyMissingRows)
        sys.error(
          "copyMissingRows is not supported for non-frozen collections when the source or target " +
            "only exposes the subscript form of WRITETIME()/TTL() (ScyllaDB 2026.2+): per-element " +
            "collection metadata cannot be read for repair. Re-run the migration itself (idempotent " +
            "under USING TIMESTAMP) to converge missing rows, or disable copyMissingRows to validate " +
            "values only."
        )
    }

    // Whether to project per-element metadata for a given regular column. Non-frozen collections are
    // dropped to value-only when the endpoint only supports the subscript form.
    def projectMetadataFor(columnName: String): Boolean =
      includePerColumnMetadata &&
        !(collectionsValueOnly && nonFrozenCollectionNames.contains(columnName))

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = config.renamesMap(colDef.columnName)

          if (projectMetadataFor(colDef.columnName))
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

          if (projectMetadataFor(colDef.columnName))
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

      val targetConsistencyLevel =
        ConsistencyLevelUtils.parseConsistencyLevel(targetSettings.consistencyLevel)
      log.info(
        s"Using consistencyLevel [${targetConsistencyLevel}] for VALIDATOR TARGET based on target config [${targetSettings.consistencyLevel}]"
      )

      source
        .leftJoinWithCassandraTable(
          targetSettings.keyspace,
          targetSettings.table,
          SomeColumns(primaryKeyProjection ++ regularColumnsProjection: _*),
          SomeColumns(joinKey: _*),
          readConf = ReadConf
            .fromSparkConf(spark.sparkContext.getConf)
            .copy(consistencyLevel = targetConsistencyLevel)
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
            validationConfig.compareTimestamps,
            validationConfig.numericTypePolicy
          )
        }
        .take(validationConfig.failuresToFetch)
        .toList

      if (validationConfig.copyMissingRows) {
        if (
          !includePerColumnMetadata &&
          validationConfig.repairWritetimeStrategy != RepairWritetimeStrategy.Source
        ) {
          log.warn(
            s"repairWritetimeStrategy=${validationConfig.repairWritetimeStrategy} is configured " +
              "but preserveTimestamps is disabled. The strategy only applies to timestamp-preserving " +
              "repair writes and will be ignored for this run."
          )
        }
        if (perElementCollectionMetadata) {
          log.warn(
            "copyMissingRows for a table with non-frozen (multi-cell) collections under " +
              "preserveCollectionTimestamps: missing rows are repaired with a scalar/frozen base " +
              "write plus per-element collection-append passes, restoring element TTL/WRITETIME. " +
              "With repairWritetimeStrategy=source the original per-element WRITETIMEs are kept. " +
              "With coordinator/config, the override is applied to BOTH the base write and the " +
              "collection appends (per-element WRITETIME granularity is collapsed to the override) " +
              "so the repaired row and its collection elements stay consistent."
          )
          // C2: state the convergence limits explicitly so operators do not treat
          // --copyMissingRows as a general reconciliation tool for these tables.
          log.warn(
            "copyMissingRows LIMITATION for non-frozen collections: only rows entirely absent from " +
              "the target are repaired. A row that already exists on the target but has a DIFFERENT " +
              "collection (missing, extra, or stale elements) is reported by validation but NOT " +
              "converged here — collection-append repair is additive (it can only add elements, " +
              "never remove target-side extras) and re-adding could resurrect elements deleted on " +
              "the target. To converge present-but-incomplete rows, resume the migration itself " +
              "(base insert + appends are idempotent under USING TIMESTAMP) rather than relying on " +
              "validation repair."
          )
        }
        log.info("Copying missing rows from source to target")

        val repairSchema =
          buildRepairSchema(
            sourceTableDef,
            config.renamesMap,
            includePerColumnMetadata,
            nonFrozenCollectionNames
          )

        val missingRowsRdd =
          cachedJoined.filter { case (_, r) => r.isEmpty }.persist(StorageLevel.MEMORY_AND_DISK)
        try {
          val missingSourceRowCount = missingRowsRdd.count()

          if (missingSourceRowCount > 0) {
            val repairFields = repairSchema.fields.toIndexedSeq
            val rawRepairDf = spark.createDataFrame(
              missingRowsRdd.map { case (sourceRow, _) =>
                Row.fromSeq(
                  repairFields.map { field =>
                    coerceRepairValue(
                      field.dataType,
                      readers.Cassandra.widenTimestampValue(
                        readers.Cassandra.convertValue(sourceRow.getRaw(field.name))
                      )
                    )
                  }
                )
              },
              repairSchema
            )

            // Precompute the writetime override (if any) ONCE so the scalar/frozen base write and
            // every collection-append pass are stamped consistently. Lazy so the value-only repair
            // path (no metadata) neither logs nor triggers the `config` validation error.
            //   - source: no override; base + appends keep original per-cell/per-element WRITETIME.
            //   - coordinator/config: override applied to BOTH base and appends. This collapses the
            //     per-element WRITETIMEs to a single value, but keeping the appends on source
            //     WRITETIME while overriding the base would leave the base "resurrected" (live) with
            //     collection elements that are still shadowed by their older tombstones — an
            //     inconsistent, partially-empty row. Consistency is preferred over per-element
            //     granularity whenever the operator explicitly opts into an override strategy.
            lazy val repairWritetimeOverrideMicros: Option[Long] =
              validationConfig.repairWritetimeStrategy match {
                case RepairWritetimeStrategy.Source =>
                  log.info(
                    "repairWritetimeStrategy=source: using original source writetime(s). " +
                      "Repair writes may be shadowed by newer delete tombstones on the target."
                  )
                  None
                case RepairWritetimeStrategy.Coordinator =>
                  val micros = System.currentTimeMillis() * 1000L
                  log.info(
                    s"repairWritetimeStrategy=coordinator: overriding writetime to $micros. " +
                      "Repair writes will beat most tombstones but may resurrect deleted rows."
                  )
                  Some(micros)
                case RepairWritetimeStrategy.Config =>
                  val micros = targetSettings.writeWritetimestampInuS.getOrElse(
                    sys.error(
                      "repairWritetimeStrategy=config requires target.writeWritetimestampInuS " +
                        "to be set in the configuration."
                    )
                  )
                  log.info(
                    s"repairWritetimeStrategy=config: overriding writetime to $micros " +
                      "(from target.writeWritetimestampInuS)."
                  )
                  Some(micros)
              }

            def overrideWritetimeColumn(
              rdd: RDD[Row],
              writeSchema: StructType,
              writetimeColumn: String
            ): RDD[Row] =
              repairWritetimeOverrideMicros match {
                case None => rdd
                case Some(micros) =>
                  val writetimeIdx = writeSchema.fieldIndex(writetimeColumn)
                  rdd.map { row =>
                    val values = row.toSeq.toArray
                    values(writetimeIdx) match {
                      case com.datastax.spark.connector.types.CassandraOption.Unset =>
                      case _ => values(writetimeIdx) = java.lang.Long.valueOf(micros)
                    }
                    Row(ArraySeq.unsafeWrapArray(values): _*)
                  }
              }

            def applyRepairStrategy(
              rdd: RDD[Row],
              writeSchema: StructType,
              timestampColumns: TimestampColumns
            ): RDD[Row] = overrideWritetimeColumn(rdd, writeSchema, timestampColumns.writeTime)

            if (perElementCollectionMetadata) {
              // Scalar/frozen base write + per-element collection-append passes, mirroring the
              // direct migration multi-pass write so missing rows regain element-level metadata.
              val (baseRdd, baseSchema, timestampColumns, appendWrites) =
                readers.Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, rawRepairDf)

              writers.Scylla.writeRowRDD(
                targetSettings,
                Nil,
                applyRepairStrategy(baseRdd, baseSchema, timestampColumns),
                baseSchema,
                Some(timestampColumns),
                None,
                sourceSettings
              )

              appendWrites.foreach { caw =>
                writers.Scylla.writeCollectionAppendRDD(
                  targetSettings,
                  Nil,
                  caw.columnName,
                  overrideWritetimeColumn(caw.rdd, caw.schema, "writetime"),
                  caw.schema,
                  None,
                  sourceSettings
                )
              }
            } else if (repairWithScalarMetadata) {
              val (repairRdd, writeRepairSchema, timestampColumns) =
                readers.Cassandra.explodeRowsFromPerColumnMeta(spark, rawRepairDf)

              writers.Scylla.writeRowRDD(
                targetSettings,
                Nil,
                applyRepairStrategy(repairRdd, writeRepairSchema, timestampColumns),
                writeRepairSchema,
                Some(timestampColumns),
                None,
                sourceSettings
              )
            } else {
              writers.Scylla.writeDataframe(
                targetSettings,
                Nil,
                rawRepairDf,
                None,
                None,
                sourceSettings
              )
            }
          }

          log.info(
            s"Finished copying missing rows to target: $missingSourceRowCount missing row(s) copied"
          )
        } finally missingRowsRdd.unpersist()
      }

      failures
    } finally
      if (validationConfig.copyMissingRows) cachedJoined.unpersist()
  }

}
