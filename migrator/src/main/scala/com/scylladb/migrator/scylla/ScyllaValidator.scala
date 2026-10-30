package com.scylladb.migrator.scylla

import com.datastax.spark.connector.{
  toSparkContextFunctions,
  CassandraRow,
  CassandraRowMetadata,
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
import scala.collection.mutable

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

  /** How the validator reads one endpoint's per-element collection metadata. */
  private sealed trait ElementMetadataRead

  /** Projected in the scan/join with the collection-wide form (Cassandra 5.0+). */
  private case object CollectionWideRead extends ElementMetadataRead

  /** Read with per-element point lookups after the join (ScyllaDB 2026.2+). */
  private final case class SubscriptRead(binder: readers.Cassandra.PkBinder)
      extends ElementMetadataRead

  /** Probe how `endpoint` exposes per-element collection metadata. `None` when it cannot be read:
    * the server supports neither form, or it only supports the subscript form and the primary key
    * (`pkColumns`, in lookup order) has a type that cannot be bound into a point lookup.
    */
  private def elementMetadataRead(
    endpoint: String,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    tableDef: TableDef,
    pkColumns: Seq[com.datastax.spark.connector.cql.ColumnDef]
  ): Option[ElementMetadataRead] =
    readers.Cassandra.probeMetadataReadStrategy(
      connector,
      keyspace,
      table,
      readers.Cassandra.nonFrozenCollectionColumns(tableDef)
    ) match {
      case Right(readers.Cassandra.ArrayMetadataRead) => Some(CollectionWideRead)
      case Right(readers.Cassandra.SubscriptMetadataRead) =>
        readers.Cassandra.pkBinder(pkColumns) match {
          case Right(binder) => Some(SubscriptRead(binder))
          case Left(reason) =>
            log.warn(
              s"Validation $endpoint exposes per-element collection WRITETIME()/TTL() only via " +
                s"per-element point lookups, which cannot bind its primary key: $reason"
            )
            None
        }
      case Left(_) =>
        log.warn(
          s"Validation $endpoint cannot read per-element collection WRITETIME()/TTL() " +
            "(requires Cassandra 5.0+ or ScyllaDB 2026.2+)."
        )
        None
    }

  /** A non-frozen collection column whose per-element metadata is read with point lookups.
    *
    * @param rowName
    *   its name in the validator's rows (renamed on both sides)
    * @param sourceName
    *   its name in the source table
    * @param targetName
    *   its name in the target table
    */
  private[scylla] final case class LookupColumn(
    rowName: String,
    sourceName: String,
    targetName: String,
    sorter: readers.Cassandra.ElementSorter[_]
  )

  /** One endpoint read with point lookups. `pk` pairs each primary-key column's name in the
    * validator's rows with its name in this endpoint's table, in `binder` order.
    */
  private final case class LookupSide(
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    pk: Seq[(String, String)],
    binder: readers.Cassandra.PkBinder,
    consistencyLevel: com.datastax.oss.driver.api.core.ConsistencyLevel,
    dropVanishedElements: Boolean
  )

  /** Add `<col>_ttl`/`<col>_writetime` per-element lists, read with point lookups, to the source
    * and/or target rows of `joined`, in the same shape the collection-wide form returns. A source
    * row that no longer exists is skipped; a target row that no longer exists becomes missing.
    */
  private def addElementMetadata(
    joined: RDD[(CassandraRow, Option[CassandraRow])],
    sourceSide: Option[LookupSide],
    targetSide: Option[LookupSide],
    columns: Seq[LookupColumn]
  ): RDD[(CassandraRow, Option[CassandraRow])] =
    joined.mapPartitions { pairs =>
      def reader(side: LookupSide): readers.Cassandra.SubscriptPointReader = {
        val session = side.connector.openSession()
        org.apache.spark.TaskContext.get().addTaskCompletionListener[Unit](_ => session.close())
        new readers.Cassandra.SubscriptPointReader(
          session,
          side.keyspace,
          side.table,
          side.pk.map(_._2),
          side.consistencyLevel
        )
      }
      val sourceReader = sourceSide.map(side => side -> reader(side))
      val targetReader = targetSide.map(side => side -> reader(side))
      val sourceColumns = columns.map(c => (c.rowName, c.sourceName, c.sorter))
      val targetColumns = columns.map(c => (c.rowName, c.targetName, c.sorter))

      pairs.flatMap { case (sourceRow, targetRow) =>
        val source = sourceReader match {
          case Some((side, r)) => withElementMetadata(sourceRow, side, r, sourceColumns)
          case None            => Some(sourceRow)
        }
        source.map { s =>
          val target = targetRow.flatMap { row =>
            targetReader match {
              case Some((side, r)) => withElementMetadata(row, side, r, targetColumns)
              case None            => Some(row)
            }
          }
          (s, target)
        }
      }
    }

  /** `row` with each column's per-element `<rowName>_ttl`/`<rowName>_writetime` lists inserted
    * right after it, in the column's element order. `None` when the row no longer exists.
    */
  private def withElementMetadata(
    row: CassandraRow,
    side: LookupSide,
    reader: readers.Cassandra.SubscriptPointReader,
    columns: Seq[(String, String, readers.Cassandra.ElementSorter[_])]
  ): Option[CassandraRow] = {
    val pkBinds = side.binder.bind(side.pk.map { case (rowName, _) => row.getRaw(rowName) })
    val added = mutable.Map.empty[String, (AnyRef, Seq[Integer], Seq[java.lang.Long])]
    val it = columns.iterator
    while (it.hasNext) {
      val (rowName, tableName, sorter) = it.next()
      row.getRaw(rowName) match {
        case null => added(rowName) = (null, null, null)
        case m: scala.collection.Map[_, _] =>
          val entries =
            sorter.sortEntries(m.asInstanceOf[scala.collection.Map[Any, Any]].toIndexedSeq)
          lookup(reader, tableName, entries.map(_._1), pkBinds, side.dropVanishedElements) match {
            case None => return None
            case Some((kept, ttls, wts)) =>
              val value =
                if (kept.length == entries.length) m
                else kept.map(entries).toMap
              added(rowName) = (value.asInstanceOf[AnyRef], ttls, wts)
          }
        case s: scala.collection.Iterable[_] =>
          val elements =
            sorter.sortElements(s.asInstanceOf[scala.collection.Iterable[Any]].toIndexedSeq)
          lookup(reader, tableName, elements, pkBinds, side.dropVanishedElements) match {
            case None => return None
            case Some((kept, ttls, wts)) =>
              val value =
                if (kept.length == elements.length) s
                else kept.map(elements).toSet
              added(rowName) = (value.asInstanceOf[AnyRef], ttls, wts)
          }
        case other =>
          sys.error(s"Unexpected value for non-frozen collection '$rowName': ${other.getClass}")
      }
    }
    val names = mutable.ArrayBuffer.empty[String]
    val values = mutable.ArrayBuffer.empty[AnyRef]
    row.metaData.columnNames.indices.foreach { i =>
      val name = row.metaData.columnNames(i)
      added.get(name) match {
        case Some((value, ttls, wts)) =>
          names ++= Seq(name, name + "_ttl", name + "_writetime")
          values ++= Seq(value, ttls, wts)
        case None =>
          names += name
          values += row.columnValues(i)
      }
    }
    Some(
      new CassandraRow(
        CassandraRowMetadata.fromColumnNames(names.toIndexedSeq),
        values.toIndexedSeq
      )
    )
  }

  /** Point-read the metadata of `keys`. Returns the indices of the kept elements with their
    * TTLs/WRITETIMEs: every element, or with `dropVanished` only those whose WRITETIME is still
    * present. `None` when the row no longer exists.
    */
  private def lookup(
    reader: readers.Cassandra.SubscriptPointReader,
    column: String,
    keys: IndexedSeq[Any],
    pkBinds: Seq[AnyRef],
    dropVanished: Boolean
  ): Option[(IndexedSeq[Int], Seq[Integer], Seq[java.lang.Long])] =
    if (keys.isEmpty) Some((IndexedSeq.empty, Nil, Nil))
    else
      reader.read(column, keys, pkBinds, withValues = false).map { case (_, wts, ttls) =>
        val kept =
          if (dropVanished) keys.indices.filter(i => wts(i) != null) else keys.indices
        (kept, kept.map(ttls), kept.map(wts))
      }

  /** `row` without the named columns (a no-op when `names` is empty). */
  private[scylla] def dropColumns(row: CassandraRow, names: Set[String]): CassandraRow =
    if (names.isEmpty) row
    else {
      val kept =
        row.metaData.columnNames.indices.filterNot(i => names.contains(row.metaData.columnNames(i)))
      new CassandraRow(
        CassandraRowMetadata.fromColumnNames(kept.map(row.metaData.columnNames)),
        kept.map(row.columnValues)
      )
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

    // How each endpoint exposes per-element collection metadata (same probe the migrate path uses):
    // Cassandra 5.0+ accepts the collection-wide `WRITETIME(col)`/`TTL(col)` form, projected in the
    // scan/join below; ScyllaDB 2026.2+ only the element-subscript form, read with per-element point
    // lookups after the join; older servers neither.
    //   - per-element metadata is COMPARED when both endpoints can read it; otherwise non-frozen
    //     collection columns are compared by VALUE only (scalar/frozen metadata is still compared).
    //   - the SOURCE also reads it for copyMissingRows, which only needs the source's metadata.
    val targetTableDef =
      if (!perElementCollectionMetadata) sourceTableDef
      else
        targetConnector.withSessionDo(
          Schema.tableFromCassandra(_, targetSettings.keyspace, targetSettings.table)
        )
    val (sourceRead, targetRead) =
      if (!perElementCollectionMetadata) (None, None)
      else
        (
          elementMetadataRead(
            "source",
            sourceConnector,
            sourceSettings.keyspace,
            sourceSettings.table,
            sourceTableDef,
            (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          ),
          elementMetadataRead(
            "target",
            targetConnector,
            targetSettings.keyspace,
            targetSettings.table,
            targetTableDef,
            (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
              .map(c => targetTableDef.columnByName(config.renamesMap(c.columnName)))
          )
        )
    val compareElementMetadata = sourceRead.isDefined && targetRead.isDefined
    val collectionsValueOnly = perElementCollectionMetadata && !compareElementMetadata

    if (collectionsValueOnly) {
      log.warn(
        "The validator compares non-frozen collection columns by VALUE only " +
          s"(${nonFrozenCollectionNames.mkString(", ")}); their per-element TTL/WRITETIME are NOT " +
          "validated. Scalar and frozen-collection metadata are still compared."
      )
      // copyMissingRows repair reconstructs per-element collection TTL/WRITETIME from the source
      // metadata, so it needs a source that exposes it.
      if (validationConfig.copyMissingRows && sourceRead.isEmpty)
        sys.error(
          "copyMissingRows is not supported for non-frozen collections when the source cannot read " +
            "per-element WRITETIME()/TTL() (requires Cassandra 5.0+, or ScyllaDB 2026.2+ with a " +
            "primary key of plain types): per-element collection metadata cannot be read for " +
            "repair. Re-run the migration itself (idempotent under USING TIMESTAMP) to converge " +
            "missing rows, or disable copyMissingRows to validate values only."
        )
    }

    val readSourceCollectionMetadata =
      sourceRead.isDefined && (compareElementMetadata || validationConfig.copyMissingRows)
    val readTargetCollectionMetadata = compareElementMetadata
    val sourceLookups: Option[readers.Cassandra.PkBinder] =
      sourceRead.collect { case SubscriptRead(binder) if readSourceCollectionMetadata => binder }
    val targetLookups: Option[readers.Cassandra.PkBinder] =
      targetRead.collect { case SubscriptRead(binder) if readTargetCollectionMetadata => binder }

    // Whether the scan/join projects per-element metadata for a given regular column. Non-frozen
    // collections project it only on an endpoint that reads it in the scan and when it is needed.
    def projectMetadataFor(columnName: String, projectCollectionMetadata: Boolean): Boolean =
      includePerColumnMetadata &&
        (!nonFrozenCollectionNames.contains(columnName) ||
          !perElementCollectionMetadata || projectCollectionMetadata)

    // Collection metadata the source reads only for repair; stripped before comparison so both
    // rows carry the same columns.
    val sourceOnlyColumns: Set[String] =
      if (readSourceCollectionMetadata && !readTargetCollectionMetadata)
        nonFrozenCollectionNames.flatMap { name =>
          val alias = config.renamesMap(name)
          Set(alias + "_ttl", alias + "_writetime")
        }
      else Set.empty

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = config.renamesMap(colDef.columnName)

          if (
            projectMetadataFor(
              colDef.columnName,
              readSourceCollectionMetadata && sourceRead.contains(CollectionWideRead)
            )
          )
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

          if (
            projectMetadataFor(
              colDef.columnName,
              readTargetCollectionMetadata && targetRead.contains(CollectionWideRead)
            )
          )
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

    // On subscript-only endpoints, add the per-element metadata the scan/join could not project.
    val withElementMetadata =
      if (sourceLookups.isEmpty && targetLookups.isEmpty) joined
      else {
        val pkColumns = sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns
        val sourcePk = pkColumns.map(c => (config.renamesMap(c.columnName), c.columnName))
        val targetPk =
          pkColumns.map(c => (config.renamesMap(c.columnName), config.renamesMap(c.columnName)))
        val lookupColumns = readers.Cassandra.nonFrozenCollectionColumns(sourceTableDef).map { c =>
          val sorter = readers.Cassandra
            .collectionElementOrdering(c.columnType)
            .getOrElse(sys.error(s"No element ordering for '${c.columnName}'"))
          LookupColumn(
            config.renamesMap(c.columnName),
            c.columnName,
            config.renamesMap(c.columnName),
            sorter
          )
        }
        val sourceSide = sourceLookups.map(binder =>
          LookupSide(
            sourceConnector,
            sourceSettings.keyspace,
            sourceSettings.table,
            sourcePk,
            binder,
            ConsistencyLevelUtils.parseConsistencyLevel(sourceSettings.consistencyLevel),
            dropVanishedElements = true
          )
        )
        val targetSide = targetLookups.map(binder =>
          LookupSide(
            targetConnector,
            targetSettings.keyspace,
            targetSettings.table,
            targetPk,
            binder,
            ConsistencyLevelUtils.parseConsistencyLevel(targetSettings.consistencyLevel),
            dropVanishedElements = false
          )
        )
        sourceSide.foreach(_ =>
          log.info("Reading source per-element collection metadata with point lookups.")
        )
        targetSide.foreach(_ =>
          log.info("Reading target per-element collection metadata with point lookups.")
        )
        addElementMetadata(joined, sourceSide, targetSide, lookupColumns)
      }

    val cachedJoined =
      if (validationConfig.copyMissingRows)
        withElementMetadata.persist(StorageLevel.MEMORY_AND_DISK)
      else withElementMetadata

    try {
      val failures = cachedJoined
        .flatMap { case (l, r) =>
          RowComparisonFailure.compareCassandraRows(
            dropColumns(l, sourceOnlyColumns),
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
              "converged here, since re-writing them could resurrect elements deleted on the " +
              "target. To converge present-but-incomplete rows, re-run the migration itself " +
              "(idempotent under USING TIMESTAMP; it also removes target elements older than every " +
              "source element) rather than relying on validation repair."
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
              // The explosion persists `rawRepairDf` so the base and append passes share one
              // read; release it once every pass has been written (or one has failed).
              try {
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
              } finally rawRepairDf.unpersist()
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
