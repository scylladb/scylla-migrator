package com.scylladb.migrator.scylla

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ CassandraConnector, Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ MigratorConfig, Rename, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers
import com.scylladb.migrator.readers.MySQL
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.{ CassandraSQLRow, DataTypeConverter }
import org.apache.spark.sql.{ Column, DataFrame, Row, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ BinaryType, StringType, StructType }
import org.apache.spark.storage.StorageLevel

import java.sql.DatabaseMetaData
import java.util.Locale
import scala.collection.mutable.ListBuffer
import scala.util.Using

object MySQLToScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla.MySQLToScyllaValidator")
  private[scylla] sealed trait ValidationCandidate
  private[scylla] case class FinalValidationFailure(failure: RowComparisonFailure)
      extends ValidationCandidate
  private[scylla] case class MatchedRowValidationCandidate(
    sourcePkValues: Vector[Any],
    sourceRepr: String,
    targetRepr: String,
    directDifferingFields: List[String],
    hashMismatch: Boolean
  ) extends ValidationCandidate
  private[scylla] case class HashRefinementFrames(
    source: DataFrame,
    target: DataFrame
  )

  private[scylla] def buildCaseInsensitiveRenameMap(
    renames: List[Rename]
  ): Map[String, String] =
    renames
      .groupBy(_.from.toLowerCase(Locale.ROOT))
      .view
      .map { case (sourceName, entries) =>
        val targets = entries.map(_.to).distinct
        if (targets.size > 1)
          sys.error(
            s"Renames contain conflicting case-insensitive mappings for source column '${sourceName}': " +
              s"${targets.mkString(", ")}"
          )
        sourceName -> targets.head
      }
      .toMap

  private[scylla] def escapeSparkColumnName(name: String): String =
    s"`${name.replace("`", "``")}`"

  private[scylla] def sparkColumn(name: String): Column =
    col(escapeSparkColumnName(name))

  private[scylla] def areDifferent(
    left: Option[Any],
    right: Option[Any],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  ): Boolean =
    (left, right) match {
      case (Some(l: Number), Some(r: Number)) =>
        areNumbersDifferent(l, r, floatingPointTolerance)
      case _ =>
        RowComparisonFailure.areDifferent(
          left,
          right,
          timestampMsTolerance,
          floatingPointTolerance
        )
    }

  private sealed trait FloatingPointSpecial
  private case object NaNValue extends FloatingPointSpecial
  private case class InfinityValue(sign: Int) extends FloatingPointSpecial

  private def areNumbersDifferent(left: Number, right: Number, tolerance: Double): Boolean =
    floatingPointSpecial(left)
      .orElse(floatingPointSpecial(right))
      .map { _ =>
        (floatingPointSpecial(left), floatingPointSpecial(right)) match {
          case (Some(NaNValue), Some(NaNValue)) => false
          case (Some(InfinityValue(leftSign)), Some(InfinityValue(rightSign))) =>
            leftSign != rightSign
          case _ => true
        }
      }
      .getOrElse {
        (normalizedIntegralValue(left), normalizedIntegralValue(right)) match {
          case (Some(l), Some(r)) => l != r
          case _ =>
            (normalizedDecimalValue(left), normalizedDecimalValue(right)) match {
              case (Some(l), Some(r)) => areNumericalValuesDifferent(l, r, tolerance)
              case _                  => left != right
            }
        }
      }

  private def floatingPointSpecial(value: Number): Option[FloatingPointSpecial] =
    value match {
      case d: java.lang.Double if d.isNaN      => Some(NaNValue)
      case d: java.lang.Double if d.isInfinite => Some(InfinityValue(Math.signum(d).toInt))
      case f: java.lang.Float if f.isNaN       => Some(NaNValue)
      case f: java.lang.Float if f.isInfinite  => Some(InfinityValue(Math.signum(f).toInt))
      case _                                   => None
    }

  private def normalizedIntegralValue(value: Number): Option[BigInt] =
    value match {
      case b: java.lang.Byte        => Some(BigInt(b.longValue))
      case s: java.lang.Short       => Some(BigInt(s.longValue))
      case i: java.lang.Integer     => Some(BigInt(i.longValue))
      case l: java.lang.Long        => Some(BigInt(l.longValue))
      case bi: java.math.BigInteger => Some(BigInt(bi))
      case bi: BigInt               => Some(bi)
      case bd: java.math.BigDecimal =>
        val stripped = bd.stripTrailingZeros
        if (stripped.scale <= 0) Some(BigInt(stripped.toBigIntegerExact)) else None
      case bd: BigDecimal =>
        val stripped = bd.bigDecimal.stripTrailingZeros
        if (stripped.scale <= 0) Some(BigInt(stripped.toBigIntegerExact)) else None
      case _ => None
    }

  private def normalizedDecimalValue(value: Number): Option[BigDecimal] =
    value match {
      case d: java.lang.Double if d.isNaN || d.isInfinite => None
      case f: java.lang.Float if f.isNaN || f.isInfinite  => None
      case b: java.lang.Byte                              => Some(BigDecimal(BigInt(b.longValue)))
      case s: java.lang.Short                             => Some(BigDecimal(BigInt(s.longValue)))
      case i: java.lang.Integer                           => Some(BigDecimal(BigInt(i.longValue)))
      case l: java.lang.Long                              => Some(BigDecimal(BigInt(l.longValue)))
      case bi: java.math.BigInteger                       => Some(BigDecimal(BigInt(bi)))
      case bi: BigInt                                     => Some(BigDecimal(bi))
      case bd: java.math.BigDecimal                       => Some(BigDecimal(bd))
      case bd: BigDecimal                                 => Some(bd)
      case f: java.lang.Float                             => Some(BigDecimal.decimal(f.doubleValue))
      case d: java.lang.Double                            => Some(BigDecimal.decimal(d.doubleValue))
      case _                                              => None
    }

  private def areNumericalValuesDifferent(
    x: BigDecimal,
    y: BigDecimal,
    tolerance: BigDecimal
  ): Boolean =
    (x - y).abs > tolerance

  private[scylla] def differingFieldNamesForRow(
    joinedRow: Row,
    fieldIndices: Seq[(String, Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  ): List[String] =
    fieldIndices.flatMap { case (colName, srcIdx, tgtIdx) =>
      val srcVal = if (joinedRow.isNullAt(srcIdx)) None else Some(joinedRow.get(srcIdx))
      val tgtVal = if (joinedRow.isNullAt(tgtIdx)) None else Some(joinedRow.get(tgtIdx))
      if (
        areDifferent(
          srcVal,
          tgtVal,
          timestampMsTolerance,
          floatingPointTolerance
        )
      )
        Some(colName)
      else
        None
    }.toList

  private[scylla] def compareFieldsBySchemaForRow(
    joinedRow: Row,
    directFieldIndices: Seq[(String, Int, Int)],
    hashBackedFieldIndices: Seq[(String, Int, Int)],
    contentHashFieldIndices: Option[(Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  ): List[String] = {
    val directDifferences =
      differingFieldNamesForRow(
        joinedRow,
        directFieldIndices,
        timestampMsTolerance,
        floatingPointTolerance
      )

    val hashBackedDifferences = contentHashFieldIndices match {
      case Some((srcHashIdx, tgtHashIdx)) =>
        val srcHash = if (joinedRow.isNullAt(srcHashIdx)) None else Some(joinedRow.get(srcHashIdx))
        val tgtHash = if (joinedRow.isNullAt(tgtHashIdx)) None else Some(joinedRow.get(tgtHashIdx))
        if (
          areDifferent(
            srcHash,
            tgtHash,
            timestampMsTolerance,
            floatingPointTolerance
          )
        )
          differingFieldNamesForRow(
            joinedRow,
            hashBackedFieldIndices,
            timestampMsTolerance,
            floatingPointTolerance
          )
        else
          Nil
      case None =>
        differingFieldNamesForRow(
          joinedRow,
          hashBackedFieldIndices,
          timestampMsTolerance,
          floatingPointTolerance
        )
    }

    directDifferences ++ hashBackedDifferences
  }

  private[scylla] def hasContentHashMismatch(
    joinedRow: Row,
    contentHashFieldIndices: Option[(Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  ): Boolean =
    contentHashFieldIndices.exists { case (srcHashIdx, tgtHashIdx) =>
      val srcHash = if (joinedRow.isNullAt(srcHashIdx)) None else Some(joinedRow.get(srcHashIdx))
      val tgtHash = if (joinedRow.isNullAt(tgtHashIdx)) None else Some(joinedRow.get(tgtHashIdx))
      areDifferent(
        srcHash,
        tgtHash,
        timestampMsTolerance,
        floatingPointTolerance
      )
    }

  private[scylla] def materializeValidationCandidate(
    candidate: ValidationCandidate,
    hashBackedColumns: Seq[String],
    refinedHashDifferences: Map[Vector[Any], List[String]]
  ): Option[RowComparisonFailure] =
    candidate match {
      case FinalValidationFailure(failure) => Some(failure)
      case matched: MatchedRowValidationCandidate =>
        val hashDifferences =
          if (!matched.hashMismatch) Nil
          else
            refinedHashDifferences.getOrElse(
              normalizePrimaryKeyValues(matched.sourcePkValues),
              List(
                s"hashColumns(${hashBackedColumns.mkString(", ")}) " +
                  "(content hash mismatch; values fetched by PK could not be resolved)"
              )
            )
        val differingFields = matched.directDifferingFields ++ hashDifferences
        if (differingFields.isEmpty) None
        else
          Some(
            RowComparisonFailure(
              matched.sourceRepr,
              Some(matched.targetRepr),
              List(RowComparisonFailure.Item.DifferingFieldValues(differingFields))
            )
          )
    }

  private[scylla] def collectFailureSample(
    candidateFailuresRdd: RDD[ValidationCandidate],
    rawSourceDF: DataFrame,
    rawTargetDF: DataFrame,
    primaryKey: Seq[String],
    hashBackedColumns: Seq[String],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double,
    failuresToFetch: Int
  )(implicit spark: SparkSession): List[RowComparisonFailure] =
    if (failuresToFetch <= 0) Nil
    else {
      val failures = ListBuffer.empty[RowComparisonFailure]
      val candidateIterator = candidateFailuresRdd.toLocalIterator
      val refinementFrames =
        Option
          .when(hashBackedColumns.nonEmpty)(
            createHashRefinementFrames(rawSourceDF, rawTargetDF, primaryKey, hashBackedColumns)
          )

      try
        while (candidateIterator.hasNext && failures.size < failuresToFetch) {
          val batchSize = math.max(failuresToFetch - failures.size, 1)
          val batch = candidateIterator.take(batchSize).toList
          val hashMismatchCandidates =
            if (hashBackedColumns.nonEmpty)
              batch.collect {
                case candidate: MatchedRowValidationCandidate if candidate.hashMismatch =>
                  candidate
              }
            else Nil

          val refinedHashDifferences =
            if (hashMismatchCandidates.nonEmpty)
              resolveHashBackedDifferences(
                refinementFrames.getOrElse(
                  throw new IllegalStateException("Missing hash refinement frames")
                ),
                primaryKey,
                hashMismatchCandidates.map(_.sourcePkValues),
                hashBackedColumns,
                timestampMsTolerance,
                floatingPointTolerance
              )
            else Map.empty[Vector[Any], List[String]]

          batch.iterator
            .flatMap(
              materializeValidationCandidate(_, hashBackedColumns, refinedHashDifferences)
            )
            .take(failuresToFetch - failures.size)
            .foreach(failures += _)
        }
      finally
        refinementFrames.foreach { frames =>
          frames.source.unpersist(blocking = false)
          frames.target.unpersist(blocking = false)
        }

      failures.toList
    }

  private[scylla] def resolveFieldName(fields: Array[String], name: String): String =
    fields
      .find(_.equalsIgnoreCase(name))
      .getOrElse(
        sys.error(
          s"Column '$name' not found in schema. Available columns: ${fields.mkString(", ")}. " +
            "This may indicate a missing rename entry or schema mismatch."
        )
      )

  private[scylla] def differingFieldsBetweenRows(
    sourceRow: Row,
    sourceFields: Array[String],
    targetRow: Row,
    targetFields: Array[String],
    columns: Seq[String],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  ): List[String] =
    columns.flatMap { colName =>
      val srcIdx = sourceFields.indexOf(resolveFieldName(sourceFields, colName))
      val tgtIdx = targetFields.indexOf(resolveFieldName(targetFields, colName))
      val srcVal = if (sourceRow.isNullAt(srcIdx)) None else Some(sourceRow.get(srcIdx))
      val tgtVal = if (targetRow.isNullAt(tgtIdx)) None else Some(targetRow.get(tgtIdx))
      if (
        areDifferent(
          srcVal,
          tgtVal,
          timestampMsTolerance,
          floatingPointTolerance
        )
      )
        Some(s"$colName (source=${truncateValue(srcVal)}, target=${truncateValue(tgtVal)})")
      else None
    }.toList

  private[scylla] def validateTargetPrimaryKey(
    configuredPrimaryKey: Seq[String],
    actualTargetPrimaryKey: Seq[String]
  ): Unit = {
    val configuredLower = configuredPrimaryKey.map(_.toLowerCase(Locale.ROOT))
    val actualLower = actualTargetPrimaryKey.map(_.toLowerCase(Locale.ROOT))
    if (actualLower != configuredLower)
      sys.error(
        s"Configured primaryKey (after renames) does not match target table's actual PK. " +
          s"Configured: ${configuredPrimaryKey.mkString(", ")}. " +
          s"Actual target PK: ${actualTargetPrimaryKey.mkString(", ")}. " +
          "Validation cannot safely proceed with a mismatched join key."
      )
  }

  private[scylla] def validateSourcePrimaryKey(
    configuredPrimaryKey: Seq[String],
    actualSourcePrimaryKey: Seq[String]
  ): Unit = {
    val configuredLower = configuredPrimaryKey.map(_.toLowerCase(Locale.ROOT))
    val actualLower = actualSourcePrimaryKey.map(_.toLowerCase(Locale.ROOT))
    if (actualLower != configuredLower)
      sys.error(
        s"Configured primaryKey does not match the MySQL table's actual PK. " +
          s"Configured: ${configuredPrimaryKey.mkString(", ")}. " +
          s"Actual source PK: ${actualSourcePrimaryKey.mkString(", ")}. " +
          "Validation cannot safely proceed with a mismatched join key."
      )
  }

  private[scylla] def sourcePrimaryKeyFromMetadata(
    metaData: DatabaseMetaData,
    catalog: String,
    tableName: String
  ): Seq[String] =
    Using.resource(
      // getPrimaryKeys expects the literal table name, not a metadata pattern. Escaping `_`/`%`
      // here breaks primary-key discovery on MySQL Connector/J, including for ordinary table
      // names such as `users_numeric`.
      metaData.getPrimaryKeys(catalog, null, tableName)
    ) { resultSet =>
      Iterator
        .continually(resultSet.next())
        .takeWhile(identity)
        .map(_ => resultSet.getShort("KEY_SEQ").toInt -> resultSet.getString("COLUMN_NAME"))
        .toList
        .sortBy(_._1)
        .map(_._2)
    }

  private[scylla] def sourcePrimaryKeyFromMetadata(
    sourceSettings: SourceSettings.MySQL
  ): Seq[String] =
    readers.MySQL.withJdbcConnection(sourceSettings) { connection =>
      val catalog = Option(connection.getCatalog).getOrElse(sourceSettings.database)
      sourcePrimaryKeyFromMetadata(connection.getMetaData, catalog, sourceSettings.table)
    }

  private[scylla] def validateSourceColumnsPresentInTarget(
    sourceColumns: Seq[String],
    targetColumns: Seq[String]
  ): Unit = {
    val targetColumnsLower = targetColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    val missingInTarget =
      sourceColumns.filterNot(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    if (missingInTarget.nonEmpty)
      sys.error(
        s"Source columns not found in target after renames: ${missingInTarget.mkString(", ")}. " +
          "Validation would silently skip these columns, so the run has been aborted. " +
          "If this is unexpected, check your target schema or 'renames' configuration."
      )
  }

  private[scylla] def targetOnlyColumns(
    sourceColumns: Seq[String],
    targetColumns: Seq[String]
  ): Seq[String] = {
    val sourceColumnsLower = sourceColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    targetColumns.filterNot(c => sourceColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
  }

  private[scylla] def normalizePrimaryKeyComponent(value: Any): Any = value match {
    case bytes: Array[Byte] => bytes.toIndexedSeq
    case other              => other
  }

  private[scylla] def normalizePrimaryKeyValues(values: Seq[Any]): Vector[Any] =
    values.iterator.map(normalizePrimaryKeyComponent).toVector

  private[scylla] def selectColumnsForHashRefinement(
    df: DataFrame,
    primaryKey: Seq[String],
    hashBackedColumns: Seq[String]
  ): DataFrame = {
    val requiredColumns =
      (primaryKey ++ hashBackedColumns).distinct.map(resolveFieldName(df.schema.fieldNames, _))
    df.select(requiredColumns.map(sparkColumn): _*)
  }

  private[scylla] def createHashRefinementFrames(
    rawSourceDF: DataFrame,
    rawTargetDF: DataFrame,
    primaryKey: Seq[String],
    hashBackedColumns: Seq[String]
  ): HashRefinementFrames =
    HashRefinementFrames(
      selectColumnsForHashRefinement(rawSourceDF, primaryKey, hashBackedColumns)
        .persist(StorageLevel.MEMORY_AND_DISK),
      selectColumnsForHashRefinement(rawTargetDF, primaryKey, hashBackedColumns)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

  private[scylla] def selectAndAliasColumns(
    df: DataFrame,
    requestedColumns: Seq[String]
  ): DataFrame = {
    val schemaFields = df.schema.fieldNames
    df.select(
      requestedColumns.toIndexedSeq.map { columnName =>
        sparkColumn(resolveFieldName(schemaFields, columnName)).as(columnName)
      }: _*
    )
  }

  private[scylla] def validateHashColumnsPresentOnBothSides(
    requestedHashColumns: Seq[String],
    sourceColumns: Seq[String],
    targetColumns: Seq[String]
  ): Unit = {
    val sourceColumnsLower = sourceColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    val targetColumnsLower = targetColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    val missingInSource =
      requestedHashColumns.filterNot(c => sourceColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    val missingInTarget =
      requestedHashColumns.filterNot(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))

    if (missingInSource.nonEmpty || missingInTarget.nonEmpty) {
      val details = List(
        if (missingInSource.nonEmpty)
          Some(s"missing in source: ${missingInSource.mkString(", ")}")
        else None,
        if (missingInTarget.nonEmpty)
          Some(s"missing in target: ${missingInTarget.mkString(", ")}")
        else None
      ).flatten.mkString("; ")

      sys.error(
        s"Configured hashColumns must exist on both source and target after renames, but found $details"
      )
    }
  }

  private def resolveTargetColumns(
    targetTableDef: TableDef,
    columns: Seq[String]
  ): Seq[String] = {
    val targetFields = targetTableDef.columns.map(_.columnName).toArray
    columns.map(resolveFieldName(targetFields, _))
  }

  private def buildTargetSchema(
    targetTableDef: TableDef,
    selectedColumns: Seq[String]
  ): StructType =
    StructType(
      selectedColumns.map { columnName =>
        DataTypeConverter.toStructField(targetTableDef.columnByName(columnName))
      }
    )

  private def targetReadConf(
    spark: SparkSession,
    targetSettings: TargetSettings.Scylla
  ): ReadConf = {
    val consistencyLevel =
      com.scylladb.migrator.ConsistencyLevelUtils.parseConsistencyLevel(
        targetSettings.consistencyLevel
      )
    log.info(
      s"Using consistencyLevel [${consistencyLevel}] for VALIDATOR TARGET based on target config [${targetSettings.consistencyLevel}]"
    )
    ReadConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(consistencyLevel = consistencyLevel)
  }

  private[scylla] def liveWriteWarning(
    sourceSettings: SourceSettings.MySQL,
    targetSettings: TargetSettings.Scylla
  ): String =
    s"MySQL-to-ScyllaDB validation is not point-in-time safe while either side is receiving " +
      s"writes. The validator may scan ${sourceSettings.database}.${sourceSettings.table} and " +
      s"${targetSettings.keyspace}.${targetSettings.table} using multiple statements over time, " +
      "which can produce false mismatches or miss real ones. Run validation only after source " +
      "and target writes have quiesced."

  private def readTargetRows(
    spark: SparkSession,
    targetConnector: CassandraConnector,
    targetTableDef: TableDef,
    targetSettings: TargetSettings.Scylla,
    selectedColumns: Seq[String]
  ): DataFrame = {
    val resolvedSelectedColumns = resolveTargetColumns(targetTableDef, selectedColumns)
    val schema = buildTargetSchema(targetTableDef, resolvedSelectedColumns)
    val selectColumns = resolvedSelectedColumns.map(ColumnName(_))
    val rows: RDD[Row] = spark.sparkContext
      .cassandraTable[CassandraSQLRow](targetSettings.keyspace, targetSettings.table)
      .withConnector(targetConnector)
      .withReadConf(targetReadConf(spark, targetSettings))
      .select(selectColumns: _*)
      .map(row => Row.fromSeq(row.toSeq.map(readers.Cassandra.convertValue)))
    spark.createDataFrame(rows, schema)
  }

  private def lookupTargetRowsForSourceKeys(
    spark: SparkSession,
    sourceDF: DataFrame,
    targetConnector: CassandraConnector,
    targetTableDef: TableDef,
    targetSettings: TargetSettings.Scylla,
    primaryKeyColumns: Seq[String],
    selectedColumns: Seq[String]
  ): DataFrame = {
    val sourceSchemaFields = sourceDF.schema.fieldNames
    val resolvedSourcePK = primaryKeyColumns.map(resolveFieldName(sourceSchemaFields, _))
    val resolvedTargetPK = resolveTargetColumns(targetTableDef, primaryKeyColumns)
    val resolvedSelectedColumns = resolveTargetColumns(targetTableDef, selectedColumns)
    val schema = buildTargetSchema(targetTableDef, resolvedSelectedColumns)
    val targetRows = sourceDF
      .select(resolvedSourcePK.map(sparkColumn): _*)
      .distinct()
      .rdd
      .leftJoinWithCassandraTable[CassandraSQLRow](
        targetSettings.keyspace,
        targetSettings.table,
        SomeColumns(resolvedSelectedColumns.map(ColumnName(_)): _*),
        SomeColumns(resolvedTargetPK.map(ColumnName(_)): _*),
        targetReadConf(spark, targetSettings)
      )
      .withConnector(targetConnector)
      .flatMap(_._2)
      .map(row => Row.fromSeq(row.toSeq.map(readers.Cassandra.convertValue)))
    spark.createDataFrame(targetRows, schema)
  }

  private[scylla] def collectExtraTargetFailureSample(
    sourceKeys: DataFrame,
    targetKeys: DataFrame,
    primaryKeyColumns: Seq[String],
    failuresToFetch: Int
  ): List[RowComparisonFailure] =
    if (failuresToFetch <= 0) Nil
    else
      targetKeys
        .join(sourceKeys.distinct(), primaryKeyColumns, "left_anti")
        .rdd
        .map { row =>
          val targetRepr =
            primaryKeyColumns
              .map(pk => s"$pk=${row.getAs[Any](pk)}")
              .mkString(", ")
          RowComparisonFailure(
            targetRepr,
            None,
            List(RowComparisonFailure.Item.ExtraTargetRow)
          )
        }
        .take(failuresToFetch)
        .toList

  private[scylla] def collectExtraTargetFailureSample(
    sourceDF: DataFrame,
    targetConnector: CassandraConnector,
    targetTableDef: TableDef,
    targetSettings: TargetSettings.Scylla,
    primaryKeyColumns: Seq[String],
    failuresToFetch: Int
  )(implicit spark: SparkSession): List[RowComparisonFailure] =
    if (failuresToFetch <= 0) Nil
    else {
      val sourceKeys = selectAndAliasColumns(sourceDF, primaryKeyColumns)
      val targetKeys =
        selectAndAliasColumns(
          readTargetRows(spark, targetConnector, targetTableDef, targetSettings, primaryKeyColumns),
          primaryKeyColumns
        )

      collectExtraTargetFailureSample(sourceKeys, targetKeys, primaryKeyColumns, failuresToFetch)
    }

  def runValidation(
    sourceSettings: SourceSettings.MySQL,
    targetSettings: TargetSettings.Scylla,
    config: MigratorConfig
  )(implicit spark: SparkSession): List[RowComparisonFailure] = {

    val validationConfig =
      config.validation.getOrElse(
        sys.error("Missing required property 'validation' in the configuration file.")
      )

    val primaryKey = sourceSettings.primaryKey.getOrElse(
      sys.error(
        "Missing required property 'primaryKey' in MySQL source configuration. " +
          "The validator needs to know which columns form the primary key for joining rows."
      )
    )

    if (primaryKey.isEmpty)
      sys.error("'primaryKey' must contain at least one column name.")

    val hashColumns = validationConfig.hashColumns.filter(_.nonEmpty)
    hashColumns.foreach(cols =>
      require(cols.forall(_.trim.nonEmpty), "hashColumns must not contain empty or blank strings")
    )

    val renamesByLower = buildCaseInsensitiveRenameMap(config.getRenamesOrNil)
    def resolveRename(column: String): String =
      renamesByLower.getOrElse(column.toLowerCase(Locale.ROOT), column)

    val renamedPK = primaryKey.map(resolveRename)
    val renamedPKLower = renamedPK.map(_.toLowerCase(Locale.ROOT)).toSet

    if (config.getRenamesOrNil.nonEmpty) {
      val unmappedPK =
        primaryKey.filterNot(pk => renamesByLower.contains(pk.toLowerCase(Locale.ROOT)))
      if (unmappedPK.nonEmpty)
        log.warn(
          s"PK columns with no explicit rename (using identity): ${unmappedPK.mkString(", ")}"
        )
    }

    require(
      renamedPK.distinct.size == renamedPK.size,
      s"Renames must not map multiple primary key columns to the same target name. Got: ${renamedPK.mkString(", ")}"
    )

    // Validate that hashColumns do not overlap with primaryKey columns.
    // hashColumns are documented as source-side names, so check them against source PK names.
    // After applying renames, also check the renamed hash columns against the target PK names.
    hashColumns.foreach { cols =>
      val pkSourceSet = primaryKey.map(_.toLowerCase(Locale.ROOT)).toSet
      val pkTargetSet = renamedPK.map(_.toLowerCase(Locale.ROOT)).toSet
      val sourceOverlap = cols.filter(c => pkSourceSet.contains(c.toLowerCase(Locale.ROOT)))
      val renamedHashCols = cols.map(resolveRename)
      val targetOverlap =
        renamedHashCols.filter(c => pkTargetSet.contains(c.toLowerCase(Locale.ROOT)))
      val overlap = (sourceOverlap ++ targetOverlap).distinct
      if (overlap.nonEmpty)
        sys.error(
          s"hashColumns must not include primary key columns, but found overlap: ${overlap.mkString(", ")}. " +
            "Primary key columns are always selected directly and must not be hashed."
        )
    }

    log.info("Starting MySQL-to-ScyllaDB validation")
    log.info(
      s"Source: MySQL ${sourceSettings.database}.${sourceSettings.table} " +
        s"at ${sourceSettings.host}:${sourceSettings.port}"
    )
    log.info(
      s"Target: ScyllaDB ${targetSettings.keyspace}.${targetSettings.table} " +
        s"at ${targetSettings.host}:${targetSettings.port}"
    )
    log.info(s"Primary key columns: ${primaryKey.mkString(", ")}")
    log.warn(liveWriteWarning(sourceSettings, targetSettings))
    hashColumns.foreach(cols =>
      log.warn(
        s"Hash-based comparison enabled for columns: ${cols.mkString(", ")}. " +
          "This reduces Spark-side join/shuffle volume, but the validator still reads the " +
          "original MySQL and ScyllaDB payload columns before hashing."
      )
    )

    val actualSourcePK = sourcePrimaryKeyFromMetadata(sourceSettings)
    if (actualSourcePK.isEmpty)
      sys.error(
        s"MySQL table ${sourceSettings.database}.${sourceSettings.table} does not expose a primary key via JDBC metadata. " +
          "Validation requires the real source primary key."
      )
    validateSourcePrimaryKey(primaryKey, actualSourcePK)

    // Always read all columns from MySQL; hash is computed in Spark (not MySQL)
    val rawSourceDF = {
      val df = readers.MySQL.readDataframe(spark, sourceSettings).dataFrame
      // Validate that all PK columns exist in the MySQL table before proceeding.
      // This gives a clear error message instead of a confusing "src_xxx not found" later.
      val dfColsLower = df.columns.map(_.toLowerCase(Locale.ROOT)).toSet
      val missingPK = primaryKey.filterNot(pk => dfColsLower.contains(pk.toLowerCase(Locale.ROOT)))
      if (missingPK.nonEmpty)
        sys.error(
          s"primaryKey columns not found in MySQL table: ${missingPK.mkString(", ")}. " +
            s"Available columns: ${df.columns.mkString(", ")}"
        )
      if (primaryKey.map(_.toLowerCase(Locale.ROOT)).distinct.size != primaryKey.size)
        sys.error(s"primaryKey contains duplicate columns: ${primaryKey.mkString(", ")}")
      val renamed =
        df.select(df.columns.toIndexedSeq.map(c => sparkColumn(c).as(resolveRename(c))): _*)
      val renamedCols = renamed.columns.map(_.toLowerCase(Locale.ROOT))
      val duplicates = renamedCols.diff(renamedCols.distinct)
      if (duplicates.nonEmpty)
        sys.error(
          s"Column rename collision: multiple source columns map to the same target name(s): " +
            s"${duplicates.distinct.mkString(", ")}. " +
            "Check your 'renames' configuration for conflicting mappings."
        )
      renamed
    }

    log.info(s"Connecting to ScyllaDB target at ${targetSettings.host}:${targetSettings.port}")

    // Verify that the configured primaryKey (after renames) matches the target table's actual PK.
    // This prevents silent incorrect validation results when the PK is mis-specified.
    val targetConnector =
      Connectors.targetConnector(spark.sparkContext.getConf, targetSettings)
    val targetTableDef = targetConnector.withSessionDo(
      Schema.tableFromCassandra(_, targetSettings.keyspace, targetSettings.table)
    )
    val actualTargetPK =
      (targetTableDef.partitionKey ++ targetTableDef.clusteringColumns)
        .map(_.columnName.toLowerCase(Locale.ROOT))
    validateTargetPrimaryKey(renamedPK, actualTargetPK)
    val targetColumnNames = targetTableDef.columns.map(_.columnName)
    val selectedTargetColumns = rawSourceDF.columns.toSeq

    // Fail when renamed source columns are missing in the target. Continuing would silently drop
    // those columns from comparison and could produce a false-clean validation result.
    validateSourceColumnsPresentInTarget(rawSourceDF.columns.toSeq, targetColumnNames)
    val missingInSource = targetOnlyColumns(rawSourceDF.columns.toSeq, targetColumnNames)
    if (missingInSource.nonEmpty)
      log.info(
        s"Target columns not present in (renamed) source: ${missingInSource.mkString(", ")}"
      )

    if (sourceSettings.where.isDefined)
      log.warn(
        "Source 'where' filter is configured. Disabling extra-target-row detection and " +
          "probing only target rows matching the filtered source primary keys."
      )
    val rawTargetDF = lookupTargetRowsForSourceKeys(
      spark,
      rawSourceDF,
      targetConnector,
      targetTableDef,
      targetSettings,
      renamedPK,
      selectedTargetColumns
    )

    val (sourceDF, targetDF, directComparableColumns, hashBackedColumns) = hashColumns match {
      case Some(cols) =>
        val renamedCols = cols.map(resolveRename)
        validateHashColumnsPresentOnBothSides(
          requestedHashColumns = renamedCols,
          sourceColumns        = rawSourceDF.columns.toSeq,
          targetColumns        = targetColumnNames
        )

        val hashedSource =
          addContentHash(rawSourceDF, renamedCols, renamedPK, dropHashedColumns = true)
        val hashedTarget =
          addContentHash(rawTargetDF, renamedCols, renamedPK, dropHashedColumns = true)
        val hashBackedColumnsLower = renamedCols.map(_.toLowerCase(Locale.ROOT)).toSet
        val directCols = hashedSource.columns
          .filter(c => !renamedPKLower.contains(c.toLowerCase(Locale.ROOT)))
          .filter(_ != MySQL.ContentHashColumn)
          .filterNot(c => hashBackedColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
        (hashedSource, hashedTarget, directCols, renamedCols)

      case None =>
        val nonPKCols =
          rawSourceDF.columns.filter(c => !renamedPKLower.contains(c.toLowerCase(Locale.ROOT)))
        (rawSourceDF, rawTargetDF, nonPKCols, Nil)
    }

    val targetColumnsLower = targetDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val droppedColumns =
      (directComparableColumns ++ (if (hashColumns.isDefined) Seq(MySQL.ContentHashColumn)
                                   else Nil))
        .filterNot(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    if (droppedColumns.nonEmpty)
      sys.error(
        s"Comparable columns unexpectedly disappeared from the target schema: ${droppedColumns.mkString(", ")}. " +
          "Validation cannot continue safely."
      )
    val finalDirectComparableColumns =
      directComparableColumns.filter(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    val finalHashBackedColumns = hashBackedColumns
    log.info(
      s"Comparable columns: ${(finalDirectComparableColumns ++ finalHashBackedColumns).mkString(", ")}"
    )

    val sourcePrefixed = prefixColumns(sourceDF, "src_")
    val targetPrefixed = prefixColumns(targetDF, "tgt_")

    // Resolve PK column names against the actual DataFrame schemas using case-insensitive
    // lookup. This is necessary because ScyllaDB normalizes unquoted column names to lowercase,
    // so a MySQL column "UserId" may appear as "userid" in the target DataFrame. Spark's
    // DataFrame.apply() resolves columns case-insensitively, but Row.fieldIndex() is
    // case-sensitive and would throw IllegalArgumentException on a mismatch.
    val srcSchemaFields = sourcePrefixed.schema.fieldNames
    val tgtSchemaFields = targetPrefixed.schema.fieldNames

    val joinCondition = renamedPK
      .map { pk =>
        val srcCol = resolveFieldName(srcSchemaFields, s"src_$pk")
        val tgtCol = resolveFieldName(tgtSchemaFields, s"tgt_$pk")
        sparkColumn(srcCol) === sparkColumn(tgtCol)
      }
      .reduce(_ && _)

    val joined = sourcePrefixed.join(targetPrefixed, joinCondition, "left_outer")

    val joinedSchemaFields = joined.schema.fieldNames
    val floatTol = validationConfig.floatingPointTolerance
    val tsTol = validationConfig.timestampMsTolerance

    // Pre-compute field indices to avoid repeated case-insensitive lookups on every row.
    // For wide tables (100+ columns) × millions of rows, this provides significant speedup.
    val directFieldIndices = finalDirectComparableColumns.toIndexedSeq.map { colName =>
      val srcFieldName = resolveFieldName(joinedSchemaFields, s"src_$colName")
      val tgtFieldName = resolveFieldName(joinedSchemaFields, s"tgt_$colName")
      (colName, joinedSchemaFields.indexOf(srcFieldName), joinedSchemaFields.indexOf(tgtFieldName))
    }
    val contentHashFieldIndices =
      if (hashColumns.isDefined) {
        val srcFieldName = resolveFieldName(joinedSchemaFields, s"src_${MySQL.ContentHashColumn}")
        val tgtFieldName = resolveFieldName(joinedSchemaFields, s"tgt_${MySQL.ContentHashColumn}")
        Some((joinedSchemaFields.indexOf(srcFieldName), joinedSchemaFields.indexOf(tgtFieldName)))
      } else None
    val fieldIndexByName =
      directFieldIndices.map { case (name, srcIdx, tgtIdx) =>
        name -> (srcIdx, tgtIdx)
      }.toMap

    // Pre-compute PK field indices so that the RDD closure uses index-based access
    // instead of calling resolveField (linear scan) on every row.
    val srcPKIndices = renamedPK.map { pk =>
      val fieldName = resolveFieldName(joinedSchemaFields, s"src_$pk")
      (pk, joinedSchemaFields.indexOf(fieldName))
    }.toArray
    val tgtPKIndices = renamedPK.map { pk =>
      val fieldName = resolveFieldName(joinedSchemaFields, s"tgt_$pk")
      (pk, joinedSchemaFields.indexOf(fieldName))
    }.toArray

    val candidateFailuresRdd: RDD[ValidationCandidate] = joined.rdd
      .flatMap { joinedRow =>
        val srcNull = srcPKIndices.forall { case (_, idx) => joinedRow.isNullAt(idx) }
        val tgtNull = tgtPKIndices.forall { case (_, idx) => joinedRow.isNullAt(idx) }

        if (srcNull) {
          // Row exists in target but not in source
          val tgtRepr =
            tgtPKIndices
              .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
              .mkString(", ")
          Some(
            FinalValidationFailure(
              RowComparisonFailure(
                tgtRepr,
                None,
                List(RowComparisonFailure.Item.ExtraTargetRow)
              )
            )
          )
        } else if (tgtNull) {
          val srcRepr =
            srcPKIndices
              .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
              .mkString(", ")
          Some(
            FinalValidationFailure(
              RowComparisonFailure(
                srcRepr,
                None,
                List(RowComparisonFailure.Item.MissingTargetRow)
              )
            )
          )
        } else {
          val directDifferingFields =
            differingFieldNamesForRow(joinedRow, directFieldIndices, tsTol, floatTol).map {
              colName =>
                val (srcIdx, tgtIdx) = fieldIndexByName(colName)
                val srcVal = if (joinedRow.isNullAt(srcIdx)) None else Some(joinedRow.get(srcIdx))
                val tgtVal = if (joinedRow.isNullAt(tgtIdx)) None else Some(joinedRow.get(tgtIdx))
                s"$colName (source=${truncateValue(srcVal)}, target=${truncateValue(tgtVal)})"
            }
          val hashMismatch =
            hasContentHashMismatch(joinedRow, contentHashFieldIndices, tsTol, floatTol)
          if (directDifferingFields.isEmpty && !hashMismatch) None
          else {
            val srcPkValues = srcPKIndices.map { case (_, idx) => joinedRow.get(idx) }.toVector
            val srcRepr =
              srcPKIndices
                .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
                .mkString(", ")
            val tgtRepr =
              tgtPKIndices
                .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
                .mkString(", ")
            Some(
              MatchedRowValidationCandidate(
                srcPkValues,
                srcRepr,
                tgtRepr,
                directDifferingFields.toList,
                hashMismatch
              )
            )
          }
        }
      }

    val sourceSideFailures = collectFailureSample(
      candidateFailuresRdd,
      rawSourceDF,
      rawTargetDF,
      renamedPK,
      finalHashBackedColumns,
      tsTol,
      floatTol,
      validationConfig.failuresToFetch
    )
    val failures =
      if (
        sourceSettings.where.isDefined || sourceSideFailures.size >= validationConfig.failuresToFetch
      )
        sourceSideFailures
      else
        sourceSideFailures ++ collectExtraTargetFailureSample(
          rawSourceDF,
          targetConnector,
          targetTableDef,
          targetSettings,
          renamedPK,
          validationConfig.failuresToFetch - sourceSideFailures.size
        )
    log.info(
      s"Validation complete for ${sourceSettings.database}.${sourceSettings.table} -> " +
        s"${targetSettings.keyspace}.${targetSettings.table}. " +
        s"Collected ${failures.size} failure(s) in sample."
    )

    failures
  }

  private[scylla] def resolveHashBackedDifferences(
    rawSourceDF: DataFrame,
    rawTargetDF: DataFrame,
    primaryKey: Seq[String],
    primaryKeyValues: Seq[Vector[Any]],
    hashBackedColumns: Seq[String],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  )(implicit spark: SparkSession): Map[Vector[Any], List[String]] = {
    val refinementFrames =
      createHashRefinementFrames(rawSourceDF, rawTargetDF, primaryKey, hashBackedColumns)
    try
      resolveHashBackedDifferences(
        refinementFrames,
        primaryKey,
        primaryKeyValues,
        hashBackedColumns,
        timestampMsTolerance,
        floatingPointTolerance
      )
    finally {
      refinementFrames.source.unpersist(blocking = false)
      refinementFrames.target.unpersist(blocking = false)
    }
  }

  private def resolveHashBackedDifferences(
    refinementFrames: HashRefinementFrames,
    primaryKey: Seq[String],
    primaryKeyValues: Seq[Vector[Any]],
    hashBackedColumns: Seq[String],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double
  )(implicit spark: SparkSession): Map[Vector[Any], List[String]] = {
    val projectedSource = refinementFrames.source
    val projectedTarget = refinementFrames.target
    val resolvedPrimaryKey = primaryKey.map(resolveFieldName(projectedSource.schema.fieldNames, _))
    val primaryKeySchema = StructType(resolvedPrimaryKey.map(projectedSource.schema(_)))
    val distinctPrimaryKeyValues = primaryKeyValues.distinctBy(normalizePrimaryKeyValues)
    val keyRows = distinctPrimaryKeyValues.map(Row.fromSeq)
    val keyDf = spark.createDataFrame(spark.sparkContext.parallelize(keyRows), primaryKeySchema)

    val sourceLookup =
      prefixColumns(keyDf.join(projectedSource, resolvedPrimaryKey, "inner"), "src_")
    val targetLookup =
      prefixColumns(keyDf.join(projectedTarget, resolvedPrimaryKey, "inner"), "tgt_")

    val sourceLookupFields = sourceLookup.schema.fieldNames
    val targetLookupFields = targetLookup.schema.fieldNames
    val joinCondition = primaryKey
      .map { pk =>
        val srcCol = resolveFieldName(sourceLookupFields, s"src_$pk")
        val tgtCol = resolveFieldName(targetLookupFields, s"tgt_$pk")
        sparkColumn(srcCol) === sparkColumn(tgtCol)
      }
      .reduce(_ && _)
    val joined = sourceLookup.join(targetLookup, joinCondition, "full_outer")
    val joinedFields = joined.schema.fieldNames
    val srcPKIndices = primaryKey.map { pk =>
      joinedFields.indexOf(resolveFieldName(joinedFields, s"src_$pk"))
    }.toArray
    val tgtPKIndices = primaryKey.map { pk =>
      joinedFields.indexOf(resolveFieldName(joinedFields, s"tgt_$pk"))
    }.toArray
    val hashBackedFieldIndices = hashBackedColumns.map { colName =>
      val srcIdx = joinedFields.indexOf(resolveFieldName(joinedFields, s"src_$colName"))
      val tgtIdx = joinedFields.indexOf(resolveFieldName(joinedFields, s"tgt_$colName"))
      (colName, srcIdx, tgtIdx)
    }

    joined.rdd
      .map { joinedRow =>
        val srcNull = srcPKIndices.forall(joinedRow.isNullAt)
        val tgtNull = tgtPKIndices.forall(joinedRow.isNullAt)
        val rawPrimaryKeyValues =
          if (!srcNull) srcPKIndices.map(joinedRow.get).toVector
          else tgtPKIndices.map(joinedRow.get).toVector
        val normalizedPrimaryKey = normalizePrimaryKeyValues(rawPrimaryKeyValues)
        val differingFields =
          if (srcNull || tgtNull)
            List(
              s"hashColumns(${hashBackedColumns.mkString(", ")}) " +
                "(content hash mismatch; values fetched by PK could not be resolved)"
            )
          else
            hashBackedFieldIndices.flatMap { case (colName, srcIdx, tgtIdx) =>
              val srcVal = if (joinedRow.isNullAt(srcIdx)) None else Some(joinedRow.get(srcIdx))
              val tgtVal = if (joinedRow.isNullAt(tgtIdx)) None else Some(joinedRow.get(tgtIdx))
              if (
                areDifferent(
                  srcVal,
                  tgtVal,
                  timestampMsTolerance,
                  floatingPointTolerance
                )
              )
                Some(
                  s"$colName (source=${truncateValue(srcVal)}, target=${truncateValue(tgtVal)})"
                )
              else None
            }.toList
        normalizedPrimaryKey -> differingFields
      }
      .collect()
      .toMap
  }

  /** Add a `_content_hash` column to a DataFrame using per-column SHA-256 digests. Each column is
    * cast to StringType then hashed with SHA-256, with `sha2("1|", 256)` used as the sentinel for
    * null values so that every element in the concatenation is a fixed-width hex digest. The
    * results are concatenated with '|' as separator and the whole concatenation is hashed again.
    *
    * This function is applied identically to both the MySQL-sourced DataFrame and the
    * ScyllaDB-sourced DataFrame, ensuring that the same Spark code path produces the hash on both
    * sides. This eliminates any risk of hash divergence due to differing string conversion rules
    * between MySQL and Spark.
    *
    * After adding the hash, the original hashed columns are dropped from the DataFrame to reduce
    * shuffle data volume during the join.
    */
  private[scylla] def addContentHash(
    df: DataFrame,
    hashCols: List[String],
    pkCols: List[String],
    dropHashedColumns: Boolean = true
  ): DataFrame = {
    val dfColsLower = df.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    require(
      !dfColsLower.contains(MySQL.ContentHashColumn.toLowerCase(Locale.ROOT)),
      s"Source/target table contains a column named '${MySQL.ContentHashColumn}' which conflicts " +
        "with the internal hash column. Rename the column or disable hash-based validation."
    )
    val existingHashCols = hashCols.filter(c => dfColsLower.contains(c.toLowerCase(Locale.ROOT)))
    if (existingHashCols.isEmpty) {
      log.warn("No hash columns found in DataFrame. Skipping hash computation.")
      df
    } else {
      // Resolve user-supplied column names against the actual DataFrame schema so that
      // subsequent operations (col(), drop()) use the exact case from the schema rather
      // than the user-supplied case. Spark's drop() is case-insensitive by default, but
      // using resolved names makes the code robust against configuration changes.
      val dfCols = df.columns
      def resolveCol(name: String): String =
        dfCols.find(_.equalsIgnoreCase(name)).getOrElse(name)
      val resolvedHashCols =
        existingHashCols.map(resolveCol).sortBy(_.toLowerCase(Locale.ROOT))

      log.info(
        s"Computing content hash for columns: ${resolvedHashCols.mkString(", ")}"
      )

      val contentHashBits = 256
      val perColHashes = resolvedHashCols.map { c =>
        val encodedValue = df.schema(c).dataType match {
          case BinaryType => base64(sparkColumn(c))
          case _          => sparkColumn(c).cast(StringType)
        }
        when(sparkColumn(c).isNull, sha2(lit("1|"), contentHashBits))
          .otherwise(sha2(concat(lit("0|"), encodedValue), contentHashBits))
      }
      val hashCol = sha2(concat_ws("|", perColHashes: _*), contentHashBits)
      val withHash = df.withColumn(MySQL.ContentHashColumn, hashCol)

      if (!dropHashedColumns) withHash
      else {
        // Preserve primary key columns; drop only the hashed columns to reduce shuffle volume.
        val pkColsLower = pkCols.map(_.toLowerCase(Locale.ROOT)).toSet
        val colsToDrop = resolvedHashCols
          .filterNot(c => pkColsLower.contains(c.toLowerCase(Locale.ROOT)))
        colsToDrop.foldLeft(withHash) { (d, c) =>
          d.drop(c)
        }
      }
    }
  }

  private[scylla] def prefixColumns(df: DataFrame, prefix: String): DataFrame =
    df.select(df.columns.toIndexedSeq.map(c => sparkColumn(c).as(s"$prefix$c")): _*)

  private val MaxValueReprLength = 100

  /** Truncate a value for inclusion in failure messages. */
  private def truncateValue(value: Option[Any]): String = value match {
    case None => "NULL"
    case Some(v) =>
      val repr = v match {
        case arr: Array[Byte] => s"[${arr.length} bytes]"
        case arr: Array[_]    => arr.mkString("[", ",", "]")
        case other            => other.toString
      }
      if (repr.length > MaxValueReprLength) repr.take(MaxValueReprLength) + "..."
      else repr
  }
}
