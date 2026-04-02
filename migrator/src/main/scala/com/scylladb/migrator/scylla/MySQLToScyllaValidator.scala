package com.scylladb.migrator.scylla

import com.datastax.spark.connector.cql.Schema
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.readers
import com.scylladb.migrator.readers.MySQL
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import java.util.Locale

object MySQLToScyllaValidator {

  private val log = LogManager.getLogger("com.scylladb.migrator.scylla.MySQLToScyllaValidator")

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

    val renamesMap = config.renamesMap
    val renamedPK = primaryKey.map(renamesMap)
    val renamedPKLower = renamedPK.map(_.toLowerCase(Locale.ROOT)).toSet

    if (config.getRenamesOrNil.nonEmpty) {
      val unmappedPK = primaryKey.filterNot(pk => config.getRenamesOrNil.exists(_.from == pk))
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
      val renamedHashCols = cols.map(renamesMap)
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
    hashColumns.foreach(cols =>
      log.info(s"Hash-based comparison for columns: ${cols.mkString(", ")}")
    )

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
      val renamed = df.select(df.columns.toIndexedSeq.map(c => col(c).as(renamesMap(c))): _*)
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

    val targetConnectionOptions = ScyllaSparkConnectionOptions.fromTargetSettings(targetSettings)

    log.info(s"Connecting to ScyllaDB target at ${targetSettings.host}:${targetSettings.port}")

    // Note: the target is read in full even when source.where filters the MySQL read. This is
    // intentional -- the validator cannot reliably translate a MySQL WHERE clause into a CQL
    // filter. For large target tables this may be wasteful, but it ensures correctness: the
    // left_outer join (used when where is set) correctly detects missing rows in the target
    // without producing false "extra target row" positives from pre-existing data.
    val rawTargetDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(targetConnectionOptions)
      .load()

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
    val configuredPKLower = renamedPK.map(_.toLowerCase(Locale.ROOT))
    if (actualTargetPK.toSet != configuredPKLower.toSet)
      log.warn(
        s"Configured primaryKey (after renames) does not match target table's actual PK. " +
          s"Configured: ${renamedPK.mkString(", ")}. " +
          s"Actual target PK: ${actualTargetPK.mkString(", ")}. " +
          "This may produce incorrect validation results."
      )

    // Warn when renamed source columns don't match target columns. This catches missing rename
    // entries that would silently cause the join to report every row as different.
    val renamedSourceCols = rawSourceDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val targetCols = rawTargetDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val missingInTarget = renamedSourceCols -- targetCols
    val missingInSource = targetCols -- renamedSourceCols
    if (missingInTarget.nonEmpty)
      log.warn(
        s"Source columns not found in target after renames: ${missingInTarget.mkString(", ")}. " +
          "These columns will be excluded from comparison. " +
          "If this is unexpected, check your 'renames' configuration."
      )
    if (missingInSource.nonEmpty)
      log.info(
        s"Target columns not present in (renamed) source: ${missingInSource.mkString(", ")}"
      )

    val (sourceDF, targetDF, comparableColumns) = hashColumns match {
      case Some(cols) =>
        val renamedCols = cols.map(renamesMap)
        // Intersect hash columns with both source and target to ensure the same
        // column set is hashed on both sides. Without this, a column present in
        // the source but missing in the target would cause every row to show a
        // hash mismatch.
        // Use case-insensitive comparison because ScyllaDB/CQL normalizes
        // unquoted column names to lowercase.
        val srcColsLower = rawSourceDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
        val tgtColsLower = rawTargetDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
        val effectiveHashCols = renamedCols
          .filter(c => srcColsLower.contains(c.toLowerCase(Locale.ROOT)))
          .filter(c => tgtColsLower.contains(c.toLowerCase(Locale.ROOT)))
        val effectiveHashColsLower =
          effectiveHashCols.map(_.toLowerCase(Locale.ROOT)).toSet
        val droppedHashCols =
          renamedCols.filterNot(c => effectiveHashColsLower.contains(c.toLowerCase(Locale.ROOT)))
        if (droppedHashCols.nonEmpty)
          log.warn(
            s"Hash columns not present in both source and target (excluded from hash): " +
              s"${droppedHashCols.mkString(", ")}"
          )

        val hashedSource = addContentHash(rawSourceDF, effectiveHashCols, renamedPK)
        val hashedTarget = addContentHash(rawTargetDF, effectiveHashCols, renamedPK)
        val nonPKCols = hashedSource.columns
          .filter(c => !renamedPKLower.contains(c.toLowerCase(Locale.ROOT)))
          .filter(_ != MySQL.ContentHashColumn)
        val comparableCols =
          if (hashedSource.columns.contains(MySQL.ContentHashColumn))
            nonPKCols :+ MySQL.ContentHashColumn
          else {
            log.warn("Hash column was not created. Falling back to full column comparison.")
            nonPKCols
          }
        (hashedSource, hashedTarget, comparableCols)

      case None =>
        val nonPKCols =
          rawSourceDF.columns.filter(c => !renamedPKLower.contains(c.toLowerCase(Locale.ROOT)))
        (rawSourceDF, rawTargetDF, nonPKCols)
    }

    val targetColumnsLower = targetDF.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val droppedColumns =
      comparableColumns.filterNot(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    if (droppedColumns.nonEmpty)
      log.warn(
        s"Columns present in source but missing in target (skipped): ${droppedColumns.mkString(", ")}"
      )
    val finalComparableColumns =
      comparableColumns.filter(c => targetColumnsLower.contains(c.toLowerCase(Locale.ROOT)))
    log.info(s"Comparable columns: ${finalComparableColumns.mkString(", ")}")

    val sourcePrefixed = prefixColumns(sourceDF, "src_")
    val targetPrefixed = prefixColumns(targetDF, "tgt_")

    // Resolve PK column names against the actual DataFrame schemas using case-insensitive
    // lookup. This is necessary because ScyllaDB normalizes unquoted column names to lowercase,
    // so a MySQL column "UserId" may appear as "userid" in the target DataFrame. Spark's
    // DataFrame.apply() resolves columns case-insensitively, but Row.fieldIndex() is
    // case-sensitive and would throw IllegalArgumentException on a mismatch.
    val srcSchemaFields = sourcePrefixed.schema.fieldNames
    val tgtSchemaFields = targetPrefixed.schema.fieldNames

    def resolveField(fields: Array[String], name: String): String =
      fields
        .find(_.equalsIgnoreCase(name))
        .getOrElse(
          sys.error(
            s"Column '$name' not found in schema. Available columns: ${fields.mkString(", ")}. " +
              "This may indicate a missing rename entry or schema mismatch."
          )
        )

    val joinCondition = renamedPK
      .map { pk =>
        val srcCol = resolveField(srcSchemaFields, s"src_$pk")
        val tgtCol = resolveField(tgtSchemaFields, s"tgt_$pk")
        sourcePrefixed(srcCol) === targetPrefixed(tgtCol)
      }
      .reduce(_ && _)

    // Note: this join loads both the full source and target tables via Spark. For very large
    // tables (millions+ rows) this can be expensive in memory and network I/O. Unlike the
    // Cassandra-to-Scylla validator which uses per-partition lookups, this approach performs a
    // full shuffle. Use the `failuresToFetch` config to limit how many failures are collected.
    val joinType = if (sourceSettings.where.isDefined) {
      log.warn(
        "Source 'where' filter is configured. Disabling extra-target-row detection " +
          "because the target table may contain rows outside the filter scope."
      )
      "left_outer"
    } else "full_outer"
    val joined = sourcePrefixed.join(targetPrefixed, joinCondition, joinType)

    val joinedSchemaFields = joined.schema.fieldNames
    val floatTol = validationConfig.floatingPointTolerance
    val tsTol = validationConfig.timestampMsTolerance

    // Pre-compute field indices to avoid repeated case-insensitive lookups on every row.
    // For wide tables (100+ columns) × millions of rows, this provides significant speedup.
    val fieldIndices = finalComparableColumns.map { colName =>
      val srcFieldName = resolveField(joinedSchemaFields, s"src_$colName")
      val tgtFieldName = resolveField(joinedSchemaFields, s"tgt_$colName")
      (colName, joinedSchemaFields.indexOf(srcFieldName), joinedSchemaFields.indexOf(tgtFieldName))
    }.toArray

    // Pre-compute PK field indices so that the RDD closure uses index-based access
    // instead of calling resolveField (linear scan) on every row.
    val srcPKIndices = renamedPK.map { pk =>
      val fieldName = resolveField(joinedSchemaFields, s"src_$pk")
      (pk, joinedSchemaFields.indexOf(fieldName))
    }.toArray
    val tgtPKIndices = renamedPK.map { pk =>
      val fieldName = resolveField(joinedSchemaFields, s"tgt_$pk")
      (pk, joinedSchemaFields.indexOf(fieldName))
    }.toArray

    val failuresRdd = joined.rdd
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
            RowComparisonFailure(
              tgtRepr,
              None,
              List(RowComparisonFailure.Item.ExtraTargetRow)
            )
          )
        } else if (tgtNull) {
          val srcRepr =
            srcPKIndices
              .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
              .mkString(", ")
          Some(
            RowComparisonFailure(srcRepr, None, List(RowComparisonFailure.Item.MissingTargetRow))
          )
        } else {
          val differingFields = fieldIndices.flatMap { case (colName, srcIdx, tgtIdx) =>
            val srcVal = if (joinedRow.isNullAt(srcIdx)) None else Some(joinedRow.get(srcIdx))
            val tgtVal = if (joinedRow.isNullAt(tgtIdx)) None else Some(joinedRow.get(tgtIdx))
            if (RowComparisonFailure.areDifferent(srcVal, tgtVal, tsTol, floatTol))
              Some(
                s"$colName (source=${truncateValue(srcVal)}, target=${truncateValue(tgtVal)})"
              )
            else
              None
          }
          if (differingFields.isEmpty) None
          else {
            val srcRepr =
              srcPKIndices
                .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
                .mkString(", ")
            val tgtRepr =
              tgtPKIndices
                .map { case (pk, idx) => s"$pk=${joinedRow.get(idx)}" }
                .mkString(", ")
            Some(
              RowComparisonFailure(
                srcRepr,
                Some(tgtRepr),
                List(RowComparisonFailure.Item.DifferingFieldValues(differingFields.toList))
              )
            )
          }
        }
      }

    val failures = failuresRdd.take(validationConfig.failuresToFetch).toList
    log.info(
      s"Validation complete for ${sourceSettings.database}.${sourceSettings.table} -> " +
        s"${targetSettings.keyspace}.${targetSettings.table}. " +
        s"Collected ${failures.size} failure(s) in sample."
    )

    failures
  }

  /** Add a `_content_hash` column to a DataFrame using per-column MD5 digests. Each column is cast
    * to StringType then hashed with MD5, with `md5("__NULL_SENTINEL__")` used as the sentinel for
    * null values so that every element in the concatenation is a 32-char hex digest. The results
    * are concatenated with '|' as separator and the whole concatenation is hashed again.
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
    pkCols: List[String]
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

      // Cast each column to StringType before hashing so that different underlying types
      // (IntegerType, DecimalType, TimestampType, etc.) produce a consistent string
      // representation. Without this cast, md5() on non-string types would either produce
      // different bytes or throw an AnalysisException.
      val nullSentinel = md5(lit("__NULL_SENTINEL__"))
      val perColHashes = resolvedHashCols.map { c =>
        coalesce(md5(col(c).cast(StringType)), nullSentinel)
      }
      val hashCol = md5(concat_ws("|", perColHashes: _*))
      val withHash = df.withColumn(MySQL.ContentHashColumn, hashCol)

      // Preserve primary key columns; drop only the hashed columns to reduce shuffle volume.
      val pkColsLower = pkCols.map(_.toLowerCase(Locale.ROOT)).toSet
      val colsToDrop = resolvedHashCols
        .filterNot(c => pkColsLower.contains(c.toLowerCase(Locale.ROOT)))
      colsToDrop.foldLeft(withHash) { (d, c) =>
        d.drop(c)
      }
    }
  }

  private[scylla] def prefixColumns(df: DataFrame, prefix: String): DataFrame =
    df.select(df.columns.toIndexedSeq.map(c => col(c).as(s"$prefix$c")): _*)

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
