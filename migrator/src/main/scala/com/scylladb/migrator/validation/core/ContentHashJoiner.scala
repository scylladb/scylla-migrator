package com.scylladb.migrator.validation.core

import com.scylladb.migrator.readers.MySQL
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ Column, DataFrame, Row }
import org.apache.spark.sql.functions.{ base64, concat, concat_ws, lit, sha2, when }
import org.apache.spark.sql.types.{ BinaryType, StringType }
import java.util.Locale

object ContentHashJoiner {
  private val log = LogManager.getLogger("com.scylladb.migrator.validation.core.ContentHashJoiner")

  val ContentHashColumn: String = MySQL.ContentHashColumn

  def addContentHash(
    df: DataFrame,
    hashCols: List[String],
    pkCols: List[String],
    dropHashedColumns: Boolean = true
  ): DataFrame = {
    val dfColsLower = df.columns.map(_.toLowerCase(Locale.ROOT)).toSet
    require(
      !dfColsLower.contains(ContentHashColumn.toLowerCase(Locale.ROOT)),
      s"Source/target table contains a column named '$ContentHashColumn' which conflicts " +
        "with the internal hash column. Rename the column or disable hash-based validation."
    )
    val existingHashCols = hashCols.filter(c => dfColsLower.contains(c.toLowerCase(Locale.ROOT)))
    if (existingHashCols.isEmpty) {
      log.warn("No hash columns found in DataFrame. Skipping hash computation.")
      df
    } else {
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
          case BinaryType => base64(SchemaResolver.sparkColumn(c))
          case _          => SchemaResolver.sparkColumn(c).cast(StringType)
        }
        when(SchemaResolver.sparkColumn(c).isNull, sha2(lit("1|"), contentHashBits))
          .otherwise(sha2(concat(lit("0|"), encodedValue), contentHashBits))
      }
      val hashCol = sha2(concat_ws("|", perColHashes: _*), contentHashBits)
      val withHash = df.withColumn(ContentHashColumn, hashCol)

      if (!dropHashedColumns) withHash
      else {
        val pkColsLower = pkCols.map(_.toLowerCase(Locale.ROOT)).toSet
        val colsToDrop = resolvedHashCols
          .filterNot(c => pkColsLower.contains(c.toLowerCase(Locale.ROOT)))
        colsToDrop.foldLeft(withHash) { (d, c) =>
          d.drop(c)
        }
      }
    }
  }

  def differingFieldNamesForRow(
    joinedRow: Row,
    fieldIndices: Seq[(String, Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): List[String] =
    fieldIndices.flatMap { case (colName, srcIdx, tgtIdx) =>
      val srcVal = if (joinedRow.isNullAt(srcIdx)) None else Some(joinedRow.get(srcIdx))
      val tgtVal = if (joinedRow.isNullAt(tgtIdx)) None else Some(joinedRow.get(tgtIdx))
      if (
        RowComparisonFailure.areDifferent(
          srcVal,
          tgtVal,
          timestampMsTolerance,
          floatingPointTolerance,
          numericTypePolicy
        )
      )
        Some(colName)
      else
        None
    }.toList

  /** Compare fields split into direct and hash-backed groups. Direct columns are compared first via
    * [[RowComparisonFailure.areDifferent]], which delegates to
    * [[NumericComparison.compareWithPolicy]] for Number pairs — so StrictType/DetectWiden type
    * mismatches (e.g. Float(1.5f) vs Double(1.5d)) are caught at this stage. Hash-backed columns
    * skip per-value comparison when content hashes match. Because the hash is computed over
    * string-cast values, it is type-erasing and cannot detect per-value numeric type mismatches.
    * This gap is addressed in [[com.scylladb.migrator.scylla.MySQLToScyllaValidator]]: under
    * DetectWiden, Float/Double and cross-category (numeric vs non-numeric) hash-backed columns are
    * promoted to direct comparison so per-value checks run; under StrictType, schema-level type
    * mismatches on remaining hash-backed columns are reported as errors.
    */
  def compareFieldsBySchemaForRow(
    joinedRow: Row,
    directFieldIndices: Seq[(String, Int, Int)],
    hashBackedFieldIndices: Seq[(String, Int, Int)],
    contentHashFieldIndices: Option[(Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): List[String] = {
    val directDifferences =
      differingFieldNamesForRow(
        joinedRow,
        directFieldIndices,
        timestampMsTolerance,
        floatingPointTolerance,
        numericTypePolicy
      )

    val hashBackedDifferences = contentHashFieldIndices match {
      case Some((srcHashIdx, tgtHashIdx)) =>
        val srcHash = if (joinedRow.isNullAt(srcHashIdx)) None else Some(joinedRow.get(srcHashIdx))
        val tgtHash = if (joinedRow.isNullAt(tgtHashIdx)) None else Some(joinedRow.get(tgtHashIdx))
        if (
          RowComparisonFailure.areDifferent(
            srcHash,
            tgtHash,
            timestampMsTolerance,
            floatingPointTolerance,
            numericTypePolicy
          )
        )
          differingFieldNamesForRow(
            joinedRow,
            hashBackedFieldIndices,
            timestampMsTolerance,
            floatingPointTolerance,
            numericTypePolicy
          )
        else
          Nil
      case None =>
        differingFieldNamesForRow(
          joinedRow,
          hashBackedFieldIndices,
          timestampMsTolerance,
          floatingPointTolerance,
          numericTypePolicy
        )
    }

    directDifferences ++ hashBackedDifferences
  }

  def hasContentHashMismatch(
    joinedRow: Row,
    contentHashFieldIndices: Option[(Int, Int)],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): Boolean =
    contentHashFieldIndices.exists { case (srcHashIdx, tgtHashIdx) =>
      val srcHash = if (joinedRow.isNullAt(srcHashIdx)) None else Some(joinedRow.get(srcHashIdx))
      val tgtHash = if (joinedRow.isNullAt(tgtHashIdx)) None else Some(joinedRow.get(tgtHashIdx))
      RowComparisonFailure.areDifferent(
        srcHash,
        tgtHash,
        timestampMsTolerance,
        floatingPointTolerance,
        numericTypePolicy
      )
    }
}
