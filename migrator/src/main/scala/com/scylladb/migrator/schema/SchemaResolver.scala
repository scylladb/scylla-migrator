package com.scylladb.migrator.schema

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions.col

import java.util.Locale

/** Shared case-insensitive schema resolution utilities for readers, writers, and validators. */
object SchemaResolver {

  /** Non-throwing case-insensitive field lookup. Prefers exact-case match, falls back to
    * case-insensitive using Locale.ROOT normalization.
    */
  def findFieldName(fields: Array[String], name: String): Option[String] = {
    val nameLower = name.toLowerCase(Locale.ROOT)
    fields.find(_ == name).orElse(fields.find(_.toLowerCase(Locale.ROOT) == nameLower))
  }

  /** Case-insensitive field lookup. Prefers exact-case match. Throws with a diagnostic error
    * listing available columns if no match is found.
    */
  def resolveFieldName(fields: Array[String], name: String): String =
    findFieldName(fields, name)
      .getOrElse(
        sys.error(
          s"Column '$name' not found in schema. Available columns: ${fields.mkString(", ")}. " +
            "This may indicate a missing rename entry or schema mismatch."
        )
      )

  /** Backtick-escape a column name for Spark SQL expressions (handles dots and embedded backticks).
    */
  def escapeSparkColumnName(name: String): String =
    s"`${name.replace("`", "``")}`"

  /** Create a Spark Column reference with proper backtick quoting. */
  def sparkColumn(name: String): Column =
    col(escapeSparkColumnName(name))

  /** Detect case-insensitive column name collisions in a field name sequence. Throws
    * IllegalArgumentException with details if collisions are found.
    *
    * @param context
    *   human-readable description of where the collision was detected
    */
  def requireNoCollisions(fieldNames: Seq[String], context: String): Unit = {
    val collisions =
      fieldNames
        .groupBy(_.toLowerCase(Locale.ROOT))
        .collect { case (_, names) if names.size > 1 => names.distinct }
    if (collisions.nonEmpty) {
      val collisionDetails = collisions
        .map(names => names.mkString("[", ", ", "]"))
        .mkString(", ")
      throw new IllegalArgumentException(
        s"Column name collision detected $context. " +
          s"Multiple source columns resolve to the same target column name: $collisionDetails. " +
          "Check the configured renames and source schema."
      )
    }
  }

  /** Validate that all requested columns exist (case-insensitively) in the available columns.
    * Throws with a diagnostic error listing missing columns.
    *
    * @param context
    *   human-readable description (e.g. "in target after renames")
    */
  def validateColumnPresence(
    requested: Seq[String],
    available: Seq[String],
    context: String
  ): Unit = {
    val availableLower = available.map(_.toLowerCase(Locale.ROOT)).toSet
    val missing = requested.filterNot(c => availableLower.contains(c.toLowerCase(Locale.ROOT)))
    if (missing.nonEmpty)
      sys.error(
        s"Columns not found: ${missing.mkString(", ")}. Context: $context. " +
          s"Available: ${available.mkString(", ")}."
      )
  }

  /** Return columns present in `superset` but not in `subset` (case-insensitive comparison). */
  def columnsOnlyIn(superset: Seq[String], subset: Seq[String]): Seq[String] = {
    val subsetLower = subset.map(_.toLowerCase(Locale.ROOT)).toSet
    superset.filterNot(c => subsetLower.contains(c.toLowerCase(Locale.ROOT)))
  }

  /** Prefix all DataFrame column names with a given string. */
  def prefixColumns(df: DataFrame, prefix: String): DataFrame =
    df.select(df.columns.toIndexedSeq.map(c => sparkColumn(c).as(s"$prefix$c")): _*)

  /** Select and alias columns by resolving names case-insensitively against the DataFrame schema.
    */
  def selectAndAlias(df: DataFrame, columns: Seq[String]): DataFrame = {
    val schemaFields = df.schema.fieldNames
    df.select(
      columns.toIndexedSeq.map { columnName =>
        sparkColumn(resolveFieldName(schemaFields, columnName)).as(columnName)
      }: _*
    )
  }
}
