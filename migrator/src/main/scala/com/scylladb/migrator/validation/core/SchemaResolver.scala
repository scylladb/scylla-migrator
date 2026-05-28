package com.scylladb.migrator.validation.core

import com.scylladb.migrator.config.Rename
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions.col
import java.util.Locale

object SchemaResolver {

  def buildCaseInsensitiveRenameMap(
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

  def escapeSparkColumnName(name: String): String =
    s"`${name.replace("`", "``")}`"

  def sparkColumn(name: String): Column =
    col(escapeSparkColumnName(name))

  def resolveFieldName(fields: Array[String], name: String): String =
    fields
      .find(_.equalsIgnoreCase(name))
      .getOrElse(
        sys.error(
          s"Column '$name' not found in schema. Available columns: ${fields.mkString(", ")}. " +
            "This may indicate a missing rename entry or schema mismatch."
        )
      )

  def prefixColumns(df: DataFrame, prefix: String): DataFrame =
    df.select(df.columns.toIndexedSeq.map(c => sparkColumn(c).as(s"$prefix$c")): _*)
}
