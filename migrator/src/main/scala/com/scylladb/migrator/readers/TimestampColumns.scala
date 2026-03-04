package com.scylladb.migrator.readers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ IntegerType, LongType, StructType }

case class TimestampColumns(ttl: String, writeTime: String)

object TimestampColumns {

  /** Prefix used to mark metadata columns in Parquet files. These columns carry CQL TTL and
    * writetime values and should not be treated as regular data columns when migrating to non-CQL
    * targets.
    */
  val MetaPrefix = "__migrator_meta_"

  /** Rename per-column `_ttl` and `_writetime` columns to `__migrator_meta_` prefixed names for
    * storage in Parquet. Only renames columns where a matching base column exists (e.g., `foo_ttl`
    * is renamed only if `foo` exists in the schema).
    */
  def renameForParquet(df: DataFrame): DataFrame = {
    val fieldNames = df.schema.fields.map(_.name).toSet
    val renames = df.schema.fields.collect {
      case f
          if f.name.endsWith("_ttl") &&
            fieldNames.contains(f.name.stripSuffix("_ttl")) =>
        (f.name, s"${MetaPrefix}${f.name}")
      case f
          if f.name.endsWith("_writetime") &&
            fieldNames.contains(f.name.stripSuffix("_writetime")) =>
        (f.name, s"${MetaPrefix}${f.name}")
    }
    renames.foldLeft(df) { case (acc, (from, to)) => acc.withColumnRenamed(from, to) }
  }

  /** Rename `__migrator_meta_*_ttl` and `__migrator_meta_*_writetime` columns back to their
    * original `*_ttl` and `*_writetime` names.
    */
  def renameFromParquet(df: DataFrame): DataFrame = {
    val renames = df.schema.fields.collect {
      case f if f.name.startsWith(MetaPrefix) => (f.name, f.name.stripPrefix(MetaPrefix))
    }
    renames.foldLeft(df) { case (acc, (from, to)) => acc.withColumnRenamed(from, to) }
  }

  /** Check if schema has per-column metadata columns from a Parquet file written by the migrator.
    */
  def hasPerColumnMetaInParquet(schema: StructType): Boolean =
    schema.fields.exists { f =>
      f.name.startsWith(MetaPrefix) &&
      (f.name.endsWith("_ttl") || f.name.endsWith("_writetime"))
    }

  /** Strip all `__migrator_meta_*` columns from a DataFrame. Use this when migrating Parquet data
    * to targets that do not support CQL TTL/writetime metadata (e.g., DynamoDB).
    */
  def stripMetaColumns(df: DataFrame): DataFrame = {
    val metaCols = df.schema.fields.collect {
      case f if f.name.startsWith(MetaPrefix) => f.name
    }
    metaCols.foldLeft(df)((acc, col) => acc.drop(col))
  }
}
