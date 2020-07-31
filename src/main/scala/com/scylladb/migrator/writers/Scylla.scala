package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, Rename, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }

object Scylla {
  val log = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
    df: DataFrame,
    timestampColumns: Option[TimestampColumns],
    tokenRangeAccumulator: Option[TokenRangeAccumulator])(implicit spark: SparkSession): Unit = {
    val connector = Connectors.targetConnector(spark.sparkContext.getConf, target)
    val writeConf = WriteConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(
        ttl = timestampColumns.map(_.ttl).fold(TTLOption.defaultValue)(TTLOption.perRow),
        timestamp = timestampColumns
          .map(_.writeTime)
          .fold(TimestampOption.defaultValue)(TimestampOption.perRow)
      )

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    val renamedSchema = renames
      .foldLeft(df) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

    log.info("Schema after renames:")
    log.info(renamedSchema.treeString)

    val columnSelector =
      timestampColumns match {
        case None =>
          SomeColumns(renamedSchema.fields.map(_.name: ColumnRef): _*)
        case Some(TimestampColumns(ttl, writeTime)) =>
          SomeColumns(
            renamedSchema.fields
              .map(x => x.name: ColumnRef)
              .filterNot(ref => ref.columnName == ttl || ref.columnName == writeTime): _*)
      }

    df.rdd.saveToCassandra(
      target.keyspace,
      target.table,
      columnSelector,
      writeConf,
      tokenRangeAccumulator = tokenRangeAccumulator
    )(connector, SqlRowWriter.Factory)
  }

}
