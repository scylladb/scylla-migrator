package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, Rename, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

object Scylla {
  val log = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
    df: DataFrame,
    timestampColumns: Option[TimestampColumns])(implicit spark: SparkSession): Unit = {
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

    val columnSelector = SomeColumns(renamedSchema.fields.map(_.name: ColumnRef): _*)

    // Spark's conversion from its internal Decimal type to java.math.BigDecimal
    // pads the resulting value with trailing zeros corresponding to the scale of the
    // Decimal type. Some users don't like this so we conditionally strip those.
    val rdd =
      if (!target.stripTrailingZerosForDecimals) df.rdd
      else
        df.rdd.map { row =>
          Row.fromSeq(row.toSeq.map {
            case x: java.math.BigDecimal => x.stripTrailingZeros()
            case x                       => x
          })
        }

    rdd
      .saveToCassandra(
        target.keyspace,
        target.table,
        columnSelector,
        writeConf
      )(connector, SqlRowWriter.Factory)
  }

}
