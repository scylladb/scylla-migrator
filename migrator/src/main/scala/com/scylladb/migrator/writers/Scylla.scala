package com.scylladb.migrator.writers

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ Rename, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

import scala.collection.compat.immutable.ArraySeq

object Scylla {
  val log: Logger = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
    df: DataFrame,
    timestampColumns: Option[TimestampColumns],
    tokenRangeAccumulator: Option[TokenRangeAccumulator])(implicit spark: SparkSession): Unit = {
    val connector = Connectors.targetConnector(spark.sparkContext.getConf, target)

    val consistencyLevel = target.consistencyLevel match {
      case "LOCAL_QUORUM" => ConsistencyLevel.LOCAL_QUORUM
      case "QUORUM"       => ConsistencyLevel.QUORUM
      case "LOCAL_ONE"    => ConsistencyLevel.LOCAL_ONE
      case "ONE"          => ConsistencyLevel.ONE
      case _              => ConsistencyLevel.LOCAL_QUORUM // Default for Target is LOCAL_QUORUM
    }
    if (consistencyLevel.toString == target.consistencyLevel) {
      log.info(
        s"Using consistencyLevel [${consistencyLevel}] for TARGET based on target config [${target.consistencyLevel}]")
    } else {
      log.info(
        s"Using DEFAULT consistencyLevel [${consistencyLevel}] for TARGET based on unrecognized target config [${target.consistencyLevel}]")
    }

    val tempWriteConf = WriteConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(consistencyLevel = consistencyLevel)

    val writeConf = {
      if (timestampColumns.nonEmpty) {
        tempWriteConf.copy(
          ttl = timestampColumns.map(_.ttl).fold(TTLOption.defaultValue)(TTLOption.perRow),
          timestamp = timestampColumns
            .map(_.writeTime)
            .fold(TimestampOption.defaultValue)(TimestampOption.perRow)
        )
      } else if (target.writeTTLInS.nonEmpty || target.writeWritetimestampInuS.nonEmpty) {
        var hardcodedTempWriteConf = tempWriteConf
        if (target.writeTTLInS.nonEmpty) {
          hardcodedTempWriteConf =
            hardcodedTempWriteConf.copy(ttl = TTLOption.constant(target.writeTTLInS.get))
        }
        if (target.writeWritetimestampInuS.nonEmpty) {
          hardcodedTempWriteConf = hardcodedTempWriteConf.copy(
            timestamp = TimestampOption.constant(target.writeWritetimestampInuS.get))
        }
        hardcodedTempWriteConf
      } else {
        tempWriteConf
      }
    }

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

    val columnSelector = SomeColumns(
      ArraySeq.unsafeWrapArray(renamedSchema.fields.map(_.name: ColumnRef)): _*)

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
        writeConf,
        tokenRangeAccumulator = tokenRangeAccumulator
      )(connector, SqlRowWriter.Factory)
  }

}
