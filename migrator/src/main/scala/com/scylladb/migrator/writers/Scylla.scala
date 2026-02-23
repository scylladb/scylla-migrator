package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.Schema
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ Rename, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LongAccumulator
import com.datastax.oss.driver.api.core.ConsistencyLevel

import scala.collection.immutable.ArraySeq
import java.util.Locale

object Scylla {
  val log: Logger = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  private[writers] case class PrimaryKeyResolution(
    sourcePkNames: Set[String],
    resolvedSourcePkNames: Set[String],
    unresolvedSourcePkNames: Set[String],
    fieldIndices: Array[Int]
  )

  private[writers] def resolvePrimaryKeyColumns(
    targetPkNames: Set[String],
    renames: List[Rename],
    dfSchema: StructType
  ): PrimaryKeyResolution = {
    val reverseRenames = renames.map(r => r.to -> r.from).toMap
    val sourcePkNames =
      targetPkNames.map(name => reverseRenames.getOrElse(name, name))
    val dfFieldNamesLower =
      dfSchema.fieldNames.map(name => name.toLowerCase(Locale.ROOT) -> name).toMap
    val resolution = sourcePkNames.map { sourcePkName =>
      sourcePkName -> dfFieldNamesLower.get(sourcePkName.toLowerCase(Locale.ROOT))
    }

    val resolvedSourcePkNames = resolution.collect {
      case (_, Some(dfFieldName)) =>
        dfFieldName
    }
    val unresolvedSourcePkNames = resolution.collect {
      case (sourcePkName, None) =>
        sourcePkName
    }
    val fieldIndices = resolvedSourcePkNames.map(dfSchema.fieldIndex).toArray

    PrimaryKeyResolution(
      sourcePkNames,
      resolvedSourcePkNames,
      unresolvedSourcePkNames,
      fieldIndices
    )
  }

  private[writers] def requireAllPrimaryKeysResolved(
    targetPkNames: Set[String],
    pkResolution: PrimaryKeyResolution
  ): Unit =
    if (pkResolution.resolvedSourcePkNames.size != targetPkNames.size) {
      val resolved = pkResolution.resolvedSourcePkNames.mkString(", ")
      val unresolved = pkResolution.unresolvedSourcePkNames.mkString(", ")
      throw new IllegalArgumentException(
        s"Cannot resolve all primary key columns from the source DataFrame. " +
          s"Resolved: [${resolved}], unresolved: [${unresolved}]. " +
          s"Check that column names in the source data match the target table, " +
          s"or configure renames accordingly.")
    }

  private[writers] def dropRowsWithNullPrimaryKeys(
    rdd: RDD[Row],
    pkFieldIndices: Array[Int],
    nullPkRowsDropped: LongAccumulator
  ): RDD[Row] =
    rdd.filter { row =>
      val hasNullPk = pkFieldIndices.exists(i => row.isNullAt(i))
      if (hasNullPk) nullPkRowsDropped.add(1L)
      !hasNullPk
    }

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

    // Retrieve the target table schema to identify primary key columns
    val tableDef =
      connector.withSessionDo(Schema.tableFromCassandra(_, target.keyspace, target.table))
    val targetPkNames = tableDef.primaryKey.map(_.columnName).toSet

    val pkResolution = resolvePrimaryKeyColumns(targetPkNames, renames, df.schema)
    pkResolution.unresolvedSourcePkNames.foreach { sourcePkName =>
      log.warn(s"Primary key column '${sourcePkName}' not found in source DataFrame schema")
    }
    requireAllPrimaryKeysResolved(targetPkNames, pkResolution)

    val pkColumnsInDf = pkResolution.resolvedSourcePkNames
    val pkFieldIndices = pkResolution.fieldIndices
    log.info(
      s"Primary key columns in target table: ${targetPkNames.mkString(", ")}; " +
        s"corresponding source columns: ${pkColumnsInDf.mkString(", ")}")

    val nullPkRowsDropped =
      spark.sparkContext.longAccumulator("Rows dropped due to null primary key")

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

    // Filter out rows where any primary key column is null to prevent infinite
    // retries against the target database (see issue #262)
    val filteredRdd = dropRowsWithNullPrimaryKeys(rdd, pkFieldIndices, nullPkRowsDropped)

    filteredRdd
      .saveToCassandra(
        target.keyspace,
        target.table,
        columnSelector,
        writeConf,
        tokenRangeAccumulator = tokenRangeAccumulator
      )(connector, SqlRowWriter.Factory)

    if (nullPkRowsDropped.value > 0) {
      log.warn(
        s"Dropped ${nullPkRowsDropped.value} rows with null primary key values " +
          s"(columns: ${targetPkNames.mkString(", ")})")
    }
  }

}
