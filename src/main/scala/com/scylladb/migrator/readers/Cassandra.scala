package com.scylladb.migrator.readers

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.types.CassandraOption
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, SourceSettings }
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.{ CassandraSQLRow, DataTypeConverter }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ LongType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

object Cassandra {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Cassandra")

  case class Selection(columnRefs: List[ColumnRef],
                       schema: StructType,
                       timestampColumns: Option[TimestampColumns])

  def determineCopyType(tableDef: TableDef,
                        preserveTimesRequest: Boolean): Either[Throwable, CopyType] =
    if (tableDef.columnTypes.exists(_.isCollection) && preserveTimesRequest)
      Left(
        new Exception("TTL/Writetime preservation is unsupported for tables with collection types"))
    else if (preserveTimesRequest && tableDef.regularColumns.nonEmpty)
      Right(CopyType.WithTimestampPreservation)
    else if (preserveTimesRequest && tableDef.regularColumns.isEmpty) {
      log.warn("No regular columns in the table - disabling timestamp preservation")
      Right(CopyType.NoTimestampPreservation)
    } else Right(CopyType.NoTimestampPreservation)

  def createSelection(tableDef: TableDef,
                      origSchema: StructType,
                      preserveTimes: Boolean): Either[Throwable, Selection] =
    determineCopyType(tableDef, preserveTimes).right map {
      case CopyType.WithTimestampPreservation =>
        val columnRefs =
          tableDef.partitionKey.map(_.ref) ++
            tableDef.clusteringColumns.map(_.ref) ++
            tableDef.regularColumns.flatMap { column =>
              val colName = column.columnName

              List(
                column.ref,
                colName.ttl as s"${colName}_ttl",
                colName.writeTime as s"${colName}_writetime"
              )
            }

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))

        val schema = StructType(for {
          origField <- origSchema.fields
          isRegular = tableDef.regularColumns.exists(_.ref.columnName == origField.name)
          field <- if (isRegular)
                    List(
                      origField,
                      StructField(s"${origField.name}_ttl", LongType, true),
                      StructField(s"${origField.name}_writetime", LongType, true))
                  else List(origField)
        } yield field)

        log.info("Schema generated with TTLs and Writetimes:")
        schema.printTreeString()

        Selection(columnRefs.toList, schema, Some(TimestampColumns("ttl", "writetime")))

      case CopyType.NoTimestampPreservation =>
        // We're not using the `tableDef.allColumns` property here in order to generate
        // a schema that is consistent with the timestamp preservation case; the ordering
        // must be (partition keys, clustering keys, regular columns).
        val columnRefs = (tableDef.partitionKey.map(_.ref) ++
          tableDef.clusteringColumns.map(_.ref) ++
          tableDef.regularColumns.map(_.ref)).toList

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))
        log.info("Schema generated:")
        origSchema.printTreeString()

        Selection(columnRefs, origSchema, None)
    }

  def explodeRow(row: Row,
                 schema: StructType,
                 primaryKeyOrdinals: Map[String, Int],
                 regularKeyOrdinals: Map[String, (Int, Int, Int)]) =
    if (regularKeyOrdinals.isEmpty) List(row)
    else
      regularKeyOrdinals
        .flatMap {
          case (fieldName, (ordinal, ttlOrdinal, writetimeOrdinal))
              if !row.isNullAt(writetimeOrdinal) =>
            Some(
              (
                fieldName,
                if (row.isNullAt(ordinal)) CassandraOption.Null
                else CassandraOption.Value(row.get(ordinal)),
                if (row.isNullAt(ttlOrdinal)) None
                else Some(row.getLong(ttlOrdinal)),
                row.getLong(writetimeOrdinal)))

          case _ =>
            None
        }
        .groupBy {
          case (fieldName, value, ttl, writetime) => (ttl, writetime)
        }
        .mapValues(
          _.map {
            case (fieldName, value, _, _) => fieldName -> value
          }.toMap
        )
        .map {
          case ((ttl, writetime), fields) =>
            val newValues = schema.fields.map { field =>
              primaryKeyOrdinals
                .get(field.name)
                .flatMap { ord =>
                  if (row.isNullAt(ord)) None
                  else Some(row.get(ord))
                }
                .getOrElse(fields.getOrElse(field.name, CassandraOption.Unset))
            } ++ Seq(ttl.getOrElse(0L), writetime)

            Row(newValues: _*)
        }

  def indexFields(currentFieldNames: List[String],
                  origFieldNames: List[String],
                  tableDef: TableDef) = {
    val fieldIndices = currentFieldNames.zipWithIndex.toMap
    val primaryKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.primaryKey.exists(_.ref.columnName == origFieldName)
        index <- fieldIndices.get(origFieldName)
      } yield origFieldName -> index).toMap

    val regularKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.regularColumns.exists(_.ref.columnName == origFieldName)
        fieldIndex     <- fieldIndices.get(origFieldName)
        ttlIndex       <- fieldIndices.get(s"${origFieldName}_ttl")
        writetimeIndex <- fieldIndices.get(s"${origFieldName}_writetime")
      } yield origFieldName -> (fieldIndex, ttlIndex, writetimeIndex)).toMap

    (primaryKeyIndices, regularKeyIndices)
  }

  def adjustDataframeForTimestampPreservation(spark: SparkSession,
                                              df: DataFrame,
                                              timestampColumns: Option[TimestampColumns],
                                              origSchema: StructType,
                                              tableDef: TableDef): DataFrame =
    timestampColumns match {
      case None => df
      case Some(TimestampColumns(ttl, writeTime)) =>
        val (primaryKeyOrdinals, regularKeyOrdinals) = indexFields(
          df.schema.fields.map(_.name).toList,
          origSchema.fields.map(_.name).toList,
          tableDef)

        val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
        val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(regularKeyOrdinals)
        val broadcastSchema = spark.sparkContext.broadcast(origSchema)
        val finalSchema = StructType(
          origSchema.fields ++
            Seq(StructField(ttl, LongType, true), StructField(writeTime, LongType, true))
        )

        log.info("Schema that'll be used for writing to Scylla:")
        log.info(finalSchema.treeString)

        df.flatMap {
          explodeRow(
            _,
            broadcastSchema.value,
            broadcastPrimaryKeyOrdinals.value,
            broadcastRegularKeyOrdinals.value)
        }(RowEncoder(finalSchema))

    }

  def readDataframe(spark: SparkSession,
                    source: SourceSettings.Cassandra,
                    preserveTimes: Boolean,
                    tokenRangesToSkip: Set[(Token[_], Token[_])]): SourceDataFrame = {
    val connector = Connectors.sourceConnector(spark.sparkContext.getConf, source)
    val readConf = ReadConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(
        splitCount      = source.splitCount,
        fetchSizeInRows = source.fetchSize
      )

    val tableDef =
      Schema.tableFromCassandra(connector, source.keyspace, source.table)
    log.info("TableDef retrieved for source:")
    log.info(tableDef)

    val origSchema = StructType(tableDef.columns.map(DataTypeConverter.toStructField))
    log.info("Original schema loaded:")
    origSchema.printTreeString()

    val selection = createSelection(tableDef, origSchema, preserveTimes).fold(throw _, identity)

    val rdd = spark.sparkContext
      .cassandraTable[CassandraSQLRow](
        source.keyspace,
        source.table,
        (s, e) => !tokenRangesToSkip.contains((s, e)))
      .withConnector(connector)
      .withReadConf(readConf)
      .select(selection.columnRefs: _*)
      .asInstanceOf[RDD[Row]]

    val resultingDataframe = adjustDataframeForTimestampPreservation(
      spark,
      // spark.createDataFrame does something weird with the encoder (tries to convert the row again),
      // so it's important to use createDataset with an explciit encoder instead here
      spark.createDataset(rdd)(RowEncoder(selection.schema)),
      selection.timestampColumns,
      origSchema,
      tableDef
    )

    SourceDataFrame(resultingDataframe, selection.timestampColumns, true)
  }
}
