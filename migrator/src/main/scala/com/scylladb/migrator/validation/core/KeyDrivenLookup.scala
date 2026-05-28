package com.scylladb.migrator.validation.core

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ CassandraConnector, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.readers
import com.scylladb.migrator.config.TargetSettings
import org.apache.spark.sql.cassandra.{ CassandraSQLRow, DataTypeConverter }
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types.StructType

object KeyDrivenLookup {

  def resolveTargetColumns(
    targetTableDef: TableDef,
    columns: Seq[String]
  ): Seq[String] = {
    val targetFields = targetTableDef.columns.map(_.columnName).toArray
    columns.map(SchemaResolver.resolveFieldName(targetFields, _))
  }

  def buildTargetSchema(
    targetTableDef: TableDef,
    selectedColumns: Seq[String]
  ): StructType =
    StructType(
      selectedColumns.map { columnName =>
        DataTypeConverter.toStructField(targetTableDef.columnByName(columnName))
      }
    )

  def lookupTargetRowsForSourceKeys(
    spark: SparkSession,
    sourceDF: DataFrame,
    targetConnector: CassandraConnector,
    targetTableDef: TableDef,
    targetSettings: TargetSettings.Scylla,
    primaryKeyColumns: Seq[String],
    selectedColumns: Seq[String],
    readConf: ReadConf
  ): DataFrame = {
    val sourceSchemaFields = sourceDF.schema.fieldNames
    val resolvedSourcePK =
      primaryKeyColumns.map(SchemaResolver.resolveFieldName(sourceSchemaFields, _))
    val resolvedTargetPK = resolveTargetColumns(targetTableDef, primaryKeyColumns)
    val resolvedSelectedColumns = resolveTargetColumns(targetTableDef, selectedColumns)
    val schema = buildTargetSchema(targetTableDef, resolvedSelectedColumns)
    val targetRows = sourceDF
      .select(resolvedSourcePK.map(SchemaResolver.sparkColumn): _*)
      .distinct()
      .rdd
      .leftJoinWithCassandraTable[CassandraSQLRow](
        targetSettings.keyspace,
        targetSettings.table,
        SomeColumns(resolvedSelectedColumns.map(ColumnName(_)): _*),
        SomeColumns(resolvedTargetPK.map(ColumnName(_)): _*),
        readConf
      )
      .withConnector(targetConnector)
      .flatMap(_._2)
      .map(row => Row.fromSeq(row.toSeq.map(readers.Cassandra.convertValue)))
    spark.createDataFrame(targetRows, schema)
  }
}
