package com.scylladb.migrator.benchmarks.util

import com.datastax.spark.connector.cql.{ ColumnDef, PartitionKeyColumn, RegularColumn, TableDef }
import com.datastax.spark.connector.types.{ BigIntType, CassandraOption, IntType, VarCharType }
import org.apache.spark.sql.types.{ IntegerType, LongType, StringType, StructField, StructType }
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.immutable.ArraySeq

object BenchmarkFixtures {

  /** Simple flat schema: (id TEXT PK, col1 TEXT, col2 INT, col3 BIGINT) */
  val simpleSchema: StructType = StructType(
    Seq(
      StructField("id", StringType, nullable    = false),
      StructField("col1", StringType, nullable  = true),
      StructField("col2", IntegerType, nullable = true),
      StructField("col3", LongType, nullable    = true)
    )
  )

  /** Schema with TTL/writetime columns appended for each regular column */
  val timestampSchema: StructType = StructType(
    Seq(
      StructField("id", StringType, nullable           = false),
      StructField("col1", StringType, nullable         = true),
      StructField("col1_ttl", IntegerType, nullable    = true),
      StructField("col1_writetime", LongType, nullable = true),
      StructField("col2", IntegerType, nullable        = true),
      StructField("col2_ttl", IntegerType, nullable    = true),
      StructField("col2_writetime", LongType, nullable = true),
      StructField("col3", LongType, nullable           = true),
      StructField("col3_ttl", IntegerType, nullable    = true),
      StructField("col3_writetime", LongType, nullable = true)
    )
  )

  val simpleTableDef: TableDef = TableDef(
    keyspaceName      = "bench",
    tableName         = "simple",
    partitionKey      = Seq(ColumnDef("id", PartitionKeyColumn, VarCharType)),
    clusteringColumns = Seq.empty,
    regularColumns = Seq(
      ColumnDef("col1", RegularColumn, VarCharType),
      ColumnDef("col2", RegularColumn, IntType),
      ColumnDef("col3", RegularColumn, BigIntType)
    )
  )

  val primaryKeyOrdinals: Map[String, Int] = Map("id" -> 0)

  val regularKeyOrdinals: Map[String, (Int, Int, Int)] = Map(
    "col1" -> (1, 2, 3),
    "col2" -> (4, 5, 6),
    "col3" -> (7, 8, 9)
  )

  /** Generate a simple Row with String values (post-conversion) */
  def makeSimpleRow(i: Int): Row =
    Row(s"id-$i", s"value-$i", i, i.toLong * 1000L)

  /** Generate a Row with UTF8String values (pre-conversion, as from Cassandra connector RDD) */
  def makeUTF8Row(i: Int): Row =
    Row(UTF8String.fromString(s"id-$i"), UTF8String.fromString(s"value-$i"), i, i.toLong * 1000L)

  /** Generate a Row with TTL/writetime columns (for explodeRow benchmarks) */
  def makeTimestampRow(i: Int): Row = {
    val writetime = System.currentTimeMillis() * 1000L
    Row(
      s"id-$i",
      s"value-$i",
      3600,
      writetime,
      i,
      3600,
      writetime,
      i.toLong * 1000L,
      3600,
      writetime
    )
  }

  /** Generate a Row where all regular columns share the same timestamp (single-group path) */
  def makeSingleTimestampRow(i: Int): Row = {
    val writetime = 1000000L
    val ttl = 3600
    Row(
      s"id-$i",
      s"value-$i",
      ttl,
      writetime,
      i,
      ttl,
      writetime,
      i.toLong * 1000L,
      ttl,
      writetime
    )
  }

  /** Generate a Row where regular columns have different timestamps (multi-group path) */
  def makeMultiTimestampRow(i: Int): Row =
    Row(
      s"id-$i",
      s"value-$i",
      3600,
      1000000L,
      i,
      7200,
      2000000L,
      i.toLong * 1000L,
      null,
      null
    )
}
