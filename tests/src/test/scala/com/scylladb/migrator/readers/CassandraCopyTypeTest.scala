package com.scylladb.migrator.readers

import com.datastax.spark.connector.cql.{ ColumnDef, PartitionKeyColumn, RegularColumn, TableDef }
import com.datastax.spark.connector.types.{ BigIntType, CounterType, ListType, TextType }
import com.scylladb.migrator.config.CopyType

class CassandraCopyTypeTest extends munit.FunSuite {

  private def makeTableDef(regularColumns: Seq[ColumnDef]): TableDef =
    TableDef(
      "test_ks",
      "test_table",
      partitionKey      = Seq(ColumnDef("id", PartitionKeyColumn, TextType)),
      clusteringColumns = Seq.empty,
      regularColumns    = regularColumns
    )

  test("counter table with preserveTimestamps=true disables timestamp preservation") {
    val tableDef = makeTableDef(
      Seq(
        ColumnDef("counter1", RegularColumn, CounterType),
        ColumnDef("counter2", RegularColumn, CounterType)
      )
    )
    val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
    assertEquals(result, Right(CopyType.NoTimestampPreservation))
  }

  test("counter table with preserveTimestamps=false returns NoTimestampPreservation") {
    val tableDef = makeTableDef(
      Seq(
        ColumnDef("counter1", RegularColumn, CounterType)
      )
    )
    val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = false)
    assertEquals(result, Right(CopyType.NoTimestampPreservation))
  }

  test("non-counter table with preserveTimestamps=true returns WithTimestampPreservation") {
    val tableDef = makeTableDef(
      Seq(
        ColumnDef("value", RegularColumn, BigIntType)
      )
    )
    val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
    assertEquals(result, Right(CopyType.WithTimestampPreservation))
  }

  test("collection table with preserveTimestamps=true returns error") {
    val tableDef = makeTableDef(
      Seq(
        ColumnDef("items", RegularColumn, ListType(TextType))
      )
    )
    val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
    assert(result.isLeft)
  }

  test("hasCounterColumns detects counter columns") {
    val withCounters = makeTableDef(
      Seq(
        ColumnDef("counter1", RegularColumn, CounterType),
        ColumnDef("value", RegularColumn, BigIntType)
      )
    )
    assert(Cassandra.hasCounterColumns(withCounters))

    val withoutCounters = makeTableDef(
      Seq(
        ColumnDef("value", RegularColumn, BigIntType)
      )
    )
    assert(!Cassandra.hasCounterColumns(withoutCounters))
  }
}
