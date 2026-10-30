package com.scylladb.migrator.readers

import com.datastax.spark.connector.cql.{ ColumnDef, PartitionKeyColumn }
import com.datastax.spark.connector.types.{
  BlobType,
  ColumnType,
  DateType,
  IntType,
  TextType,
  TimeType,
  TimestampType,
  TupleType,
  UUIDType
}

class SubscriptPkBinderTest extends munit.FunSuite {

  private def binder(types: ColumnType[_]*): Cassandra.PkBinder =
    Cassandra
      .pkBinder(types.zipWithIndex.map { case (t, i) => ColumnDef(s"k$i", PartitionKeyColumn, t) })
      .fold(reason => fail(reason), identity)

  test("binds the migration reader's decoded key values") {
    val bound = binder(UUIDType, TimestampType, BlobType, DateType)
      .bind(
        Seq(
          "8f2a6c1e-3b7d-4a52-9c0e-1d4f6a8b2c39",
          1709637753000L,
          Array[Byte](1, 2),
          new java.sql.Date(0L)
        )
      )
    assertEquals(bound(0), java.util.UUID.fromString("8f2a6c1e-3b7d-4a52-9c0e-1d4f6a8b2c39"))
    assertEquals(bound(1), java.time.Instant.ofEpochMilli(1709637753000L))
    assertEquals(bound(2), java.nio.ByteBuffer.wrap(Array[Byte](1, 2)))
    assertEquals(bound(3), new java.sql.Date(0L).toLocalDate)
  }

  test("binds the driver types kept by the validator's rows") {
    val uuid = java.util.UUID.randomUUID()
    val instant = java.time.Instant.ofEpochMilli(1709637753000L)
    val date = java.time.LocalDate.of(2024, 3, 5)
    val time = java.time.LocalTime.of(11, 22, 33)
    val blob = java.nio.ByteBuffer.wrap(Array[Byte](3))
    val bound = binder(UUIDType, TimestampType, DateType, TimeType, BlobType, TextType, IntType)
      .bind(Seq(uuid, instant, date, time, blob, "a", Integer.valueOf(7)))
    assertEquals(bound, Seq[AnyRef](uuid, instant, date, time, blob, "a", Integer.valueOf(7)))
    assertEquals(
      binder(TimestampType).bind(Seq(java.util.Date.from(instant))),
      Seq[AnyRef](instant)
    )
  }

  test("reports key types that cannot be bound") {
    val reason = Cassandra.pkBinder(
      Seq(ColumnDef("k", PartitionKeyColumn, TupleType(Nil: _*)))
    )
    assert(reason.isLeft, s"expected a tuple key to be rejected, got $reason")
  }
}
