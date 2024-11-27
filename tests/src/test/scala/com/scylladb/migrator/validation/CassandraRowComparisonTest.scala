package com.scylladb.migrator.validation

import com.datastax.spark.connector.CassandraRow
import com.scylladb.migrator.validation.RowComparisonFailure.{Item, cassandraRowComparisonFailure}

class CassandraRowComparisonTest extends munit.FunSuite {

  val item: CassandraRow = CassandraRow.fromMap(Map("foo" -> "bar"))

  def compareItems(
    item: CassandraRow,
    maybeOther: Option[CassandraRow],
    floatingPointTolerance: Double = 0.0001,
    timestampMsTolerance: Long = 1L,
    ttlToleranceMillis: Long = 1L,
    writetimeToleranceMillis: Long = 1L,
    compareTimestamps: Boolean = true
  ): Option[RowComparisonFailure] =
    RowComparisonFailure.compareCassandraRows(
      item,
      maybeOther,
      floatingPointTolerance,
      timestampMsTolerance,
      ttlToleranceMillis,
      writetimeToleranceMillis,
      compareTimestamps
    )

  test("No difference") {
    val result = compareItems(item, Some(item))
    assertEquals(result, None)
  }

  test("Missing row") {
    val result = compareItems(item, None)
    val expected =
      Some(cassandraRowComparisonFailure(item, None, List(Item.MissingTargetRow)))
    assertEquals(result, expected)
  }

  test("Missing column") {
    val otherItem = CassandraRow.fromMap(Map.empty)
    val result =
      compareItems(item, Some(otherItem))
    val expected =
      Some(cassandraRowComparisonFailure(item, Some(otherItem), List(Item.MismatchedColumnCount)))
    assertEquals(result, expected)
  }

  test("Misspelled column") {
    val otherItem = CassandraRow.fromMap(Map("baz" -> "bah"))
    val result = compareItems(item, Some(otherItem))
    val expected =
      Some(cassandraRowComparisonFailure(item, Some(otherItem), List(Item.MismatchedColumnNames)))
    assertEquals(result, expected)
  }

  test("Incorrect value") {
    val otherItem = CassandraRow.fromMap(Map("foo" -> "boom"))
    val result = compareItems(item, Some(otherItem))
    val expected =
      Some(
        cassandraRowComparisonFailure(
          item,
          Some(otherItem),
          List(Item.DifferingFieldValues(List("foo")))))
    assertEquals(result, expected)
  }

  test("Numerical values within the tolerance threshold") {
    val numericalItem =
      CassandraRow.fromMap(
        Map(
          "foo" -> 123.456,
          "bar" -> 789.012
        ))
    val otherNumericalItem =
      CassandraRow.fromMap(
        Map(
          "foo" -> 123.457, // +0.001
          "bar" -> 789.112 // +0.1
        ))
    val result =
      compareItems(
        numericalItem,
        Some(otherNumericalItem),
        floatingPointTolerance = 0.01
      )
    // Only the field `bar` is reported to be different because `foo` is still within the tolerance threshold
    val expected =
      Some(
        cassandraRowComparisonFailure(
          numericalItem,
          Some(otherNumericalItem),
          List(Item.DifferingFieldValues(List("bar")))))
    assertEquals(result, expected)
  }

}
