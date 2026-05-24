package com.scylladb.migrator.validation

import com.datastax.spark.connector.CassandraRow
import com.scylladb.migrator.validation.RowComparisonFailure.{ cassandraRowComparisonFailure, Item }
import com.scylladb.migrator.validation.core.NumericTypePolicy

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
          List(Item.DifferingFieldValues(List("foo")))
        )
      )
    assertEquals(result, expected)
  }

  test("Numerical values within the tolerance threshold") {
    val numericalItem =
      CassandraRow.fromMap(
        Map(
          "foo" -> 123.456,
          "bar" -> 789.012
        )
      )
    val otherNumericalItem =
      CassandraRow.fromMap(
        Map(
          "foo" -> 123.457, // +0.001
          "bar" -> 789.112 // +0.1
        )
      )
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
          List(Item.DifferingFieldValues(List("bar")))
        )
      )
    assertEquals(result, expected)
  }

  test("BigDecimal and integral wrappers are equal under Lenient policy") {
    val left = CassandraRow.fromMap(Map("foo" -> new java.math.BigDecimal("42.0")))
    val right = CassandraRow.fromMap(Map("foo" -> 42L))

    val result = compareItems(left, Some(right))
    assertEquals(result, None)
  }

  test("Float vs Double under different policies") {
    val left = CassandraRow.fromMap(Map("foo" -> 0.1f))
    val right = CassandraRow.fromMap(Map("foo" -> 0.1))

    // Lenient treats them as equal (under default tolerance)
    assertEquals(
      RowComparisonFailure.compareCassandraRows(
        left,
        Some(right),
        0.01,
        1,
        1,
        1,
        true,
        NumericTypePolicy.Lenient
      ),
      None
    )

    // DetectWiden flags lossy widening as TypeMismatch
    assertEquals(
      RowComparisonFailure.compareCassandraRows(
        left,
        Some(right),
        0.01,
        1,
        1,
        1,
        true,
        NumericTypePolicy.DetectWiden
      ),
      Some(
        RowComparisonFailure.cassandraRowComparisonFailure(
          left,
          Some(right),
          List(Item.NumericTypeMismatch(List(("foo", "Float", "Double"))))
        )
      )
    )

    // StrictType flags any Float/Double pair as TypeMismatch
    val leftLossless = CassandraRow.fromMap(Map("foo" -> 1.5f))
    val rightLossless = CassandraRow.fromMap(Map("foo" -> 1.5))
    assertEquals(
      RowComparisonFailure.compareCassandraRows(
        leftLossless,
        Some(rightLossless),
        0.01,
        1,
        1,
        1,
        true,
        NumericTypePolicy.StrictType
      ),
      Some(
        RowComparisonFailure.cassandraRowComparisonFailure(
          leftLossless,
          Some(rightLossless),
          List(Item.NumericTypeMismatch(List(("foo", "Float", "Double"))))
        )
      )
    )
  }

}
