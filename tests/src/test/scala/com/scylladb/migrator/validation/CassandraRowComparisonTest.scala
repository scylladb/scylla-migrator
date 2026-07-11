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

  test("Per-element collection WRITETIMEs match when element-aligned lists are equal") {
    val left = CassandraRow.fromMap(
      Map(
        "id"             -> "r1",
        "tags"           -> Set(10, 20, 30),
        "tags_ttl"       -> List(0, 0, 0),
        "tags_writetime" -> List(1000L, 2000L, 3000L)
      )
    )
    val right = CassandraRow.fromMap(
      Map(
        "id"             -> "r1",
        "tags"           -> Set(10, 20, 30),
        "tags_ttl"       -> List(0, 0, 0),
        "tags_writetime" -> List(1000L, 2000L, 3000L)
      )
    )
    assertEquals(compareItems(left, Some(right)), None)
  }

  test("Per-element collection WRITETIME mismatch is reported for the differing column") {
    val left = CassandraRow.fromMap(
      Map(
        "id"             -> "r1",
        "tags"           -> Set(10, 20, 30),
        "tags_writetime" -> List(1000L, 2000L, 3000L)
      )
    )
    val right = CassandraRow.fromMap(
      Map(
        "id"             -> "r1",
        "tags"           -> Set(10, 20, 30),
        "tags_writetime" -> List(1000L, 2000L, 9999L) // last element differs beyond tolerance
      )
    )
    val result = compareItems(left, Some(right), writetimeToleranceMillis = 0L)
    assert(
      result.exists(_.items.exists(_.isInstanceOf[Item.DifferingWritetimes])),
      s"expected a DifferingWritetimes failure, got ${result}"
    )
  }

  test("Per-element collection metadata length mismatch is reported as a cardinality mismatch") {
    val left = CassandraRow.fromMap(
      Map("id" -> "r1", "tags_writetime" -> List(1000L, 2000L, 3000L))
    )
    val right = CassandraRow.fromMap(
      Map("id" -> "r1", "tags_writetime" -> List(1000L, 2000L))
    )
    val result = compareItems(left, Some(right))
    assert(
      result.exists(_.items.exists(_.isInstanceOf[Item.MetadataCardinalityMismatch])),
      s"expected a MetadataCardinalityMismatch failure for length mismatch, got ${result}"
    )
    // A length mismatch is structural, not a time delta, so it must NOT be a DifferingWritetimes.
    assert(
      !result.exists(_.items.exists(_.isInstanceOf[Item.DifferingWritetimes])),
      s"length mismatch should not be reported as DifferingWritetimes, got ${result}"
    )
  }

  test("Malformed per-element metadata is reported as MalformedMetadata, not a time delta") {
    val left = CassandraRow.fromMap(
      Map("id" -> "r1", "tags_writetime" -> List(1000L, "oops", 3000L))
    )
    val right = CassandraRow.fromMap(
      Map("id" -> "r1", "tags_writetime" -> List(1000L, 2000L, 3000L))
    )
    val result = compareItems(left, Some(right))
    assert(
      result.exists(_.items.exists(_.isInstanceOf[Item.MalformedMetadata])),
      s"expected a MalformedMetadata failure, got ${result}"
    )
    assert(
      !result.exists(_.items.exists(_.isInstanceOf[Item.DifferingWritetimes])),
      s"malformed metadata should not be reported as DifferingWritetimes, got ${result}"
    )
  }

  test("Per-element metadata vs collection cardinality mismatch is reported") {
    val left = CassandraRow.fromMap(
      Map("id" -> "r1", "tags" -> Set(10, 20, 30), "tags_writetime" -> List(1000L, 2000L))
    )
    val right = CassandraRow.fromMap(
      Map("id" -> "r1", "tags" -> Set(10, 20, 30), "tags_writetime" -> List(1000L, 2000L))
    )
    val result = compareItems(left, Some(right))
    assert(
      result.exists(_.items.exists(_.isInstanceOf[Item.MetadataCardinalityMismatch])),
      s"expected a MetadataCardinalityMismatch failure (2 writetimes for 3 elements), got ${result}"
    )
  }

  test("Per-element metadata comparison skipped when compareTimestamps is false") {
    val left = CassandraRow.fromMap(
      Map("id" -> "r1", "tags" -> Set(1), "tags_writetime" -> List(1000L))
    )
    val right = CassandraRow.fromMap(
      Map("id" -> "r1", "tags" -> Set(1), "tags_writetime" -> List(9999L))
    )
    assertEquals(compareItems(left, Some(right), compareTimestamps = false), None)
  }

  test("metadataAsLongs handles scalar, list, and null metadata") {
    val row = CassandraRow.fromMap(
      Map(
        "scalar_writetime" -> 1234L,
        "list_writetime"   -> List(1L, 2L, 3L),
        "null_writetime"   -> null
      )
    )
    assertEquals(RowComparisonFailure.metadataAsLongs(row, "scalar_writetime"), Some(Seq(1234L)))
    assertEquals(RowComparisonFailure.metadataAsLongs(row, "list_writetime"), Some(Seq(1L, 2L, 3L)))
    // A null metadata value (e.g. empty/null collection) yields None.
    assertEquals(RowComparisonFailure.metadataAsLongs(row, "null_writetime"), None)
  }

  test("Direct areDifferent flags Float(1.5f) vs Double(1.5d) before hash comparison") {
    val floatVal: Option[Any] = Some(java.lang.Float.valueOf(1.5f))
    val doubleVal: Option[Any] = Some(java.lang.Double.valueOf(1.5))

    // StrictType: flags type mismatch even though values are numerically equal
    assertEquals(
      RowComparisonFailure
        .areDifferent(floatVal, doubleVal, 0L, 0.01, NumericTypePolicy.StrictType),
      true
    )

    // DetectWiden: 1.5f widens losslessly to 1.5d, so NOT flagged
    assertEquals(
      RowComparisonFailure.areDifferent(
        floatVal,
        doubleVal,
        0L,
        0.01,
        NumericTypePolicy.DetectWiden
      ),
      false
    )

    // DetectWiden with lossy widening (0.1f → 0.1d loses precision): flagged even with tolerance
    val lossyFloat: Option[Any] = Some(java.lang.Float.valueOf(0.1f))
    val lossyDouble: Option[Any] = Some(java.lang.Double.valueOf(0.1))
    assertEquals(
      RowComparisonFailure.areDifferent(
        lossyFloat,
        lossyDouble,
        0L,
        0.01,
        NumericTypePolicy.DetectWiden
      ),
      true
    )

    // Lenient: never flags type differences
    assertEquals(
      RowComparisonFailure.areDifferent(floatVal, doubleVal, 0L, 0.01, NumericTypePolicy.Lenient),
      false
    )
  }

}
