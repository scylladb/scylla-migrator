package com.scylladb.migrator.validation

import com.scylladb.migrator.alternator.DdbValue
import com.scylladb.migrator.validation.RowComparisonFailure.{Item, dynamoDBRowComparisonFailure}
import software.amazon.awssdk.core.SdkBytes

class DynamoDBRowComparisonTest extends munit.FunSuite {

  val sameColumns: String => String = identity
  val floatingPointTolerance: Double = 0.01
  val item: Map[String, DdbValue] = Map("foo" -> DdbValue.S("bar"))

  test("No difference") {
    val result = RowComparisonFailure.compareDynamoDBRows(
      item,
      Some(item),
      sameColumns,
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("No difference with renamed column") {
    // Same as `item` but with column `foo` renamed to `quux`
    val renamedItem = Map("quux" -> DdbValue.S("bar"))
    val result = RowComparisonFailure.compareDynamoDBRows(
      item,
      Some(renamedItem),
      Map("foo" -> "quux").withDefault(identity),
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("Missing row") {
    val result =
      RowComparisonFailure.compareDynamoDBRows(
        item,
        None,
        sameColumns,
        floatingPointTolerance
      )
    val expected =
      Some(dynamoDBRowComparisonFailure(item, None, List(Item.MissingTargetRow)))
    assertEquals(result, expected)
  }

  test("Missing column") {
    val result =
      RowComparisonFailure.compareDynamoDBRows(
        item,
        Some(Map.empty),
        sameColumns,
        floatingPointTolerance
      )
    val expected =
      Some(dynamoDBRowComparisonFailure(item, Some(Map.empty), List(Item.MismatchedColumnCount)))
    assertEquals(result, expected)
  }

  test("Misspelled column") {
    val otherItem = Map("baz" -> DdbValue.S("bah"))
    val result =
      RowComparisonFailure.compareDynamoDBRows(
        item,
        Some(otherItem),
        sameColumns,
        floatingPointTolerance
      )
    val expected =
      Some(dynamoDBRowComparisonFailure(item, Some(otherItem), List(Item.MismatchedColumnNames)))
    assertEquals(result, expected)
  }

  test("Incorrect value") {
    val otherItem = Map("foo" -> DdbValue.S("boom"))
    val result =
      RowComparisonFailure.compareDynamoDBRows(
        item,
        Some(otherItem),
        sameColumns,
        floatingPointTolerance
      )
    val expected =
      Some(
        dynamoDBRowComparisonFailure(
          item,
          Some(otherItem),
          List(Item.DifferingFieldValues(List("foo")))))
    assertEquals(result, expected)
  }

  test("String sets with different order are equal") {
    val source = Map("foo" -> DdbValue.Ss(Seq("a", "b", "c")))
    val target = Map("foo" -> DdbValue.Ss(Seq("c", "a", "b")))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("Number sets with different order are equal") {
    val source = Map("foo" -> DdbValue.Ns(Seq("1", "2", "3")))
    val target = Map("foo" -> DdbValue.Ns(Seq("3", "1", "2")))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("Number sets with different order are equal within tolerance") {
    val source = Map("foo" -> DdbValue.Ns(Seq("1.001", "2.002")))
    val target = Map("foo" -> DdbValue.Ns(Seq("2.003", "1.002")))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("Binary sets with different order are equal") {
    val b1 = SdkBytes.fromUtf8String("alpha")
    val b2 = SdkBytes.fromUtf8String("beta")
    val b3 = SdkBytes.fromUtf8String("gamma")
    val source = Map("foo" -> DdbValue.Bs(Seq(b1, b2, b3)))
    val target = Map("foo" -> DdbValue.Bs(Seq(b3, b1, b2)))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    assertEquals(result, None)
  }

  test("String sets with different elements are different") {
    val source = Map("foo" -> DdbValue.Ss(Seq("a", "b", "c")))
    val target = Map("foo" -> DdbValue.Ss(Seq("a", "b", "d")))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    val expected = Some(
      dynamoDBRowComparisonFailure(
        source,
        Some(target),
        List(Item.DifferingFieldValues(List("foo")))))
    assertEquals(result, expected)
  }

  test("Number sets with different elements are different") {
    val source = Map("foo" -> DdbValue.Ns(Seq("1", "2", "3")))
    val target = Map("foo" -> DdbValue.Ns(Seq("1", "2", "4")))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    val expected = Some(
      dynamoDBRowComparisonFailure(
        source,
        Some(target),
        List(Item.DifferingFieldValues(List("foo")))))
    assertEquals(result, expected)
  }

  test("Binary sets with different elements are different") {
    val b1 = SdkBytes.fromUtf8String("alpha")
    val b2 = SdkBytes.fromUtf8String("beta")
    val b3 = SdkBytes.fromUtf8String("gamma")
    val source = Map("foo" -> DdbValue.Bs(Seq(b1, b2)))
    val target = Map("foo" -> DdbValue.Bs(Seq(b1, b3)))
    val result = RowComparisonFailure.compareDynamoDBRows(
      source,
      Some(target),
      sameColumns,
      floatingPointTolerance
    )
    val expected = Some(
      dynamoDBRowComparisonFailure(
        source,
        Some(target),
        List(Item.DifferingFieldValues(List("foo")))))
    assertEquals(result, expected)
  }

  test("Numerical values within the tolerance threshold") {
    val numericalItem =
      Map(
        "foo" -> DdbValue.N("123.456"),
        "bar" -> DdbValue.N("789.012")
      )
    val otherNumericalItem =
      Map(
        "foo" -> DdbValue.N("123.457"), // +0.001
        "bar" -> DdbValue.N("789.112") // +0.1
      )
    val result =
      RowComparisonFailure.compareDynamoDBRows(
        numericalItem,
        Some(otherNumericalItem),
        sameColumns,
        floatingPointTolerance
      )
    // Only the field `bar` is reported to be different because `foo` is still within the tolerance threshold
    val expected =
      Some(
        dynamoDBRowComparisonFailure(
          numericalItem,
          Some(otherNumericalItem),
          List(Item.DifferingFieldValues(List("bar")))))
    assertEquals(result, expected)
  }

}
