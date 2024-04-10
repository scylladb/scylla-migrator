package com.scylladb.migrator.validation

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.scylladb.migrator.AttributeValueUtils.{ numericalValue, stringValue }
import com.scylladb.migrator.validation.RowComparisonFailure.{ dynamoDBRowComparisonFailure, Item }

class RowComparisonTest extends munit.FunSuite {

  val sameColumns: String => String = identity
  val floatingPointTolerance: Double = 0.01
  val item: Map[String, AttributeValue] = Map("foo" -> stringValue("bar"))

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
    val renamedItem = Map("quux" -> stringValue("bar"))
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
    val otherItem = Map("baz" -> stringValue("bah"))
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
    val otherItem = Map("foo" -> stringValue("boom"))
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

  test("Numerical values within the tolerance threshold") {
    val numericalItem =
      Map(
        "foo" -> numericalValue("123.456"),
        "bar" -> numericalValue("789.012")
      )
    val otherNumericalItem =
      Map(
        "foo" -> numericalValue("123.457"), // +0.001
        "bar" -> numericalValue("789.112") // +0.1
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
