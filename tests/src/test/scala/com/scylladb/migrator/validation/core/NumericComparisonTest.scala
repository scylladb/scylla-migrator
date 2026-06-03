package com.scylladb.migrator.validation.core

import munit.FunSuite

class NumericComparisonTest extends FunSuite {
  import ComparisonResult._

  test("Lenient compares Int and Long of same value as equal") {
    val left = java.lang.Integer.valueOf(42)
    val right = java.lang.Long.valueOf(42L)
    val result = NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.Lenient)
    assertEquals(result, Equal)
  }

  test("Lenient compares Int and Short of same value as equal") {
    val left = java.lang.Integer.valueOf(12)
    val right = java.lang.Short.valueOf(12.toShort)
    val result = NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.Lenient)
    assertEquals(result, Equal)
  }

  test("Lenient compares Int and BigDecimal with zero scale as equal") {
    val left = java.lang.Integer.valueOf(15)
    val right = new java.math.BigDecimal("15.000")
    val result = NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.Lenient)
    assertEquals(result, Equal)
  }

  test("Lenient compares Float and Double within tolerance as equal") {
    val left = java.lang.Float.valueOf(1.23f)
    val right = java.lang.Double.valueOf(1.23)
    val result = NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.Lenient)
    assertEquals(result, Equal)
  }

  test("DetectWiden flags Float and Double with precision/widening loss as TypeMismatch") {
    val left = java.lang.Float.valueOf(0.1f)
    val right = java.lang.Double.valueOf(0.1)
    // Tolerance covers the value difference, but DetectWiden still flags the lossy widening.
    // Use Lenient if tolerance should override type-mismatch detection.
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.01, NumericTypePolicy.DetectWiden)
    assert(result.isInstanceOf[TypeMismatch])
  }

  test("DetectWiden treats Float and Double without loss as Equal") {
    val left = java.lang.Float.valueOf(1.5f)
    val right = java.lang.Double.valueOf(1.5)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.01, NumericTypePolicy.DetectWiden)
    assertEquals(result, Equal)
  }

  test("StrictType flags any Float and Double pair as TypeMismatch") {
    val left = java.lang.Float.valueOf(1.5f)
    val right = java.lang.Double.valueOf(1.5)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.01, NumericTypePolicy.StrictType)
    assert(result.isInstanceOf[TypeMismatch])
  }

  test("StrictType flags Int vs Long as TypeMismatch") {
    val left = java.lang.Integer.valueOf(42)
    val right = java.lang.Long.valueOf(42L)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.StrictType)
    assert(result.isInstanceOf[TypeMismatch])
  }

  test("StrictType allows same-type comparison as Equal") {
    val left = java.lang.Long.valueOf(42L)
    val right = java.lang.Long.valueOf(42L)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.StrictType)
    assertEquals(result, Equal)
  }

  test("DetectWiden does not flag Int vs Long (no widening loss)") {
    val left = java.lang.Integer.valueOf(42)
    val right = java.lang.Long.valueOf(42L)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.001, NumericTypePolicy.DetectWiden)
    assertEquals(result, Equal)
  }

  test("DetectWiden NaN Float vs NaN Double treated as Equal") {
    val left = java.lang.Float.valueOf(Float.NaN)
    val right = java.lang.Double.valueOf(Double.NaN)
    val result =
      NumericComparison.compareWithPolicy(left, right, 0.01, NumericTypePolicy.DetectWiden)
    assertEquals(result, Equal)
  }

  test("NaN and Infinity comparisons") {
    val nanDouble = java.lang.Double.valueOf(Double.NaN)
    val nanFloat = java.lang.Float.valueOf(Float.NaN)
    val infDouble = java.lang.Double.valueOf(Double.PositiveInfinity)
    val infFloat = java.lang.Float.valueOf(Float.PositiveInfinity)
    val negInfDouble = java.lang.Double.valueOf(Double.NegativeInfinity)

    // NaN == NaN
    assertEquals(
      NumericComparison.compareWithPolicy(nanDouble, nanFloat, 0.01, NumericTypePolicy.Lenient),
      Equal
    )
    // +Inf == +Inf
    assertEquals(
      NumericComparison.compareWithPolicy(infDouble, infFloat, 0.01, NumericTypePolicy.Lenient),
      Equal
    )
    // +Inf != -Inf
    assertEquals(
      NumericComparison.compareWithPolicy(infDouble, negInfDouble, 0.01, NumericTypePolicy.Lenient),
      Different
    )
  }

  test("Integrals use exact equality regardless of tolerance") {
    val left = java.lang.Integer.valueOf(5)
    val right = java.lang.Long.valueOf(6L)
    // Both normalize to integral — strict != even when tolerance would cover the gap
    assertEquals(NumericComparison.areNumbersDifferent(left, right, 1.5), true)
    assertEquals(NumericComparison.areNumbersDifferent(left, right, 0.0), true)
  }

  test("Mixed Int vs Double applies tolerance via decimal fallthrough") {
    val left = java.lang.Integer.valueOf(5)
    val right = java.lang.Double.valueOf(6.0)
    // Int is integral, Double is not → falls to decimal path → |5 - 6| = 1 < 1.5
    assertEquals(NumericComparison.areNumbersDifferent(left, right, 1.5), false)
    // With zero tolerance, |5 - 6| = 1 > 0
    assertEquals(NumericComparison.areNumbersDifferent(left, right, 0.0), true)
  }
}
