package com.scylladb.migrator.validation

class BoundedListAccumulatorTest extends munit.FunSuite {
  test("BoundedListAccumulator basic functionality") {
    val accumulator = BoundedListAccumulator[String](maxSize = 3)

    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Nil)

    accumulator.add("a")
    accumulator.add("b")

    assertEquals(accumulator.value, List("a", "b"))
    assertEquals(accumulator.isZero, false)

    val copy = accumulator.copy()
    assertEquals(copy.value, accumulator.value)

    accumulator.reset()
    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Nil)
  }

  test("BoundedListAccumulator caps growth on add") {
    val accumulator = BoundedListAccumulator[Int](maxSize = 3)

    (1 to 5).foreach(accumulator.add)

    assertEquals(accumulator.value.size, 3)
    assertEquals(accumulator.value, List(1, 2, 3))
  }

  test("BoundedListAccumulator caps growth on merge") {
    val accumulator1 = BoundedListAccumulator[Int](maxSize = 4, initialValue = List(1, 2, 3))
    val accumulator2 = BoundedListAccumulator[Int](maxSize = 4, initialValue = List(4, 5, 6))

    accumulator1.merge(accumulator2)

    assertEquals(accumulator1.value.size, 4)
    assertEquals(accumulator1.value, List(1, 2, 3, 4))
  }

  test("BoundedListAccumulator with maxSize = 0 stays empty") {
    val accumulator = BoundedListAccumulator[String](maxSize = 0)

    accumulator.add("a")

    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Nil)
  }

  test("BoundedListAccumulator rejects a negative maxSize") {
    intercept[IllegalArgumentException] {
      BoundedListAccumulator[String](maxSize = -1)
    }
  }
}
