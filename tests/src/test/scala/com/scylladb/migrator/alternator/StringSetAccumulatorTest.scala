package com.scylladb.migrator.alternator

class StringSetAccumulatorTest extends munit.FunSuite {
  test("StringSetAccumulator basic functionality") {
    val accumulator = StringSetAccumulator()

    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Set.empty[String])

    accumulator.add("file1.parquet")
    accumulator.add("file2.parquet")

    assertEquals(accumulator.value, Set("file1.parquet", "file2.parquet"))
    assertEquals(accumulator.isZero, false)

    val copy = accumulator.copy()
    assertEquals(copy.value, accumulator.value)

    accumulator.reset()
    assertEquals(accumulator.isZero, true)
    assertEquals(accumulator.value, Set.empty[String])
  }

  test("StringSetAccumulator merge functionality") {
    val accumulator1 = StringSetAccumulator(Set("file1.parquet"))
    val accumulator2 = StringSetAccumulator(Set("file2.parquet", "file3.parquet"))

    accumulator1.merge(accumulator2)
    assertEquals(accumulator1.value, Set("file1.parquet", "file2.parquet", "file3.parquet"))
  }
}
