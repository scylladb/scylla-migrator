package com.scylladb.migrator.validation

class PartitionKeyedAccumulatorTest extends munit.FunSuite {

  test("starts zero/empty") {
    val acc = new PartitionKeyedAccumulator[String]()
    assertEquals(acc.isZero, true)
    assertEquals(acc.value, Map.empty[Int, (Int, String)])
    assertEquals(acc.values.toList, Nil)
  }

  test("a higher stageAttemptNumber overwrites a lower one for the same partitionId") {
    val acc = new PartitionKeyedAccumulator[String]()
    acc.add((0, 0, "attempt-0"))
    acc.add((0, 1, "attempt-1"))
    assertEquals(acc.values.toList, List("attempt-1"))
    assertEquals(acc.value(0), (1, "attempt-1"))
  }

  test("a lower stageAttemptNumber arriving after a higher one is ignored (order-independent)") {
    val acc = new PartitionKeyedAccumulator[String]()
    acc.add((0, 1, "attempt-1"))
    acc.add((0, 0, "stale retry arriving late"))
    assertEquals(acc.values.toList, List("attempt-1"))
  }

  test("an equal stageAttemptNumber is not summed in (e.g. speculative duplicate)") {
    val acc = new PartitionKeyedAccumulator[String]()
    acc.add((0, 0, "first"))
    acc.add((0, 0, "speculative duplicate"))
    assertEquals(acc.values.size, 1)
  }

  test("different partitionIds coexist independently") {
    val acc = new PartitionKeyedAccumulator[Int]()
    acc.add((0, 0, 10))
    acc.add((1, 0, 20))
    acc.add((2, 3, 30))
    assertEquals(acc.values.toSet, Set(10, 20, 30))
  }

  test("merge resolves per-partition winners across two accumulator instances") {
    val acc1 = new PartitionKeyedAccumulator[String]()
    acc1.add((0, 0, "acc1-p0-attempt0"))
    acc1.add((1, 2, "acc1-p1-attempt2"))

    val acc2 = new PartitionKeyedAccumulator[String]()
    acc2.add((0, 1, "acc2-p0-attempt1")) // newer attempt for partition 0
    acc2.add((1, 1, "acc2-p1-attempt1")) // stale attempt for partition 1
    acc2.add((2, 0, "acc2-p2-attempt0")) // new partition

    acc1.merge(acc2)

    assertEquals(acc1.value(0), (1, "acc2-p0-attempt1"))
    assertEquals(acc1.value(1), (2, "acc1-p1-attempt2"))
    assertEquals(acc1.value(2), (0, "acc2-p2-attempt0"))
  }

  test("copy produces an independent snapshot") {
    val acc = new PartitionKeyedAccumulator[String]()
    acc.add((0, 0, "a"))
    val copy = acc.copy()
    acc.add((1, 0, "b"))
    assertEquals(copy.values.toList, List("a"))
    assertEquals(acc.values.toSet, Set("a", "b"))
  }

  test("reset clears all partitions") {
    val acc = new PartitionKeyedAccumulator[String]()
    acc.add((0, 0, "a"))
    acc.reset()
    assertEquals(acc.isZero, true)
    assertEquals(acc.values.toList, Nil)
  }
}

class PartitionRepairSummaryTest extends munit.FunSuite {
  test("combine sums counts and concatenates+caps the failure sample") {
    val a = PartitionRepairSummary(2L, List.fill(2)(sampleFailure("a")))
    val b = PartitionRepairSummary(3L, List.fill(2)(sampleFailure("b")))

    val combined = PartitionRepairSummary.combine(List(a, b), failuresToFetch = 3)

    assertEquals(combined.missingCount, 5L)
    assertEquals(combined.failureSample.size, 3)
  }

  test("combine with no summaries yields zero count and empty sample") {
    val combined = PartitionRepairSummary.combine(Nil, failuresToFetch = 3)

    assertEquals(combined.missingCount, 0L)
    assertEquals(combined.failureSample, Nil)
  }

  private def sampleFailure(tag: String): RowComparisonFailure =
    RowComparisonFailure(tag, None, List(RowComparisonFailure.Item.MissingTargetRow))
}
