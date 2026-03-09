package com.scylladb.migrator.alternator

import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.atomic.AtomicReference

/** A Spark Accumulator that accumulates `Int` values into a `Set[Int]`.
  *
  * We use it to track the indexes of the DynamoDB scan segments that have been migrated to the
  * target database.
  */
class IntSetAccumulator(init: Set[Int]) extends AccumulatorV2[Int, Set[Int]] {

  private val ref = new AtomicReference(init)

  def isZero: Boolean = ref.get.isEmpty

  def copy(): AccumulatorV2[Int, Set[Int]] =
    new IntSetAccumulator(ref.get)

  def reset(): Unit =
    ref.set(Set.empty)

  def add(v: Int): Unit =
    ref.getAndUpdate(_ + v)

  def merge(other: AccumulatorV2[Int, Set[Int]]): Unit =
    ref.getAndUpdate(_ ++ other.value)

  def value: Set[Int] = ref.get

}

object IntSetAccumulator {
  def apply(init: Set[Int] = Set.empty): IntSetAccumulator = new IntSetAccumulator(init)
}
