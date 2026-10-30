package com.scylladb.migrator.validation

import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.atomic.AtomicReference

/** Accumulator collecting a best-effort, bounded sample of items across tasks.
  *
  * The list stays capped at `maxSize` at all times, including right after a `merge`, so memory use
  * is O(maxSize) regardless of how many tasks contribute or how many items were seen in total.
  * Element order/composition is not a reproducible contract - this is a diagnostic sample, not
  * `RDD.take`.
  *
  * Like any accumulator mutated from inside a transformation (not read after an action), `add`
  * calls here can be applied more than once if a task is retried or speculatively re-executed.
  */
class BoundedListAccumulator[A](maxSize: Int, initialValue: List[A] = Nil)
    extends AccumulatorV2[A, List[A]] {
  require(maxSize >= 0, "maxSize must not be negative")

  private val ref = new AtomicReference(initialValue.take(maxSize))

  def isZero: Boolean = ref.get.isEmpty
  def copy(): BoundedListAccumulator[A] = new BoundedListAccumulator(maxSize, ref.get)
  def reset(): Unit = ref.set(Nil)

  def add(v: A): Unit =
    ref.getAndUpdate(current => if (current.size >= maxSize) current else current :+ v)

  def merge(other: AccumulatorV2[A, List[A]]): Unit =
    ref.getAndUpdate(current => (current ++ other.value).take(maxSize))

  def value: List[A] = ref.get
}

object BoundedListAccumulator {
  def apply[A](maxSize: Int, initialValue: List[A] = Nil): BoundedListAccumulator[A] =
    new BoundedListAccumulator(maxSize, initialValue)
}
