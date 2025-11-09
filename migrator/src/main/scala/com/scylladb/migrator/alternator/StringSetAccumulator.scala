package com.scylladb.migrator.alternator

import org.apache.spark.util.AccumulatorV2
import java.util.concurrent.atomic.AtomicReference

class StringSetAccumulator(initialValue: Set[String] = Set.empty)
    extends AccumulatorV2[String, Set[String]] {

  private val ref = new AtomicReference(initialValue)

  // Note: isZero may be momentarily inconsistent in concurrent scenarios,
  // as it reads the current value of the set without synchronization.
  // This is eventually consistent and thread-safe, but may not reflect the most recent updates.
  def isZero: Boolean = ref.get.isEmpty
  def copy(): StringSetAccumulator = new StringSetAccumulator(ref.get)
  def reset(): Unit = ref.set(Set.empty)
  def add(v: String): Unit = ref.getAndUpdate(_ + v)

  def merge(other: AccumulatorV2[String, Set[String]]): Unit =
    ref.getAndUpdate(_ ++ other.value)

  def value: Set[String] = ref.get
}

object StringSetAccumulator {
  def apply(initialValue: Set[String] = Set.empty): StringSetAccumulator =
    new StringSetAccumulator(initialValue)
}
