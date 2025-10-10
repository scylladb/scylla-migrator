package com.scylladb.migrator.alternator

import org.apache.spark.util.AccumulatorV2
import java.util.concurrent.atomic.AtomicReference

class StringSetAccumulator(initialValue: Set[String] = Set.empty)
    extends AccumulatorV2[String, Set[String]] {

  private val ref = new AtomicReference(initialValue)

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
