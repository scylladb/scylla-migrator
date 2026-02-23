package com.scylladb.migrator.alternator

import org.apache.spark.util.AccumulatorV2
import java.util.concurrent.atomic.AtomicReference

/**
  * Accumulator for tracking processed Parquet file paths during migration.
  *
  * This accumulator collects the set of Parquet file paths that have been processed
  * as part of a migration job. It is useful for monitoring progress, avoiding duplicate
  * processing, and debugging migration workflows. The accumulator is thread-safe and
  * can be used in distributed Spark jobs.
  *
  * @param initialValue The initial set of processed file paths (usually empty).
  */
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
