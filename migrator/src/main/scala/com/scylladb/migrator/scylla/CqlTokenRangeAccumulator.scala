package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.dht.Token
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.atomic.AtomicReference

/** A Spark Accumulator that accumulates token ranges into a `Set[(Token[_], Token[_])]`.
  *
  * We use it to track the token ranges that have been migrated to the target when writing to
  * Parquet (where the DataStax connector's built-in `TokenRangeAccumulator` is not available).
  */
class CqlTokenRangeAccumulator(init: Set[(Token[_], Token[_])])
    extends AccumulatorV2[Set[(Token[_], Token[_])], Set[(Token[_], Token[_])]] {

  private val ref = new AtomicReference(init)

  def isZero: Boolean = ref.get.isEmpty

  def copy(): AccumulatorV2[Set[(Token[_], Token[_])], Set[(Token[_], Token[_])]] =
    new CqlTokenRangeAccumulator(ref.get)

  def reset(): Unit =
    ref.set(Set.empty)

  def add(v: Set[(Token[_], Token[_])]): Unit =
    ref.getAndUpdate(_ ++ v)

  def merge(other: AccumulatorV2[Set[(Token[_], Token[_])], Set[(Token[_], Token[_])]]): Unit =
    ref.getAndUpdate(_ ++ other.value)

  def value: Set[(Token[_], Token[_])] = ref.get

}

object CqlTokenRangeAccumulator {
  def apply(init: Set[(Token[_], Token[_])] = Set.empty): CqlTokenRangeAccumulator =
    new CqlTokenRangeAccumulator(init)
}
