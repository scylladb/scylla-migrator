package com.scylladb.migrator.validation

import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.atomic.AtomicReference

/** Accumulator collecting (at most) one value per RDD partition, deduplicating contributions from
  * re-executed stage attempts.
  *
  * Each contribution is tagged with the partition id and the Spark `stageAttemptNumber` that
  * produced it (see `org.apache.spark.TaskContext`). `add`/`merge` keep only the entry with the
  * highest `stageAttemptNumber` for a given partition id, discarding a lower-or-equal-attempt
  * contribution rather than summing it in.
  *
  * This guards against the long-standing Spark behavior described in SPARK-9809/SPARK-10042: when a
  * stage is resubmitted (most commonly after a shuffle fetch failure), Spark reuses the same
  * `Stage` object across attempts, and tasks for partitions that had already succeeded under the
  * OLD stage attempt can be re-run under the NEW stage attempt, causing both contributions to be
  * merged on the driver. Keying by (partitionId, stageAttemptNumber) and keeping only the newest
  * attempt per partition cancels this.
  *
  * Ordinary task-level retries (same stage attempt) need no special handling here: a failed task's
  * accumulator updates are never merged into the driver-side accumulator in the first place, so
  * normally only a successful attempt's single `add` call is ever observed per partition per stage
  * attempt. The equal-attempt-number branch below is defense-in-depth (e.g. speculative execution
  * within the same stage attempt): it keeps exactly one contribution rather than summing
  * duplicates; which of two equal-attempt contributions wins is unspecified.
  */
class PartitionKeyedAccumulator[A](initialValue: Map[Int, (Int, A)] = Map.empty[Int, (Int, A)])
    extends AccumulatorV2[(Int, Int, A), Map[Int, (Int, A)]] {

  private val ref = new AtomicReference(initialValue)

  def isZero: Boolean = ref.get.isEmpty
  def copy(): PartitionKeyedAccumulator[A] = new PartitionKeyedAccumulator(ref.get)
  def reset(): Unit = ref.set(Map.empty)

  /** @param v `(partitionId, stageAttemptNumber, value)` */
  def add(v: (Int, Int, A)): Unit = {
    val (partitionId, stageAttemptNumber, value) = v
    ref.getAndUpdate { current =>
      current.get(partitionId) match {
        case Some((existingAttempt, _)) if existingAttempt >= stageAttemptNumber => current
        case _ => current.updated(partitionId, (stageAttemptNumber, value))
      }
    }
  }

  def merge(other: AccumulatorV2[(Int, Int, A), Map[Int, (Int, A)]]): Unit =
    other.value.foreach { case (partitionId, (attempt, value)) =>
      add((partitionId, attempt, value))
    }

  def value: Map[Int, (Int, A)] = ref.get

  /** The winning value for each partition that has reported one, in unspecified order. */
  def values: Iterable[A] = ref.get.values.map(_._2)
}

object PartitionKeyedAccumulator {
  def apply[A](
    initialValue: Map[Int, (Int, A)] = Map.empty[Int, (Int, A)]
  ): PartitionKeyedAccumulator[A] =
    new PartitionKeyedAccumulator(initialValue)
}

/** The per-partition diagnostic result of a repair pass: how many rows were (re)written from that
  * partition, and a locally-capped sample of comparison failures observed in it.
  */
case class PartitionRepairSummary(missingCount: Long, failureSample: List[RowComparisonFailure])

object PartitionRepairSummary {

  /** Combine the per-partition winning summaries into one: sum the counts, and concatenate the
    * per-partition failure samples, capped at `failuresToFetch` overall.
    */
  def combine(
    summaries: Iterable[PartitionRepairSummary],
    failuresToFetch: Int
  ): PartitionRepairSummary =
    PartitionRepairSummary(
      summaries.foldLeft(0L)(_ + _.missingCount),
      summaries.iterator.flatMap(_.failureSample).take(failuresToFetch).toList
    )
}
