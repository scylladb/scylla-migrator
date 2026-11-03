package com.scylladb.migrator.readers

import org.apache.spark.sql.SparkSession

/** Batching for per-element collection preservation. Its multi-pass writes (base write plus one
  * append pass per non-frozen collection) cache their input so every pass reads the same snapshot;
  * processing the input in batches bounds that cache to one batch.
  */
object CollectionBatching {

  /** Spark partitions (source splits) per batch, from `-Dscylla.migrator.collectionBatchPartitions`
    * on the DRIVER, defaulting to Spark's default parallelism (one wave of tasks). A non-positive
    * value disables batching: the whole input is one batch.
    */
  def batchPartitions(spark: SparkSession): Int =
    sys.props.get("scylla.migrator.collectionBatchPartitions") match {
      case None => math.max(1, spark.sparkContext.defaultParallelism)
      case Some(raw) =>
        scala.util
          .Try(raw.trim.toInt)
          .toOption
          .map(n => if (n <= 0) Int.MaxValue else n)
          .getOrElse(
            throw new IllegalArgumentException(
              s"-Dscylla.migrator.collectionBatchPartitions=$raw is invalid: expected an integer " +
                "(a non-positive value disables batching)."
            )
          )
    }

  /** Split `items` into consecutive, non-empty groups whose total `weight` stays within `limit`,
    * except that an item heavier than `limit` forms a group of its own.
    */
  def group[A](items: Seq[A], weight: A => Long, limit: Long): Seq[Seq[A]] = {
    val groups = Seq.newBuilder[Seq[A]]
    var current = Vector.empty[A]
    var currentWeight = 0L
    items.foreach { item =>
      val w = math.max(0L, weight(item))
      if (current.nonEmpty && currentWeight + w > limit) {
        groups += current
        current       = Vector.empty
        currentWeight = 0L
      }
      current :+= item
      currentWeight += w
    }
    if (current.nonEmpty) groups += current
    groups.result()
  }
}
