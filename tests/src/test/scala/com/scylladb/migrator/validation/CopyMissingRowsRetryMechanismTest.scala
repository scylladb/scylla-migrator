package com.scylladb.migrator.validation

import org.apache.spark.{ SparkConf, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.concurrent.TrieMap

/** A shared, mutable stand-in for an external datastore (e.g. Scylla), visible across task
  * attempts: in `local[*]` mode, tasks are separate threads within this same JVM, so a plain object
  * field is enough to observe what a "retried" task actually wrote.
  */
private object CopyMissingRowsRetryFixture {
  val target: TrieMap[Int, Set[String]] = TrieMap.empty
  val key: Int = 1
  val subwrites: Seq[String] = Seq("a", "b")
}

/** Proves, without a real Cassandra/Scylla cluster, the Spark caching mechanism that
  * `ScyllaValidator.runValidation`'s `includePerColumnMetadata` repair path now relies on (see its
  * doc comment and the `if (includePerColumnMetadata)` block around `rawRepairDf`).
  *
  * The scenario mirrors a PR review finding: a repair row whose columns are written as several
  * separate physical writes (there: TTL/WRITETIME groups, or a base + collection-append pass; here:
  * two "sub-writes" `"a"` and `"b"`). A task is made to fail right after committing the first
  * sub-write but before the second, on its first attempt only. Since the join/missingness check and
  * the write share one, otherwise-uncached chain, a naive retry re-derives "is this row missing"
  * against the target its OWN earlier attempt already partially mutated - permanently losing the
  * second sub-write.
  *
  * These tests confirm: that is true with no checkpoint at all; that a bare `persist()` already
  * closes the gap on its own, because Spark fully materializes a cached RDD partition into the
  * block store before exposing even its first element to any downstream consumer (including a write
  * that fails partway through) - so the write itself, as the first reader, is already safe; and
  * that the result is unaffected by target mutations happening after the cache is populated. The
  * production code additionally runs a dedicated `.count()` before any write begins (matching the
  * pre-existing, battle-tested pattern this restores) so that, for a Dataset backed by multi-batch
  * columnar caching, EVERY batch of EVERY partition is guaranteed materialized before any write job
  * starts, rather than relying on per-batch exposure details this plain-RDD test is too simple to
  * exercise.
  */
class CopyMissingRowsRetryMechanismTest extends munit.FunSuite {

  private def withLocalSpark(testName: String)(f: SparkContext => Unit): Unit = {
    val contextName = s"CopyMissingRowsRetryMechanismTest-$testName"
    val sc = SparkContext.getOrCreate(
      new SparkConf()
        // Plain `local[N]` hardcodes maxTaskFailures=1 regardless of `spark.task.maxFailures`
        // (see SparkContext.createTaskScheduler's LOCAL_N_REGEX handling) - only the
        // `local[N, F]` master-string form actually honors a higher retry budget locally.
        .setMaster("local[2, 1024]")
        .setAppName(contextName)
        .set("spark.driver.host", "localhost")
    )
    try f(sc)
    finally
      // Another suite in the same forked test JVM may own the active (reused) context.
      if (sc.appName == contextName) sc.stop()
  }

  /** The missingness check: a row is "missing" (and thus a repair candidate) only while the
    * fixture's target doesn't already have it - mirrors `targetRow.isEmpty` in `ScyllaValidator`'s
    * `repairRowsRdd`.
    */
  private def missingRowsRdd(sc: SparkContext): RDD[(Int, Seq[String])] =
    sc.parallelize(Seq(CopyMissingRowsRetryFixture.key), numSlices = 1).mapPartitions {
      partitionIter =>
        partitionIter.flatMap { k =>
          if (!CopyMissingRowsRetryFixture.target.contains(k))
            Some((k, CopyMissingRowsRetryFixture.subwrites))
          else None
        }
    }

  /** The repair write: commits each sub-write for each row into the fixture's target, one at a
    * time, throwing right after committing `"a"` but before `"b"` - only on the owning task's first
    * attempt, so a retry of that same task succeeds. Mirrors a connector write task that commits
    * some of a row's physical writes before failing.
    */
  private def writeWithInjectedFailure(rdd: RDD[(Int, Seq[String])]): Unit =
    rdd.foreachPartition { partitionIter =>
      partitionIter.foreach { case (k, writes) =>
        writes.foreach { sw =>
          CopyMissingRowsRetryFixture.target.update(
            k,
            CopyMissingRowsRetryFixture.target.getOrElse(k, Set.empty[String]) + sw
          )
          if (sw == "a" && TaskContext.get().attemptNumber() == 0)
            throw new RuntimeException("injected failure after the first sub-write")
        }
      }
    }

  test(
    "without a persisted checkpoint, a retry re-derives missingness against its own earlier " +
      "partial write and permanently loses the rest of the row"
  ) {
    CopyMissingRowsRetryFixture.target.clear()
    withLocalSpark("no-checkpoint") { sc =>
      writeWithInjectedFailure(missingRowsRdd(sc))
      assertEquals(
        CopyMissingRowsRetryFixture.target.get(CopyMissingRowsRetryFixture.key),
        Some(Set("a"))
      )
    }
  }

  test(
    "persist() alone, with the write as its first consumer, already closes the gap"
  ) {
    CopyMissingRowsRetryFixture.target.clear()
    withLocalSpark("persist-alone") { sc =>
      val rdd = missingRowsRdd(sc)
      rdd.persist(StorageLevel.MEMORY_AND_DISK)
      try {
        // No dedicated action drains `rdd` before the write below - the write is the first
        // consumer of the cache. Spark still fully unrolls and registers this partition's block
        // (BlockManager.doPutIterator/MemoryStore.putIteratorAsValues) before handing back an
        // iterator to any reader, so a retry here reads the pre-write decision from cache rather
        // than re-deriving it from the (by then partially mutated) target.
        writeWithInjectedFailure(rdd)
        assertEquals(
          CopyMissingRowsRetryFixture.target.get(CopyMissingRowsRetryFixture.key),
          Some(Set("a", "b"))
        )
      } finally rdd.unpersist()
    }
  }

  test(
    "the cached checkpoint is unaffected by target mutations that happen after count() returns"
  ) {
    CopyMissingRowsRetryFixture.target.clear()
    withLocalSpark("count-isolation") { sc =>
      val rdd = missingRowsRdd(sc)
      rdd.persist(StorageLevel.MEMORY_AND_DISK)
      try {
        rdd.count()
        // An unrelated mutation after the checkpoint (e.g. a different partition's write
        // landing out of band) must not change what THIS already-cached decision considers
        // missing for `key`.
        CopyMissingRowsRetryFixture.target.update(999, Set("unrelated"))
        writeWithInjectedFailure(rdd)
        assertEquals(
          CopyMissingRowsRetryFixture.target.get(CopyMissingRowsRetryFixture.key),
          Some(Set("a", "b"))
        )
      } finally rdd.unpersist()
    }
  }
}
