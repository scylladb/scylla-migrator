package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig

import scala.util.control.NonFatal

/** Manage CQL-based migrations by tracking the migrated token ranges.
  */
class CqlSavepointsManager(migratorConfig: MigratorConfig, val accumulator: TokenRangeAccumulator)
    extends SavepointsManager(migratorConfig) {
  def describeMigrationState(): String =
    s"Ranges added: ${tokenRanges(accumulator)}"

  def updateConfigWithMigrationState(): MigratorConfig =
    migratorConfig.copy(
      skipTokenRanges =
        Some(migratorConfig.getSkipTokenRangesOrEmptySet ++ tokenRanges(accumulator))
    )

  /** Log how many token ranges the source DataFrame covers and how many remain after subtracting
    * the savepoint set. Previously emitted from `ScyllaMigratorBase.migrate` gated by
    * `isInstanceOf[SourceSettings.Cassandra]`; moved here so the central `migrate` method is
    * backend-neutral and the Cassandra-specific `CassandraPartition` cast lives next to the only
    * manager that knows how to consume it.
    *
    * Defensive: if a future caller wires in a non-Cassandra DataFrame by mistake, the cast would
    * `ClassCastException`. Swallow that and log a warning rather than failing the migration — this
    * is a diagnostic, not a correctness path.
    */
  def logTokenRangeCoverage(
    sourceDF: SourceDataFrame,
    skipTokenRanges: Option[Set[(Token[_], Token[_])]]
  ): Unit =
    try {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => p.asInstanceOf[CassandraPartition[_, _]])
      val allTokenRangesBuilder = Set.newBuilder[(Token[_], Token[_])]
      cassandraPartitions.foreach(p =>
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach { tr =>
            val range: (Token[_], Token[_]) =
              (tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]])
            allTokenRangesBuilder += range
          }
      )
      val allTokenRanges = allTokenRangesBuilder.result()

      log.info("All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (skipTokenRanges.isDefined) {
        log.info(
          "Savepoints array defined, size of the array: " + skipTokenRanges.fold(0)(_.size)
        )

        val diff = allTokenRanges.diff(skipTokenRanges.getOrElse(Set.empty))
        log.info("Diff ... total diff of full ranges to savepoints is: " + diff.size)
        if (log.isDebugEnabled) {
          val sample = diff.take(20)
          log.debug(s"Sample of missing tokens (${sample.size} of ${diff.size}): $sample")
        }
      }
    } catch {
      case _: ClassCastException =>
        log.warn(
          "Source DataFrame partitions are not CassandraPartitions; skipping token-range " +
            "coverage diagnostic. This is unexpected for a CqlSavepointsManager and may indicate " +
            "an incorrectly wired source."
        )
      case NonFatal(t) =>
        log.warn(
          s"Failed to log token-range coverage diagnostic: ${Option(t.getMessage).getOrElse(t.toString)}"
        )
    }

  private def tokenRanges(accumulator: TokenRangeAccumulator): Set[(Token[_], Token[_])] =
    accumulator.value.get.map(range =>
      (range.range.start.asInstanceOf[Token[_]], range.range.end.asInstanceOf[Token[_]])
    )
}

object CqlSavepointsManager {
  def apply(
    migratorConfig: MigratorConfig,
    accumulator: TokenRangeAccumulator
  ): CqlSavepointsManager =
    new CqlSavepointsManager(migratorConfig, accumulator)
}
