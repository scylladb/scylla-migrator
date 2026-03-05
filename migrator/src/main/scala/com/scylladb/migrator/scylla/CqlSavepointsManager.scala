package com.scylladb.migrator.scylla

import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.scylladb.migrator.SavepointsManager
import com.scylladb.migrator.config.MigratorConfig

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
