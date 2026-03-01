package com.scylladb.migrator.scylla

import com.scylladb.migrator.{ BenchmarkDataGenerator, SparkUtils, ThroughputReporter }
import com.datastax.oss.driver.api.querybuilder.{ QueryBuilder, SchemaBuilder }

/** Shared throughput benchmark logic for CQL-based benchmark suites. */
trait ThroughputBenchmarkSupport { self: MigratorSuite =>

  def runThroughputBenchmark(
    scenario: String,
    tableName: String,
    configFile: String,
    rowCount: Int
  ): Unit = {
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, tableName)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, tableName, rowCount)

    BenchmarkDataGenerator.createSimpleTable(targetScylla(), keyspace, tableName)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration(configFile)
      val durationMs = System.currentTimeMillis() - startTime

      val countResult = targetScylla()
        .execute(
          QueryBuilder
            .selectFrom(keyspace, tableName)
            .countAll()
            .build()
        )
      val targetRowCount = countResult.one().getLong(0)
      assertEquals(targetRowCount, rowCount.toLong, s"Row count mismatch for $scenario")

      // Spot-check a few rows to catch data corruption
      spotCheckRows(tableName, rowCount)

      ThroughputReporter.report(scenario, rowCount.toLong, durationMs)
    } finally {
      val dropStmt = SchemaBuilder.dropTable(keyspace, tableName).ifExists().build()
      try sourceCassandra().execute(dropStmt)
      catch { case e: Exception => System.err.println(s"Cleanup: failed to drop source table: ${e.getMessage}") }
      try targetScylla().execute(dropStmt)
      catch { case e: Exception => System.err.println(s"Cleanup: failed to drop target table: ${e.getMessage}") }
    }
  }

  /** Verify a sample of rows in the target to detect data corruption. */
  protected def spotCheckRows(tableName: String, rowCount: Int): Unit = {
    val sampleIndices = BenchmarkDataGenerator.sampleIndices(rowCount)
    val prepared = targetScylla().prepare(
      s"SELECT id, col1, col2, col3 FROM $keyspace.$tableName WHERE id = ?"
    )
    for (i <- sampleIndices) {
      val row = targetScylla().execute(prepared.bind(s"id-$i")).one()
      assert(row != null, s"Spot-check: row id-$i not found in target")
      assertEquals(row.getString("col1"), s"value-$i", s"Spot-check: col1 mismatch for id-$i")
      assertEquals(row.getInt("col2"), i, s"Spot-check: col2 mismatch for id-$i")
      assertEquals(row.getLong("col3"), i.toLong * 1000L, s"Spot-check: col3 mismatch for id-$i")
    }
  }
}
