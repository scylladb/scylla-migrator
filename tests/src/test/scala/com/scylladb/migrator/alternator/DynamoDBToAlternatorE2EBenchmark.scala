package com.scylladb.migrator.alternator

import com.scylladb.migrator.{
  DynamoDBBenchmarkDataGenerator,
  SparkUtils,
  ThroughputReporter
}

/** End-to-end throughput benchmarks for DynamoDB-to-Alternator migration.
  *
  * Source: DynamoDB Local (port 8001) Target: ScyllaDB Alternator (port 8000)
  *
  * Row count is configurable via `-De2e.ddb.rows=N` (default: 500000).
  *
  * Note: DynamoDB Local is significantly slower than real DynamoDB for batch writes (25
  * items/batch), so the row count is lower than CQL-based benchmarks. The seeding time is excluded
  * from the migration measurement.
  */
class DynamoDBToAlternatorE2EBenchmark extends DynamoDBE2EBenchmarkSuite {

  test(s"DynamoDB->Alternator ${rowCount} rows") {
    val tableName = "bench_e2e_ddb"

    // Clean up both source and target from any previous runs
    deleteTableIfExists(targetAlternator(), tableName)
    DynamoDBBenchmarkDataGenerator.createSimpleTable(sourceDDb(), tableName)
    DynamoDBBenchmarkDataGenerator.insertSimpleRows(sourceDDb(), tableName, rowCount)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration("bench-e2e-dynamodb-to-alternator.yaml")
      val durationMs = System.currentTimeMillis() - startTime

      val targetCount = DynamoDBBenchmarkDataGenerator.countItems(targetAlternator(), tableName)
      assertEquals(targetCount, rowCount.toLong, "Row count mismatch for DynamoDB->Alternator")

      // Spot-check a few rows to catch data corruption
      DynamoDBBenchmarkDataGenerator.spotCheckRows(targetAlternator(), tableName, rowCount)

      ThroughputReporter.report(s"dynamodb-to-alternator-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      deleteTableIfExists(sourceDDb(), tableName)
      deleteTableIfExists(targetAlternator(), tableName)
    }
  }
}
