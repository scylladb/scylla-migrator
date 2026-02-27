package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  Benchmark,
  BenchmarkDataGenerator,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.junit.experimental.categories.Category

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

/** Abstract base class for integration throughput benchmarks.
  *
  * Extends [[MigratorSuite]] for source/target session management. Benchmark suites are tagged with
  * both Integration and Benchmark categories so they are excluded from regular CI runs but can be
  * invoked via dedicated Makefile targets.
  */
@Category(Array(classOf[Integration], classOf[Benchmark]))
abstract class BenchmarkSuite(sourcePort: Int) extends MigratorSuite(sourcePort) {

  override val munitTimeout: Duration = 30.minutes

  /** Run a throughput benchmark: create and populate a table, run the migration, and report
    * throughput.
    *
    * @param scenario
    *   Human-readable name for the benchmark
    * @param tableName
    *   CQL table name (must match YAML config)
    * @param configFile
    *   YAML config filename
    * @param rowCount
    *   Number of rows to insert
    */
  def runThroughputBenchmark(
    scenario: String,
    tableName: String,
    configFile: String,
    rowCount: Int
  ): Unit = {
    // Create and populate source table
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, tableName)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, tableName, rowCount)

    // Create empty target table
    BenchmarkDataGenerator.createSimpleTable(targetScylla(), keyspace, tableName)

    // Run the migration and measure time
    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration(configFile)
    val durationMs = System.currentTimeMillis() - startTime

    // Verify row count in target
    val countResult = targetScylla()
      .execute(
        QueryBuilder
          .selectFrom(keyspace, tableName)
          .countAll()
          .build()
      )
    val targetRowCount = countResult.one().getLong(0)
    assertEquals(targetRowCount, rowCount.toLong, s"Row count mismatch for $scenario")

    // Report throughput
    ThroughputReporter.report(scenario, rowCount.toLong, durationMs)
  }
}
