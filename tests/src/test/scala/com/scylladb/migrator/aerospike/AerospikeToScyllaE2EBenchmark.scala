package com.scylladb.migrator.aerospike

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.{ QueryBuilder, SchemaBuilder }
import com.scylladb.migrator.{ E2E, Integration, SparkUtils, ThroughputReporter }
import org.apache.logging.log4j.LogManager
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** End-to-end throughput benchmark for Aerospike-to-Scylla migration.
  *
  * Source: Aerospike (port 3000) Target: ScyllaDB (port 9042)
  *
  * Row count is configurable via `-De2e.aerospike.rows=N` (default: 5000000).
  *
  * Currently only exercises String and Long columns. A future enhancement could add Double, Binary,
  * List, and Map columns for more comprehensive conversion benchmarking.
  *
  * Note: This benchmark is excluded from CI (E2E category). Run locally with:
  * {{{make test-benchmark-e2e-aerospike-scylla}}}
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class AerospikeToScyllaE2EBenchmark extends MigratorSuite {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount: Int = sys.props
    .get("e2e.aerospike.rows")
    .getOrElse("5000000")
    .toIntOption
    .getOrElse(throw new IllegalArgumentException("e2e.aerospike.rows must be a valid integer"))

  private val setName = "bench_e2e_aerospike"
  private val configFile = "bench-e2e-aerospike-to-scylla.yaml"

  test(s"Aerospike->Scylla ${rowCount} rows") {
    // Truncate source Aerospike set
    try
      sourceAerospike()
        .truncate(new com.aerospike.client.policy.InfoPolicy(), aerospikeNamespace, setName, null)
    catch { case _: Exception => () }
    waitForTruncate(setName)

    // Insert test data into Aerospike
    AerospikeBenchmarkDataGenerator
      .insertSimpleRows(sourceAerospike(), aerospikeNamespace, setName, rowCount)

    // Create target Scylla table (Aerospike integers are all Long → BIGINT)
    val dropStmt = SchemaBuilder.dropTable(keyspace, setName).ifExists().build()
    targetScylla().execute(dropStmt)

    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("col1", DataTypes.TEXT)
      .withColumn("col2", DataTypes.BIGINT)
      .withColumn("col3", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    try {
      val startTime = System.currentTimeMillis()
      SparkUtils.successfullyPerformMigration(configFile)
      val durationMs = System.currentTimeMillis() - startTime

      // Verify row count
      val countStmt = QueryBuilder
        .selectFrom(keyspace, setName)
        .countAll()
        .build()
        .setTimeout(java.time.Duration.ofMinutes(5))
      val targetRowCount = targetScylla().execute(countStmt).one().getLong(0)
      assertEquals(targetRowCount, rowCount.toLong, "Row count mismatch")

      // Spot-check a few rows
      val sampleIndices = AerospikeBenchmarkDataGenerator.sampleIndices(rowCount)
      val prepared = targetScylla().prepare(
        s"SELECT aero_key, col1, col2, col3 FROM $keyspace.$setName WHERE aero_key = ?"
      )
      for (i <- sampleIndices) {
        val row = targetScylla().execute(prepared.bind(s"id-$i")).one()
        assert(row != null, s"Spot-check: row id-$i not found in target")
        assertEquals(row.getString("col1"), s"value-$i", s"Spot-check: col1 mismatch for id-$i")
        assertEquals(row.getLong("col2"), i.toLong, s"Spot-check: col2 mismatch for id-$i")
        assertEquals(
          row.getLong("col3"),
          i.toLong * 1000L,
          s"Spot-check: col3 mismatch for id-$i"
        )
      }

      ThroughputReporter.report(s"aerospike-to-scylla-${rowCount}", rowCount.toLong, durationMs)
    } finally {
      try targetScylla().execute(dropStmt)
      catch {
        case e: Exception =>
          LogManager.getLogger(getClass).warn("Cleanup: drop table failed", e)
      }
      try
        sourceAerospike()
          .truncate(new com.aerospike.client.policy.InfoPolicy(), aerospikeNamespace, setName, null)
      catch { case _: Exception => () }
    }
  }
}
