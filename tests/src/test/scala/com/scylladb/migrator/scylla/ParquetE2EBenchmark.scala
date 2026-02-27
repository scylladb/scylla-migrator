package com.scylladb.migrator.scylla

import com.scylladb.migrator.{
  BenchmarkDataGenerator,
  E2E,
  Integration,
  SparkUtils,
  ThroughputReporter
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.junit.experimental.categories.Category

import java.io.File
import scala.concurrent.duration._

/** End-to-end throughput benchmarks for Parquet migration paths.
  *
  * Test 1: Scylla -> Parquet (export) Test 2: Parquet -> Scylla (import, uses files from test 1)
  *
  * Source/target: ScyllaDB (port 9042) Parquet path: /app/parquet/bench_e2e (Docker volume:
  * tests/docker/parquet/bench_e2e)
  *
  * Row count is configurable via `-De2e.cql.rows=N` (default: 5000000).
  */
@Category(Array(classOf[Integration], classOf[E2E]))
class ParquetE2EBenchmark extends MigratorSuite(sourcePort = 9042) {

  override val munitTimeout: Duration = 60.minutes

  private val rowCount = sys.props.getOrElse("e2e.cql.rows", "5000000").toInt

  // Host path for the parquet volume mount
  private val parquetHostDir = new File("tests/docker/parquet/bench_e2e")

  private def deleteParquetDir(): Unit =
    if (parquetHostDir.exists()) {
      def deleteRecursive(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(deleteRecursive)
        f.delete()
      }
      deleteRecursive(parquetHostDir)
    }

  private def countParquetFiles(): Int =
    if (parquetHostDir.exists())
      parquetHostDir.listFiles().count(_.getName.endsWith(".parquet"))
    else 0

  test(s"Scylla->Parquet ${rowCount} rows") {
    val sourceTable = "bench_e2e_parquet"

    // Clean up parquet output and seed source table
    deleteParquetDir()
    BenchmarkDataGenerator.createSimpleTable(sourceCassandra(), keyspace, sourceTable)
    BenchmarkDataGenerator.insertSimpleRows(sourceCassandra(), keyspace, sourceTable, rowCount)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-scylla-to-parquet.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    val parquetFiles = countParquetFiles()
    assert(parquetFiles > 0, s"Expected parquet files in ${parquetHostDir}, found none")

    ThroughputReporter.report(s"scylla-to-parquet-${rowCount}", rowCount.toLong, durationMs)
  }

  test(s"Parquet->Scylla ${rowCount} rows") {
    val targetTable = "bench_e2e_parquet_restore"

    // Create target table; parquet files should exist from the previous test
    val parquetFiles = countParquetFiles()
    assert(
      parquetFiles > 0,
      s"No parquet files found — run 'Scylla->Parquet' test first"
    )

    BenchmarkDataGenerator.createSimpleTable(targetScylla(), keyspace, targetTable)

    val startTime = System.currentTimeMillis()
    SparkUtils.successfullyPerformMigration("bench-e2e-parquet-to-scylla.yaml")
    val durationMs = System.currentTimeMillis() - startTime

    val countResult = targetScylla()
      .execute(
        QueryBuilder
          .selectFrom(keyspace, targetTable)
          .countAll()
          .build()
      )
    val targetRowCount = countResult.one().getLong(0)
    assertEquals(targetRowCount, rowCount.toLong, "Row count mismatch for Parquet->Scylla")

    ThroughputReporter.report(s"parquet-to-scylla-${rowCount}", rowCount.toLong, durationMs)

    // Clean up parquet files after successful roundtrip
    deleteParquetDir()
  }
}
