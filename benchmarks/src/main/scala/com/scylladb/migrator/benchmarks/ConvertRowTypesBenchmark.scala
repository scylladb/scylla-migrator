package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.benchmarks.util.BenchmarkFixtures
import com.scylladb.migrator.readers.Cassandra
import org.apache.spark.sql.Row
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class ConvertRowTypesBenchmark {

  @Param(Array("100", "1000"))
  var batchSize: Int = _

  private var utf8Rows: Array[Row] = _
  private var plainRows: Array[Row] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    utf8Rows  = Array.tabulate(batchSize)(BenchmarkFixtures.makeUTF8Row)
    plainRows = Array.tabulate(batchSize)(BenchmarkFixtures.makeSimpleRow)
  }

  @Benchmark
  def convertBatch_utf8Rows(): Array[Row] =
    utf8Rows.map(row => Row.fromSeq(row.toSeq.map(Cassandra.convertValue)))

  @Benchmark
  def convertBatch_plainRows(): Array[Row] =
    plainRows.map(row => Row.fromSeq(row.toSeq.map(Cassandra.convertValue)))
}
