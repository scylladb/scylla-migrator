package com.scylladb.migrator.benchmarks

import com.scylladb.migrator.benchmarks.util.BenchmarkFixtures
import com.scylladb.migrator.readers.Cassandra
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class ExplodeRowBenchmark {

  private var singleTimestampRow: org.apache.spark.sql.Row = _
  private var multiTimestampRow: org.apache.spark.sql.Row = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    singleTimestampRow = BenchmarkFixtures.makeSingleTimestampRow(1)
    multiTimestampRow  = BenchmarkFixtures.makeMultiTimestampRow(1)
  }

  @Benchmark
  def explodeRow_singleTimestamp(): Any =
    Cassandra.explodeRow(
      singleTimestampRow,
      BenchmarkFixtures.timestampSchema,
      BenchmarkFixtures.primaryKeyOrdinals,
      BenchmarkFixtures.regularKeyOrdinals
    )

  @Benchmark
  def explodeRow_multiTimestamp(): Any =
    Cassandra.explodeRow(
      multiTimestampRow,
      BenchmarkFixtures.timestampSchema,
      BenchmarkFixtures.primaryKeyOrdinals,
      BenchmarkFixtures.regularKeyOrdinals
    )

  @Benchmark
  def explodeRow_noRegularKeys(): Any =
    Cassandra.explodeRow(
      BenchmarkFixtures.makeSimpleRow(1),
      BenchmarkFixtures.simpleSchema,
      BenchmarkFixtures.primaryKeyOrdinals,
      regularKeyOrdinals = Map.empty
    )
}
