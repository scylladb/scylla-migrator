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
class CreateSelectionBenchmark {

  @Benchmark
  def createSelection_withTimestamps(): Any =
    Cassandra.createSelection(
      BenchmarkFixtures.simpleTableDef,
      BenchmarkFixtures.simpleSchema,
      preserveTimes = true
    )

  @Benchmark
  def createSelection_withoutTimestamps(): Any =
    Cassandra.createSelection(
      BenchmarkFixtures.simpleTableDef,
      BenchmarkFixtures.simpleSchema,
      preserveTimes = false
    )
}
