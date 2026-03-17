package com.scylladb.migrator.benchmarks

import com.datastax.spark.connector.CassandraRow
import com.scylladb.migrator.alternator.DdbValue
import com.scylladb.migrator.validation.RowComparisonFailure
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class RowComparisonBenchmark {

  private var cassandraLeft: CassandraRow = _
  private var cassandraRightSame: CassandraRow = _
  private var cassandraRightDiff: CassandraRow = _

  private var cassandraWithTsLeft: CassandraRow = _
  private var cassandraWithTsSame: CassandraRow = _
  private var cassandraWithTsDiff: CassandraRow = _

  private var dynamoLeft: Map[String, DdbValue] = _
  private var dynamoRightSame: Map[String, DdbValue] = _
  private var dynamoRightDiff: Map[String, DdbValue] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val cassandraData = Map[String, Any](
      "id"   -> "row-1",
      "col1" -> "value-1",
      "col2" -> 42,
      "col3" -> 1000L
    )
    cassandraLeft      = CassandraRow.fromMap(cassandraData)
    cassandraRightSame = CassandraRow.fromMap(cassandraData)
    cassandraRightDiff = CassandraRow.fromMap(
      cassandraData.updated("col1", "different-value").updated("col2", 99)
    )

    // Rows with _ttl and _writetime columns for compareTimestamps=true
    val tsData = Map[String, Any](
      "id"             -> "row-1",
      "col1"           -> "value-1",
      "col1_ttl"       -> 3600L,
      "col1_writetime" -> 1000000L,
      "col2"           -> 42,
      "col2_ttl"       -> 3600L,
      "col2_writetime" -> 1000000L
    )
    cassandraWithTsLeft = CassandraRow.fromMap(tsData)
    cassandraWithTsSame = CassandraRow.fromMap(tsData)
    cassandraWithTsDiff = CassandraRow.fromMap(
      tsData
        .updated("col1_ttl", 7200L)
        .updated("col2_writetime", 9999999L)
    )

    dynamoLeft = Map(
      "pk"     -> DdbValue.S("user-123"),
      "name"   -> DdbValue.S("Alice"),
      "age"    -> DdbValue.N("30"),
      "active" -> DdbValue.Bool(true)
    )
    dynamoRightSame = Map(
      "pk"     -> DdbValue.S("user-123"),
      "name"   -> DdbValue.S("Alice"),
      "age"    -> DdbValue.N("30"),
      "active" -> DdbValue.Bool(true)
    )
    dynamoRightDiff = Map(
      "pk"     -> DdbValue.S("user-123"),
      "name"   -> DdbValue.S("Bob"),
      "age"    -> DdbValue.N("25"),
      "active" -> DdbValue.Bool(false)
    )
  }

  @Benchmark
  def compareCassandraRows_identical(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareCassandraRows(
      cassandraLeft,
      Some(cassandraRightSame),
      floatingPointTolerance   = 0.0,
      timestampMsTolerance     = 0L,
      ttlToleranceMillis       = 0L,
      writetimeToleranceMillis = 0L,
      compareTimestamps        = false
    )

  @Benchmark
  def compareCassandraRows_differing(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareCassandraRows(
      cassandraLeft,
      Some(cassandraRightDiff),
      floatingPointTolerance   = 0.0,
      timestampMsTolerance     = 0L,
      ttlToleranceMillis       = 0L,
      writetimeToleranceMillis = 0L,
      compareTimestamps        = false
    )

  @Benchmark
  def compareDynamoDBRows_identical(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareDynamoDBRows(
      dynamoLeft,
      Some(dynamoRightSame),
      renamedColumn          = identity,
      floatingPointTolerance = 0.0
    )

  @Benchmark
  def compareDynamoDBRows_differing(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareDynamoDBRows(
      dynamoLeft,
      Some(dynamoRightDiff),
      renamedColumn          = identity,
      floatingPointTolerance = 0.0
    )

  @Benchmark
  def compareCassandraRows_withTimestamps_identical(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareCassandraRows(
      cassandraWithTsLeft,
      Some(cassandraWithTsSame),
      floatingPointTolerance   = 0.0,
      timestampMsTolerance     = 0L,
      ttlToleranceMillis       = 0L,
      writetimeToleranceMillis = 0L,
      compareTimestamps        = true
    )

  @Benchmark
  def compareCassandraRows_withTimestamps_differing(): Option[RowComparisonFailure] =
    RowComparisonFailure.compareCassandraRows(
      cassandraWithTsLeft,
      Some(cassandraWithTsDiff),
      floatingPointTolerance   = 0.0,
      timestampMsTolerance     = 0L,
      ttlToleranceMillis       = 0L,
      writetimeToleranceMillis = 0L,
      compareTimestamps        = true
    )
}
