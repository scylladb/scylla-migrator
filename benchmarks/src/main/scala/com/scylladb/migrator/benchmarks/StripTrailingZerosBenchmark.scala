package com.scylladb.migrator.benchmarks

import com.datastax.spark.connector.types.CassandraOption
import org.apache.spark.sql.Row
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class StripTrailingZerosBenchmark {

  @Param(Array("100", "1000"))
  var batchSize: Int = _

  private var decimalRows: Array[Row] = _
  private var mixedRows: Array[Row] = _
  private var cassandraOptionRows: Array[Row] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    // Rows where all values are BigDecimals (worst case for the mapping)
    decimalRows = Array.tabulate(batchSize) { i =>
      Row.fromSeq(
        Seq(
          new java.math.BigDecimal(s"$i.12300"),
          new java.math.BigDecimal(s"${i * 10}.45600"),
          new java.math.BigDecimal(s"${i * 100}.78900")
        )
      )
    }

    // Rows with mixed types (typical case: some BigDecimals, some strings/ints)
    mixedRows = Array.tabulate(batchSize) { i =>
      Row.fromSeq(
        Seq(
          s"id-$i",
          new java.math.BigDecimal(s"$i.12300"),
          i,
          i.toLong * 1000L,
          new java.math.BigDecimal(s"${i * 10}.45600")
        )
      )
    }

    // Rows holding CassandraOption-wrapped decimals (exploded timestamp-preservation RDD path),
    // which exercise the CassandraOption.Value branch of the mapping.
    cassandraOptionRows = Array.tabulate(batchSize) { i =>
      Row.fromSeq(
        Seq(
          CassandraOption.Value(new java.math.BigDecimal(s"$i.12300")),
          CassandraOption.Value(new java.math.BigDecimal(s"${i * 10}.45600")),
          CassandraOption.Unset
        )
      )
    }
  }

  /** The stripTrailingZeros mapping as used in Scylla.writeCleanedRdd (shared by writeRowRDD and
    * writeDataframe). The CassandraOption.Value case only matches on the exploded RDD path.
    */
  private def stripTrailingZerosRow(row: Row): Row =
    Row.fromSeq(row.toSeq.map {
      case x: java.math.BigDecimal =>
        x.stripTrailingZeros()
      case CassandraOption.Value(x: java.math.BigDecimal) =>
        CassandraOption.Value(x.stripTrailingZeros())
      case x => x
    })

  @Benchmark
  def stripBatch_decimalRows(): Array[Row] =
    decimalRows.map(stripTrailingZerosRow)

  @Benchmark
  def stripBatch_mixedRows(): Array[Row] =
    mixedRows.map(stripTrailingZerosRow)

  @Benchmark
  def stripBatch_cassandraOptionRows(): Array[Row] =
    cassandraOptionRows.map(stripTrailingZerosRow)
}
