package com.scylladb.migrator.readers

class AerospikePartitionTest extends munit.FunSuite {

  // --- computePartitions (getPartitions logic) ---

  test("computePartitions: splitCount=1 yields single partition covering all 4096") {
    val parts = AerospikeRDD.computePartitions(1)
    assertEquals(parts.length, 1)
    assertEquals(parts(0).partBegin, 0)
    assertEquals(parts(0).partCount, 4096)
  }

  test("computePartitions: splitCount=4096 yields one partition per Aerospike partition") {
    val parts = AerospikeRDD.computePartitions(4096)
    assertEquals(parts.length, 4096)
    parts.zipWithIndex.foreach { case (p, i) =>
      assertEquals(p.partBegin, i)
      assertEquals(p.partCount, 1)
    }
  }

  test("computePartitions: splitCount=3 distributes remainder correctly") {
    // 4096 / 3 = 1365 remainder 1 -> first partition gets 1366, rest get 1365
    val parts = AerospikeRDD.computePartitions(3)
    assertEquals(parts.length, 3)
    assertEquals(parts(0).partCount, 1366)
    assertEquals(parts(1).partCount, 1365)
    assertEquals(parts(2).partCount, 1365)
    // Verify contiguous coverage
    assertEquals(parts(0).partBegin, 0)
    assertEquals(parts(1).partBegin, 1366)
    assertEquals(parts(2).partBegin, 2731)
    // Total must be 4096
    assertEquals(parts.map(_.partCount).sum, 4096)
  }

  test("computePartitions: splitCount=8 covers all partitions contiguously") {
    val parts = AerospikeRDD.computePartitions(8)
    assertEquals(parts.length, 8)
    assertEquals(parts.map(_.partCount).sum, 4096)
    // Verify contiguous
    parts.sliding(2).foreach { pair =>
      assertEquals(pair(1).partBegin, pair(0).partBegin + pair(0).partCount)
    }
  }

  test("computePartitions: all splits have valid indices") {
    val parts = AerospikeRDD.computePartitions(100)
    parts.zipWithIndex.foreach { case (p, i) =>
      assertEquals(p.index, i)
    }
  }

  // --- validateSplitCount ---

  test("validateSplitCount: zero throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      Aerospike.validateSplitCount(0)
    }
  }

  test("validateSplitCount: negative throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      Aerospike.validateSplitCount(-1)
    }
  }

  test("validateSplitCount: exceeds 4096 throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      Aerospike.validateSplitCount(4097)
    }
  }

  test("validateSplitCount: valid boundary values pass") {
    Aerospike.validateSplitCount(1)
    Aerospike.validateSplitCount(4096)
  }

  // --- Constants ---
  // Smoke test: if these constants change, integration test configs and Scylla table schemas
  // must be updated to match.

  test("column name and limit constants") {
    assertEquals(Aerospike.KeyColumnName, "aero_key")
    assertEquals(Aerospike.TtlColumnName, "aero_ttl")
    assertEquals(Aerospike.GenerationColumnName, "aero_generation")
    assertEquals(Aerospike.MaxSchemaSampleSize, 10000)
  }
}
