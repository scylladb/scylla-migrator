package com.scylladb.migrator.readers

import org.apache.spark.sql.Row

class AerospikeRowIteratorTest extends munit.FunSuite {
  test("iterator is lazy and returns a valid row before a later poll fails") {
    var polls = 0
    var consumed = 0
    val failure = new IllegalStateException("scan failed")
    val rows = AerospikeRDD.validRows[Int](
      () => {
        polls += 1
        if (polls == 1) Some(42) else throw failure
      },
      value => Some(Row(value)),
      onRead = () => consumed += 1
    )
    assertEquals(consumed, 0)
    assertEquals(polls, 0)
    assert(rows.hasNext)
    assert(rows.hasNext)
    assertEquals(polls, 1)
    assertEquals(consumed, 1)
    assertEquals(rows.next(), Row(42))
    assertEquals(consumed, 1)
    assertEquals(polls, 1)
    assertEquals(intercept[IllegalStateException](rows.hasNext), failure)
  }

  test("next does not wait for a stalled subsequent poll") {
    val entered = new java.util.concurrent.CountDownLatch(1)
    val release = new java.util.concurrent.CountDownLatch(1)
    var polls = 0
    val rows = AerospikeRDD.validRows[Int](
      () => {
        polls += 1
        if (polls == 1) Some(42)
        else {
          release.await()
          None
        }
      },
      value => Some(Row(value))
    )
    val result = new java.util.concurrent.atomic.AtomicReference[Row]()
    val consumer = new Thread(() => {
      result.set(rows.next())
      entered.countDown()
    })
    consumer.setDaemon(true)
    consumer.start()
    try {
      assert(entered.await(5, java.util.concurrent.TimeUnit.SECONDS))
      assertEquals(result.get(), Row(42))
      assertEquals(polls, 1)
    } finally {
      release.countDown()
      consumer.join(5000)
    }
  }

  test("mixed and all-invalid input consumes only up to the next valid row") {
    for (input <- Seq(Seq(0, 0, 1, 0, 2, 0), Seq(0, 0))) {
      val inputRows = input.iterator
      var converted = 0
      var consumed = 0
      var completed = 0
      val rows = AerospikeRDD.validRows[Int](
        () => if (inputRows.hasNext) Some(inputRows.next()) else None,
        value => {
          converted += 1
          if (value == 0) None else Some(Row(value))
        },
        () => completed += 1,
        () => consumed += 1
      )
      assertEquals(rows.toList, input.filter(_ != 0).map(Row(_)).toList)
      assertEquals(converted, input.size)
      assertEquals(consumed, input.size)
      assertEquals(completed, 1)
      assert(!rows.hasNext)
      intercept[NoSuchElementException](rows.next())
      assertEquals(completed, 1)
    }
  }

  test("completion checks scan errors even when every record was invalid") {
    val input = Iterator(1, 2)
    val error = new RuntimeException("final scan error")
    val rows = AerospikeRDD.validRows[Int](
      () => if (input.hasNext) Some(input.next()) else None,
      _ => None,
      () => throw error
    )
    assertEquals(intercept[RuntimeException](rows.hasNext), error)
  }

  test("unexpected conversion errors propagate without dropping a record") {
    val error = new IllegalArgumentException("programming failure")
    var consumed = 0
    val rows = AerospikeRDD.validRows[Int](
      () => Some(1),
      _ => {
        assertEquals(consumed, 1)
        throw error
      },
      onRead = () => consumed += 1
    )
    assertEquals(intercept[IllegalArgumentException](rows.next()), error)
    assertEquals(consumed, 1)
  }
}
