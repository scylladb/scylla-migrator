package com.scylladb.migrator.readers

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.{ LogEvent, Logger }
import org.apache.logging.log4j.core.appender.{ AbstractAppender, AppenderLoggingException }
import org.apache.logging.log4j.core.config.Property
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class AerospikeMetricsTest extends munit.FunSuite {
  private def metrics(read: Long, invalid: Long, skipped: Long): Aerospike.Metrics = {
    def counter(value: Long): LongAccumulator = {
      val accumulator = new LongAccumulator
      accumulator.add(value)
      accumulator
    }
    Aerospike.Metrics(counter(read), counter(invalid), counter(skipped))
  }

  private def withAppender(appendEvent: LogEvent => Unit)(run: => Unit): Unit = {
    val logger = Aerospike.log.asInstanceOf[Logger]
    val appender =
      new AbstractAppender("aerospike-metrics-test", null, null, false, Array.empty[Property]) {
        override def append(event: LogEvent): Unit = appendEvent(event.toImmutable)
      }
    val previousLevel = logger.getLevel
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(Level.INFO)
    try run
    finally {
      logger.removeAppender(appender)
      logger.setLevel(previousLevel)
      appender.stop()
    }
  }

  test("successful migration reports the existing evaluated-record summary") {
    for ((invalid, skipped, level) <- Seq((0L, 0L, Level.INFO), (2L, 1L, Level.WARN))) {
      val events = ArrayBuffer.empty[LogEvent]
      val counts = metrics(3L, invalid, skipped)
      var migrated = false
      withAppender { event => events += event; () } {
        counts.withMigrationReport { migrated = true }
      }
      assert(migrated)
      assertEquals(events.size, 1)
      assertEquals(events.head.getLevel, level)
      assertEquals(
        events.head.getMessage.getFormattedMessage,
        s"Aerospike source evaluated records: read=3, invalid=$invalid, skipped=$skipped. Counts can include replay."
      )
    }
  }

  test(
    "failed migration reports only explicitly incomplete counts and rethrows its original error"
  ) {
    val events = ArrayBuffer.empty[LogEvent]
    val failure = new IllegalStateException("migration failed")
    withAppender { event => events += event; () } {
      val thrown = intercept[IllegalStateException] {
        metrics(7L, 3L, 2L).withMigrationReport(throw failure)
      }
      assert(thrown eq failure)
    }
    assertEquals(events.size, 1)
    assertEquals(events.head.getLevel, Level.WARN)
    assertEquals(
      events.head.getMessage.getFormattedMessage,
      "Aerospike source INCOMPLETE evaluated records after migration failure: read=7, invalid=3, skipped=2. " +
        "Best-effort partial counts can include replay and omit accumulator updates from failed tasks."
    )
  }

  test("logger failure cannot replace a migration failure and still propagates after success") {
    val failure = new IllegalStateException("migration failed")
    var reportAttempts = 0
    withAppender { _ =>
      reportAttempts += 1
      throw new AppenderLoggingException("diagnostic logger failed")
    } {
      val counts = metrics(1L, 0L, 0L)
      val thrown = intercept[IllegalStateException] {
        counts.withMigrationReport(throw failure)
      }
      assert(thrown eq failure)
      assertEquals(reportAttempts, 1)
      intercept[AppenderLoggingException](counts.withMigrationReport(()))
      assertEquals(reportAttempts, 2)
    }
  }

  test("counter acquisition failure cannot replace the original migration Throwable") {
    val failure = new AssertionError("migration failed")
    var valueAttempts = 0
    val unavailable = new LongAccumulator {
      override def value: java.lang.Long = {
        valueAttempts += 1
        throw new AssertionError("counter unavailable")
      }
    }
    val counts = Aerospike.Metrics(unavailable, new LongAccumulator, new LongAccumulator)
    val thrown = intercept[AssertionError] {
      counts.withMigrationReport(throw failure)
    }
    assert(thrown eq failure)
    assertEquals(valueAttempts, 1)
  }
}
