package com.scylladb.migrator.readers.jdbc

import java.sql.Types

class JdbcPartitionBoundsTest extends munit.FunSuite {

  import JdbcPartitionBounds.PartitionColumnType

  test("classify maps integral and decimal JDBC types to Numeric") {
    val numericCases = Seq(
      Types.TINYINT,
      Types.SMALLINT,
      Types.INTEGER,
      Types.BIGINT,
      Types.FLOAT,
      Types.REAL,
      Types.DOUBLE,
      Types.NUMERIC,
      Types.DECIMAL
    )

    numericCases.foreach { jdbcType =>
      assertEquals(
        JdbcPartitionBounds.classify(jdbcType, "TYPE", "col"),
        PartitionColumnType.Numeric: PartitionColumnType
      )
    }
  }

  test("classify maps DATE and TIMESTAMP families") {
    assertEquals(
      JdbcPartitionBounds.classify(Types.DATE, "DATE", "col"),
      PartitionColumnType.Date: PartitionColumnType
    )
    assertEquals(
      JdbcPartitionBounds.classify(Types.TIMESTAMP, "TIMESTAMP", "col"),
      PartitionColumnType.Timestamp: PartitionColumnType
    )
    assertEquals(
      JdbcPartitionBounds.classify(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TZ", "col"),
      PartitionColumnType.Timestamp: PartitionColumnType
    )
  }

  test("classify rejects unsupported JDBC types with a descriptive message") {
    val err = intercept[RuntimeException] {
      JdbcPartitionBounds.classify(Types.VARCHAR, "VARCHAR", "name")
    }
    assert(err.getMessage.contains("Partition column 'name'"))
    assert(err.getMessage.contains("VARCHAR"))
  }

  test("validateBounds accepts numeric bounds for numeric columns") {
    JdbcPartitionBounds.validateBounds(
      partitionColumn = "id",
      jdbcTypeName    = "BIGINT",
      columnType      = PartitionColumnType.Numeric,
      lowerBound      = "1",
      upperBound      = "100",
      timeZoneId      = "UTC"
    )
  }

  test("validateBounds rejects equal numeric bounds") {
    val err = intercept[RuntimeException] {
      JdbcPartitionBounds.validateBounds(
        partitionColumn = "id",
        jdbcTypeName    = "BIGINT",
        columnType      = PartitionColumnType.Numeric,
        lowerBound      = "5",
        upperBound      = "5",
        timeZoneId      = "UTC"
      )
    }
    assert(err.getMessage.contains("must be less than"))
  }

  test("validateBounds accepts ISO date literals for DATE columns") {
    JdbcPartitionBounds.validateBounds(
      partitionColumn = "created_on",
      jdbcTypeName    = "DATE",
      columnType      = PartitionColumnType.Date,
      lowerBound      = "2024-01-01",
      upperBound      = "2024-02-01",
      timeZoneId      = "UTC"
    )
  }

  test("validateBounds rejects epoch-millis for DATE columns") {
    val err = intercept[RuntimeException] {
      JdbcPartitionBounds.validateBounds(
        partitionColumn = "created_on",
        jdbcTypeName    = "DATE",
        columnType      = PartitionColumnType.Date,
        lowerBound      = "1704067200000",
        upperBound      = "1704153600000",
        timeZoneId      = "UTC"
      )
    }
    assert(err.getMessage.contains("Epoch-millisecond bounds are not supported"))
  }

  test("validateBounds rejects non-integer numeric bounds") {
    val err = intercept[RuntimeException] {
      JdbcPartitionBounds.validateBounds(
        partitionColumn = "id",
        jdbcTypeName    = "BIGINT",
        columnType      = PartitionColumnType.Numeric,
        lowerBound      = "1.5",
        upperBound      = "100",
        timeZoneId      = "UTC"
      )
    }
    assert(err.getMessage.contains("must be an integer literal"))
  }

  test("validateBounds accepts ISO timestamps for TIMESTAMP columns and orders them") {
    JdbcPartitionBounds.validateBounds(
      partitionColumn = "created_at",
      jdbcTypeName    = "TIMESTAMP",
      columnType      = PartitionColumnType.Timestamp,
      lowerBound      = "2024-01-01 00:00:00",
      upperBound      = "2024-01-02 00:00:00",
      timeZoneId      = "UTC"
    )
  }

  test(
    "validateBounds surfaces an invalid timezone as a descriptive sys.error, not a raw DateTimeException"
  ) {
    // Regression: a malformed `spark.sql.session.timeZone` (e.g. `GMT+99:00`) caused `getZoneId`
    // to throw `java.time.DateTimeException`, escaping the migrator's error-handling contract
    // with an opaque stack. We now catch it and report the offending zone in a way an operator
    // can act on.
    val err = intercept[RuntimeException] {
      JdbcPartitionBounds.validateBounds(
        partitionColumn = "created_at",
        jdbcTypeName    = "TIMESTAMP",
        columnType      = PartitionColumnType.Timestamp,
        lowerBound      = "2024-01-01 00:00:00",
        upperBound      = "2024-01-02 00:00:00",
        timeZoneId      = "GMT+99:00"
      )
    }
    assert(
      err.getMessage.contains("Invalid session timezone"),
      s"unexpected error message: ${err.getMessage}"
    )
    assert(err.getMessage.contains("GMT+99:00"))
  }

  test("validateBounds does not parse the timezone for non-temporal column types") {
    // Lazy zone-id resolution: numeric bounds must succeed even when the session timezone is
    // unparseable, because the timezone is irrelevant to numeric parsing.
    JdbcPartitionBounds.validateBounds(
      partitionColumn = "id",
      jdbcTypeName    = "BIGINT",
      columnType      = PartitionColumnType.Numeric,
      lowerBound      = "1",
      upperBound      = "100",
      timeZoneId      = "GMT+99:00"
    )
  }
}
