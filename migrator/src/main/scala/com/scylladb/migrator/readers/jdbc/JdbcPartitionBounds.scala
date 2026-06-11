package com.scylladb.migrator.readers.jdbc

import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  getZoneId,
  stringToDate,
  stringToTimestamp
}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Types
import java.time.DateTimeException
import scala.util.Try

/** Backend-neutral validation for Spark JDBC partitioned-read bounds.
  *
  * Spark's JDBC source accepts integer literals for numeric partition columns and date/timestamp
  * literals for DATE/TIMESTAMP columns. This object encapsulates the classification of
  * `java.sql.Types` codes and the bound-ordering check, so each JDBC backend can validate uniformly
  * before constructing a partitioned `DataFrameReader`.
  */
object JdbcPartitionBounds {

  /** Coarse partition-column category understood by Spark JDBC. */
  sealed trait PartitionColumnType {
    def description: String
  }

  object PartitionColumnType {
    case object Numeric extends PartitionColumnType {
      override val description: String = "numeric"
    }
    case object Date extends PartitionColumnType {
      override val description: String = "DATE"
    }
    case object Timestamp extends PartitionColumnType {
      override val description: String = "TIMESTAMP"
    }
  }

  /** Resolved partition column information returned by metadata lookup.
    *
    * `columnName` is the case reported by the JDBC driver in `DatabaseMetaData.getColumns`. For
    * databases that fold unquoted identifiers (PostgreSQL folds to lowercase, Oracle to uppercase),
    * this case may differ from the user-supplied configured column. When constructing Spark JDBC
    * `partitionColumn` options, callers MUST pass the value returned by this metadata lookup;
    * otherwise Spark's identifier quoting may produce a name that does not match
    * `ResultSetMetaData` and the partitioned read fails with "column not found". The example case
    * `"USER_ID"` reflects MySQL's reported case for an unquoted identifier — other backends will
    * report differently.
    */
  final case class PartitionColumnMetadata(
    columnName: String,
    jdbcTypeName: String,
    columnType: PartitionColumnType
  )

  /** Classify a JDBC type code into [[PartitionColumnType]] or fail with an explanatory error. */
  def classify(
    jdbcType: Int,
    jdbcTypeName: String,
    configuredColumn: String
  ): PartitionColumnType =
    jdbcType match {
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT | Types.FLOAT |
          Types.REAL | Types.DOUBLE | Types.NUMERIC | Types.DECIMAL =>
        PartitionColumnType.Numeric
      case Types.DATE =>
        PartitionColumnType.Date
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        PartitionColumnType.Timestamp
      case _ =>
        sys.error(
          s"Partition column '$configuredColumn' has JDBC type '$jdbcTypeName', which Spark JDBC " +
            "does not support for partitioned reads. Use a numeric, DATE, or TIMESTAMP column."
        )
    }

  private def parseNumericBound(
    partitionColumn: String,
    jdbcTypeName: String,
    boundName: String,
    bound: String
  ): Long =
    Try(bound.toLong).getOrElse(
      sys.error(
        s"Partition column '$partitionColumn' has JDBC type '$jdbcTypeName', so $boundName " +
          s"must be an integer literal. Got '$bound'."
      )
    )

  private def parseTemporalBound(
    partitionColumn: String,
    jdbcTypeName: String,
    expectedLiteral: String,
    boundName: String,
    bound: String,
    parser: String => Option[Long]
  ): Long =
    parser(bound).getOrElse(
      sys.error(
        s"Partition column '$partitionColumn' has JDBC type '$jdbcTypeName', so $boundName " +
          s"must be a $expectedLiteral literal. Got '$bound'. " +
          "Epoch-millisecond bounds are not supported for temporal JDBC partition columns."
      )
    )

  /** Validate that `lowerBound < upperBound` and that both bounds parse against `columnType`.
    * Throws `RuntimeException` (via `sys.error`) on rejection, matching the legacy MySQL reader
    * contract.
    *
    * `timeZoneId` is the Spark session timezone, used to parse TIMESTAMP bounds.
    */
  def validateBounds(
    partitionColumn: String,
    jdbcTypeName: String,
    columnType: PartitionColumnType,
    lowerBound: String,
    upperBound: String,
    timeZoneId: String
  ): Unit = {
    // Resolve the zone id once per validation. Lazy so callers using only Numeric/Date columns
    // do not trigger TimeZone parsing — and so an invalid spark.sql.session.timeZone surfaces
    // as a descriptive sys.error rather than a raw DateTimeException stack.
    lazy val sessionZoneIdOrFail =
      try getZoneId(timeZoneId)
      catch {
        case _: DateTimeException =>
          sys.error(
            s"Invalid session timezone '$timeZoneId' for TIMESTAMP partition bound parsing. " +
              "Set 'spark.sql.session.timeZone' to a valid IANA zone (e.g. 'UTC', 'America/Los_Angeles')."
          )
      }

    def parse(boundName: String, bound: String): Long = columnType match {
      case PartitionColumnType.Numeric =>
        parseNumericBound(partitionColumn, jdbcTypeName, boundName, bound)
      case PartitionColumnType.Date =>
        parseTemporalBound(
          partitionColumn,
          jdbcTypeName,
          expectedLiteral = "DATE",
          boundName       = boundName,
          bound           = bound,
          parser          = value => stringToDate(UTF8String.fromString(value)).map(_.toLong)
        )
      case PartitionColumnType.Timestamp =>
        parseTemporalBound(
          partitionColumn,
          jdbcTypeName,
          expectedLiteral = "TIMESTAMP",
          boundName       = boundName,
          bound           = bound,
          parser = value => stringToTimestamp(UTF8String.fromString(value), sessionZoneIdOrFail)
        )
    }

    val lowerValue = parse("lowerBound", lowerBound)
    val upperValue = parse("upperBound", upperBound)

    if (lowerValue >= upperValue)
      sys.error(
        s"lowerBound ('$lowerBound') must be less than upperBound ('$upperBound') " +
          s"for partition column '$partitionColumn' ($jdbcTypeName)."
      )
  }
}
