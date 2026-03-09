package com.scylladb.migrator.readers

import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Handles MySQL-specific schema transformations to ensure compatibility
  * with ScyllaDB/CQL types when writing via the Spark ScyllaDB connector.
  *
  * MySQL type -> Spark JDBC type -> CQL type mapping:
  *   INT/BIGINT        -> IntegerType/LongType     -> int/bigint
  *   VARCHAR/TEXT       -> StringType               -> text
  *   DECIMAL            -> DecimalType              -> decimal
  *   DATETIME/TIMESTAMP -> TimestampType            -> timestamp
  *   BLOB/BINARY        -> BinaryType               -> blob
  *   BOOLEAN/TINYINT(1) -> BooleanType/IntegerType  -> boolean/int
  *   FLOAT/DOUBLE       -> FloatType/DoubleType     -> float/double
  *   DATE               -> DateType                 -> date
  *   JSON               -> StringType               -> text
  *   ENUM               -> StringType               -> text
  *   BIT                -> BinaryType/BooleanType   -> blob/boolean
  *   UNSIGNED BIGINT    -> DecimalType              -> varint
  *   YEAR               -> IntegerType/DateType     -> int
  *   TIME               -> TimestampType            -> text (as duration string)
  *   SET                -> StringType               -> text
  */
object MySQLSchemaMapper {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQLSchemaMapper")

  /**
    * Apply MySQL-specific type transformations to a DataFrame to ensure
    * compatibility with the ScyllaDB writer.
    *
    * Most MySQL types map cleanly through Spark's JDBC layer, but some
    * need explicit handling:
    *   - Unsigned BIGINT comes as DecimalType and may need conversion
    *   - BIT fields may come as binary arrays
    *   - Zero dates (0000-00-00) are handled by the JDBC URL parameter
    *   - YEAR type may need special handling
    *
    * @param df The raw DataFrame from MySQL JDBC read
    * @return Transformed DataFrame suitable for ScyllaDB write
    */
  def mapDataFrame(df: DataFrame): DataFrame = {
    val schema = df.schema
    var result = df

    schema.fields.foreach { field =>
      field.dataType match {
        case ByteType =>
          log.info(
            s"Converting ByteType column '${field.name}' to BooleanType " +
              "(MySQL BOOLEAN/TINYINT(1): 0 -> false, non-zero -> true)")
          result = result.withColumn(
            field.name,
            when(col(field.name).isNull, lit(null).cast(BooleanType))
              .otherwise(col(field.name) =!= lit(0.toByte))
          )

        case BinaryType if isSingleByteBinary(df, field.name) =>
          log.info(
            s"Converting single-byte binary column '${field.name}' to BooleanType (likely MySQL BIT(1))")
          result = result.withColumn(
            field.name,
            when(col(field.name).isNull, lit(null).cast(BooleanType))
              .otherwise(conv(hex(col(field.name)), 16, 10).cast(IntegerType) =!= 0)
          )

        case dt: DecimalType if dt.scale == 0 && dt.precision > 19 =>
          log.info(
            s"Column '${field.name}' has DecimalType(${dt.precision}, ${dt.scale}) - " +
              "likely MySQL UNSIGNED BIGINT. Keeping as Decimal for ScyllaDB varint compatibility.")

        case _: DecimalType =>
          log.debug(s"Column '${field.name}' is DecimalType - maps to CQL decimal")

        case TimestampType =>
          log.debug(s"Column '${field.name}' is TimestampType - maps to CQL timestamp")

        case DateType =>
          log.debug(s"Column '${field.name}' is DateType - maps to CQL date")

        case _ =>
          log.debug(s"Column '${field.name}' is ${field.dataType} - standard mapping")
      }
    }

    result
  }

  /**
    * Check whether a binary column consistently contains single-byte values,
    * which indicates it's likely a MySQL BIT(1) column.
    * Uses a sampling approach to avoid scanning the entire table.
    */
  private def isSingleByteBinary(df: DataFrame, columnName: String): Boolean =
    try {
      val sample = df
        .select(length(col(columnName)).as("len"))
        .filter(col("len").isNotNull)
        .limit(100)
        .collect()
      sample.nonEmpty && sample.forall(_.getAs[Int]("len") <= 1)
    } catch {
      case _: Exception => false
    }
}
