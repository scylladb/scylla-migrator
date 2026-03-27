package com.scylladb.migrator.readers

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** Handles MySQL-specific schema transformations to ensure compatibility with ScyllaDB/CQL types
  * when writing via the Spark ScyllaDB connector.
  *
  * MySQL type → Spark JDBC type → CQL type mapping:
  *   - INT/BIGINT → IntegerType/LongType → int/bigint
  *   - VARCHAR/TEXT → StringType → text
  *   - DECIMAL → DecimalType → decimal
  *   - DATETIME/TIMESTAMP → TimestampType → timestamp
  *   - BLOB/BINARY → BinaryType → blob
  *   - FLOAT/DOUBLE → FloatType/DoubleType → float/double
  *   - DATE → DateType → date
  *   - JSON/ENUM/SET → StringType → text
  *   - UNSIGNED BIGINT → DecimalType(precision>19, scale=0) → varint
  */
object MySQLSchemaMapper {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQLSchemaMapper")

  /** Apply MySQL-specific type transformations to a DataFrame to ensure compatibility with the
    * ScyllaDB writer.
    *
    * Most MySQL types map cleanly through Spark's JDBC layer. The main special case is unsigned
    * BIGINT which comes as DecimalType(20, 0) and maps to CQL varint.
    *
    * @param df
    *   The raw DataFrame from MySQL JDBC read
    * @return
    *   Transformed DataFrame suitable for ScyllaDB write
    */
  def mapDataFrame(df: DataFrame): DataFrame =
    mapDataFramePreservingColumn(df, columnToPreserve = "")

  /** Apply MySQL-specific type transformations, skipping the column named `columnToPreserve`.
    *
    * This is used when reading with hash-based mode: the `_content_hash` column is already a string
    * and should not be subject to type transformations.
    */
  def mapDataFramePreservingColumn(df: DataFrame, columnToPreserve: String): DataFrame = {
    val schema = df.schema

    schema.fields.foreach { field =>
      if (field.name != columnToPreserve) {
        field.dataType match {
          case dt: DecimalType if dt.scale == 0 && dt.precision > 19 =>
            log.info(
              s"Column '${field.name}' has DecimalType(${dt.precision}, ${dt.scale}) - " +
                "likely MySQL UNSIGNED BIGINT. Keeping as Decimal for ScyllaDB varint compatibility."
            )
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
    }

    df
  }
}
