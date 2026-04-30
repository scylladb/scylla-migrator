package com.scylladb.migrator.readers

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** Handles MySQL-specific schema inspection to log how MySQL types map to CQL types via the Spark
  * ScyllaDB connector. Currently no transformations are performed -- the connector handles type
  * conversion at write time based on the target CQL schema. The methods below are intentionally
  * no-ops that only log diagnostic information.
  *
  * MySQL type -> Spark JDBC type -> CQL type mapping:
  *   - INT/BIGINT -> IntegerType/LongType -> int/bigint
  *   - VARCHAR/TEXT -> StringType -> text
  *   - DECIMAL -> DecimalType -> decimal
  *   - DATETIME/TIMESTAMP -> TimestampType -> timestamp
  *   - BLOB/BINARY -> BinaryType -> blob
  *   - FLOAT/DOUBLE -> FloatType/DoubleType -> float/double
  *   - DATE -> DateType -> date
  *   - JSON/ENUM/SET -> StringType -> text
  *   - UNSIGNED BIGINT -> DecimalType(precision>19, scale=0) -> varint
  */
object MySQLSchemaLogger {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQLSchemaLogger")

  /** Log schema information for a MySQL DataFrame.
    *
    * No actual transformations are applied -- the Spark ScyllaDB connector handles type conversion.
    * The main special case worth noting is unsigned BIGINT which comes as DecimalType(20, 0) and
    * maps to CQL varint.
    *
    * @param df
    *   The raw DataFrame from MySQL JDBC read
    */
  def logSchemaInfo(df: DataFrame): Unit = {
    val schema = df.schema

    schema.fields.foreach { field =>
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
}
