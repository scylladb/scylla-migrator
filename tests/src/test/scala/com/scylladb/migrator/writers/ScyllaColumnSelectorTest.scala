package com.scylladb.migrator.writers

import com.datastax.spark.connector._
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

class ScyllaColumnSelectorTest extends munit.FunSuite {

  test("buildColumnSelector excludes ttl and writetime columns when timestampColumns is present") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("foo", StringType, nullable = true),
        StructField("bar", IntegerType, nullable = true),
        StructField("ttl", IntegerType, nullable = true),
        StructField("writetime", LongType, nullable = true)
      )
    )

    val timestampColumns = Some(TimestampColumns("ttl", "writetime"))
    val selector = Scylla.buildColumnSelector(schema, timestampColumns)

    val SomeColumns(columns @ _*) = selector
    val columnNames = columns.map { case c: ColumnName => c.columnName; case other => fail(s"unexpected ref: $other") }.toSet
    assertEquals(columnNames, Set("id", "foo", "bar"))
  }

  test("buildColumnSelector includes all columns when timestampColumns is None") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("foo", StringType, nullable = true),
        StructField("bar", IntegerType, nullable = true)
      )
    )

    val selector = Scylla.buildColumnSelector(schema, None)

    val SomeColumns(columns @ _*) = selector
    val columnNames = columns.map { case c: ColumnName => c.columnName; case other => fail(s"unexpected ref: $other") }.toSet
    assertEquals(columnNames, Set("id", "foo", "bar"))
  }

  test(
    "buildColumnSelector excludes only the specific metadata columns named in timestampColumns"
  ) {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("value", StringType, nullable = true),
        StructField("custom_ttl", IntegerType, nullable = true),
        StructField("custom_wt", LongType, nullable = true),
        StructField("ttl", IntegerType, nullable = true)
      )
    )

    val timestampColumns = Some(TimestampColumns("custom_ttl", "custom_wt"))
    val selector = Scylla.buildColumnSelector(schema, timestampColumns)

    val SomeColumns(columns @ _*) = selector
    val columnNames = columns.map { case c: ColumnName => c.columnName; case other => fail(s"unexpected ref: $other") }.toSet
    assertEquals(columnNames, Set("id", "value", "ttl"))
  }

}
