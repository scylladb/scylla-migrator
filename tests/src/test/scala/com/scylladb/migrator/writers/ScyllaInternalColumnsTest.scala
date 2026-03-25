package com.scylladb.migrator.writers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

class ScyllaInternalColumnsTest extends munit.FunSuite {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("ScyllaInternalColumnsTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("dropInternalColumns removes solr_query from DataFrame") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable         = true),
        StructField("value", IntegerType, nullable     = true),
        StructField("solr_query", StringType, nullable = true)
      )
    )
    val rows = java.util.Arrays.asList(
      org.apache.spark.sql.Row("a", 1, "q1"),
      org.apache.spark.sql.Row("b", 2, "q2")
    )
    val df = spark.createDataFrame(rows, schema)

    val result = Scylla.dropInternalColumns(df)

    assertEquals(result.schema.fieldNames.toSet, Set("id", "value"))
    assertEquals(result.count(), 2L)
    assertEquals(result.first().getString(0), "a")
    assertEquals(result.first().getInt(1), 1)
  }

  // In writeDataframe, dropInternalColumns runs BEFORE renames. These tests verify that
  // interaction: a source column named solr_query is dropped before any rename can touch it,
  // and a column renamed TO solr_query is not affected by the drop (it still has its original
  // name at drop time).

  test("dropInternalColumns drops solr_query before renames can act on it") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable         = true),
        StructField("solr_query", StringType, nullable = true),
        StructField("value", IntegerType, nullable     = true)
      )
    )
    val rows = java.util.Arrays.asList(
      org.apache.spark.sql.Row("a", "q1", 1)
    )
    val df = spark.createDataFrame(rows, schema)

    // Drop runs first, removing solr_query
    val cleaned = Scylla.dropInternalColumns(df)
    // A subsequent rename targeting solr_query has nothing to rename — harmless no-op
    val renamed = cleaned.withColumnRenamed("solr_query", "search")

    assertEquals(renamed.schema.fieldNames.toSet, Set("id", "value"))
  }

  test("column renamed TO solr_query survives dropInternalColumns") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable    = true),
        StructField("query", StringType, nullable = true)
      )
    )
    val rows = java.util.Arrays.asList(
      org.apache.spark.sql.Row("a", "q1")
    )
    val df = spark.createDataFrame(rows, schema)

    // Drop runs first — "query" is not an internal column, so nothing is dropped
    val cleaned = Scylla.dropInternalColumns(df)
    assertEquals(cleaned.schema.fieldNames.toSet, Set("id", "query"))

    // Rename happens after — this column now has the internal name, but the drop already ran
    val renamed = cleaned.withColumnRenamed("query", "solr_query")
    assertEquals(renamed.schema.fieldNames.toSet, Set("id", "solr_query"))
  }

  test("dropInternalColumns is a no-op when no internal columns are present") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable     = true),
        StructField("value", IntegerType, nullable = true)
      )
    )
    val rows = java.util.Arrays.asList(
      org.apache.spark.sql.Row("a", 1)
    )
    val df = spark.createDataFrame(rows, schema)

    val result = Scylla.dropInternalColumns(df)

    assertEquals(result.schema.fieldNames.toSet, Set("id", "value"))
    assertEquals(result.count(), 1L)
  }

}
