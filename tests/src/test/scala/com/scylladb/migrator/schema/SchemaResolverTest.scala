package com.scylladb.migrator.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

class SchemaResolverTest extends munit.FunSuite {

  // --- resolveFieldName ---

  test("resolveFieldName finds exact case match") {
    val fields = Array("userId", "email", "age")
    assertEquals(SchemaResolver.resolveFieldName(fields, "email"), "email")
  }

  test("resolveFieldName finds case-insensitive match") {
    val fields = Array("UserId", "Email", "Age")
    assertEquals(SchemaResolver.resolveFieldName(fields, "userid"), "UserId")
    assertEquals(SchemaResolver.resolveFieldName(fields, "EMAIL"), "Email")
  }

  test("resolveFieldName prefers exact-case match over case-insensitive") {
    val fields = Array("Status", "status")
    assertEquals(SchemaResolver.resolveFieldName(fields, "status"), "status")
    assertEquals(SchemaResolver.resolveFieldName(fields, "Status"), "Status")
  }

  // --- findFieldName ---

  test("findFieldName returns Some for exact match") {
    val fields = Array("id", "Name")
    assertEquals(SchemaResolver.findFieldName(fields, "id"), Some("id"))
  }

  test("findFieldName returns Some for case-insensitive match") {
    val fields = Array("UserId", "Email")
    assertEquals(SchemaResolver.findFieldName(fields, "userid"), Some("UserId"))
  }

  test("findFieldName returns None when not found") {
    val fields = Array("id", "name")
    assertEquals(SchemaResolver.findFieldName(fields, "missing"), None)
  }

  test("resolveFieldName throws with diagnostic when not found") {
    val fields = Array("id", "name")
    val error = intercept[RuntimeException] {
      SchemaResolver.resolveFieldName(fields, "missing")
    }
    assert(error.getMessage.contains("missing"))
    assert(error.getMessage.contains("id, name"))
  }

  // --- escapeSparkColumnName ---

  test("escapeSparkColumnName wraps in backticks") {
    assertEquals(SchemaResolver.escapeSparkColumnName("col"), "`col`")
  }

  test("escapeSparkColumnName doubles embedded backticks") {
    assertEquals(SchemaResolver.escapeSparkColumnName("a`b"), "`a``b`")
  }

  test("escapeSparkColumnName handles dots (no splitting)") {
    assertEquals(SchemaResolver.escapeSparkColumnName("user.name"), "`user.name`")
  }

  // --- requireNoCollisions ---

  test("requireNoCollisions passes for unique names") {
    SchemaResolver.requireNoCollisions(Seq("a", "b", "c"), "test context")
  }

  test("requireNoCollisions throws on case-insensitive collision") {
    val error = intercept[IllegalArgumentException] {
      SchemaResolver.requireNoCollisions(Seq("Foo", "foo", "bar"), "after renames")
    }
    assert(error.getMessage.contains("Foo"))
    assert(error.getMessage.contains("foo"))
    assert(error.getMessage.contains("after renames"))
  }

  // --- validateColumnPresence ---

  test("validateColumnPresence passes when all present") {
    SchemaResolver.validateColumnPresence(
      Seq("id", "name"),
      Seq("ID", "Name", "Extra"),
      "in target"
    )
  }

  test("validateColumnPresence throws listing missing columns") {
    val error = intercept[RuntimeException] {
      SchemaResolver.validateColumnPresence(
        Seq("id", "missing1", "missing2"),
        Seq("id", "other"),
        "in target"
      )
    }
    assert(error.getMessage.contains("missing1"))
    assert(error.getMessage.contains("missing2"))
    assert(error.getMessage.contains("in target"))
  }

  // --- columnsOnlyIn ---

  test("columnsOnlyIn returns columns in superset but not in subset") {
    val result = SchemaResolver.columnsOnlyIn(
      Seq("A", "B", "C"),
      Seq("a", "c")
    )
    assertEquals(result, Seq("B"))
  }

  test("columnsOnlyIn returns empty when all present") {
    val result = SchemaResolver.columnsOnlyIn(Seq("x", "y"), Seq("X", "Y"))
    assertEquals(result, Seq.empty[String])
  }

  // --- prefixColumns and selectAndAlias (require SparkSession) ---

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SchemaResolverTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("prefixColumns adds prefix to all column names") {
    import spark.implicits._
    val df = Seq((1, "a")).toDF("id", "name")
    val prefixed = SchemaResolver.prefixColumns(df, "src_")
    assertEquals(prefixed.columns.toList, List("src_id", "src_name"))
  }

  test("selectAndAlias resolves case-insensitively and aliases to requested name") {
    import spark.implicits._
    val df = Seq((1, "a", 42)).toDF("ID", "Name", "Age")
    val selected = SchemaResolver.selectAndAlias(df, Seq("id", "name"))
    assertEquals(selected.columns.toList, List("id", "name"))
    val rows = selected.collect()
    assertEquals(rows.length, 1)
    assertEquals(rows(0).getInt(0), 1)
    assertEquals(rows(0).getString(1), "a")
  }
}
