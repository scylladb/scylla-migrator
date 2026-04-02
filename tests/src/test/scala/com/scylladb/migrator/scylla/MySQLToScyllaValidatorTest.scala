package com.scylladb.migrator.scylla

import com.scylladb.migrator.readers.MySQL
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.spark.sql.SparkSession

class MySQLToScyllaValidatorTest extends munit.FunSuite {

  private def areDifferent(
    left: Option[Any],
    right: Option[Any],
    fpTol: Double = 0.0,
    tsTol: Long = 0L
  ): Boolean =
    RowComparisonFailure.areDifferent(left, right, tsTol, fpTol)

  // --- Null / None handling ---

  test("both None are equal") {
    assert(!areDifferent(None, None))
  }

  test("Some vs None are different") {
    assert(areDifferent(Some("a"), None))
  }

  test("None vs Some are different") {
    assert(areDifferent(None, Some("a")))
  }

  // --- Basic equality ---

  test("equal strings") {
    assert(!areDifferent(Some("hello"), Some("hello")))
  }

  test("different strings") {
    assert(areDifferent(Some("hello"), Some("world")))
  }

  test("equal integers") {
    assert(!areDifferent(Some(42), Some(42)))
  }

  test("different integers") {
    assert(areDifferent(Some(42), Some(43)))
  }

  // --- Timestamp comparison ---

  test("java.sql.Timestamp within tolerance") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1050L)
    assert(!areDifferent(Some(t1), Some(t2), tsTol = 100L))
  }

  test("java.sql.Timestamp outside tolerance") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1200L)
    assert(areDifferent(Some(t1), Some(t2), tsTol = 100L))
  }

  test("java.sql.Timestamp with zero tolerance uses standard equality") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1001L)
    // With tsTol = 0, the tolerance branch IS entered; Math.abs(diff) > 0 is exact equality
    assert(areDifferent(Some(t1), Some(t2), tsTol = 0L))
  }

  test("java.time.Instant within tolerance") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1050L)
    assert(!areDifferent(Some(i1), Some(i2), tsTol = 100L))
  }

  test("java.time.Instant outside tolerance") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1200L)
    assert(areDifferent(Some(i1), Some(i2), tsTol = 100L))
  }

  test("java.time.Instant with zero tolerance uses standard equality") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1001L)
    assert(areDifferent(Some(i1), Some(i2), tsTol = 0L))
  }

  // --- Float comparison ---

  test("floats within tolerance") {
    assert(!areDifferent(Some(1.0f), Some(1.001f), fpTol = 0.01))
  }

  test("floats outside tolerance") {
    assert(areDifferent(Some(1.0f), Some(1.1f), fpTol = 0.01))
  }

  // --- Double comparison ---

  test("doubles within tolerance") {
    assert(!areDifferent(Some(1.0d), Some(1.001d), fpTol = 0.01))
  }

  test("doubles outside tolerance") {
    assert(areDifferent(Some(1.0d), Some(1.1d), fpTol = 0.01))
  }

  // --- NaN / Infinity edge cases (bug #3) ---

  test("Double NaN values are considered equal") {
    assert(!areDifferent(Some(Double.NaN), Some(Double.NaN), fpTol = 0.01))
  }

  test("Float NaN values are considered equal") {
    assert(!areDifferent(Some(Float.NaN), Some(Float.NaN), fpTol = 0.01))
  }

  test("Double NaN vs regular value are different") {
    assert(areDifferent(Some(Double.NaN), Some(1.0d), fpTol = 0.01))
  }

  test("Float NaN vs regular value are different") {
    assert(areDifferent(Some(Float.NaN), Some(1.0f), fpTol = 0.01))
  }

  test("Double positive infinity values are equal") {
    assert(
      !areDifferent(Some(Double.PositiveInfinity), Some(Double.PositiveInfinity), fpTol = 0.01)
    )
  }

  test("Double positive infinity vs negative infinity are different") {
    assert(
      areDifferent(Some(Double.PositiveInfinity), Some(Double.NegativeInfinity), fpTol = 0.01)
    )
  }

  // --- BigDecimal comparison ---

  test("BigDecimals within tolerance") {
    val a = new java.math.BigDecimal("100.001")
    val b = new java.math.BigDecimal("100.002")
    assert(!areDifferent(Some(a), Some(b), fpTol = 0.01))
  }

  test("BigDecimals outside tolerance") {
    val a = new java.math.BigDecimal("100.0")
    val b = new java.math.BigDecimal("100.1")
    assert(areDifferent(Some(a), Some(b), fpTol = 0.01))
  }

  // --- Byte array comparison ---

  test("equal byte arrays") {
    val a = Array[Byte](1, 2, 3)
    val b = Array[Byte](1, 2, 3)
    assert(!areDifferent(Some(a), Some(b)))
  }

  test("different byte arrays") {
    val a = Array[Byte](1, 2, 3)
    val b = Array[Byte](1, 2, 4)
    assert(areDifferent(Some(a), Some(b)))
  }

  // --- Generic array comparison ---

  test("equal generic arrays") {
    val a = Array("a", "b")
    val b = Array("a", "b")
    assert(!areDifferent(Some(a), Some(b)))
  }

  test("different generic arrays") {
    val a = Array("a", "b")
    val b = Array("a", "c")
    assert(areDifferent(Some(a), Some(b)))
  }

  // --- prefixColumns tests (Spark-local) ---

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("MySQLToScyllaValidatorTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("prefixColumns renames all columns atomically") {
    import spark.implicits._
    val df = Seq((1, "a"), (2, "b")).toDF("id", "name")
    val prefixed = MySQLToScyllaValidator.prefixColumns(df, "src_")
    assertEquals(prefixed.columns.toList, List("src_id", "src_name"))
    assertEquals(prefixed.count(), 2L)
  }

  test("prefixColumns handles columns that match prefix pattern") {
    import spark.implicits._
    // Column "src_status" already looks like a prefixed name -- should not collide
    val df = Seq((1, "ok", "active")).toDF("status", "src_status", "flag")
    val prefixed = MySQLToScyllaValidator.prefixColumns(df, "src_")
    assertEquals(prefixed.columns.sorted.toList, List("src_flag", "src_src_status", "src_status"))
    assertEquals(prefixed.count(), 1L)
  }

  // --- addContentHash tests (Spark-local) ---

  test("addContentHash adds hash column and drops hashed columns") {
    import spark.implicits._
    val df = Seq((1, "hello", 42)).toDF("id", "text", "num")
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text", "num"), List("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
    assert(!hashed.columns.contains("text"))
    assert(!hashed.columns.contains("num"))
    assert(hashed.columns.contains("id"))
  }

  test("addContentHash preserves PK columns even if they are in hashCols") {
    import spark.implicits._
    val df = Seq((1, "hello")).toDF("id", "text")
    // id is in pkCols, so it should not be dropped even though we hash text
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    assert(hashed.columns.contains("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
  }

  test("addContentHash returns DF unchanged when no hash columns exist") {
    import spark.implicits._
    val df = Seq((1, "hello")).toDF("id", "text")
    val result =
      MySQLToScyllaValidator.addContentHash(df, List("nonexistent"), List("id"))
    assertEquals(result.columns.toSet, df.columns.toSet)
    assert(!result.columns.contains(MySQL.ContentHashColumn))
  }

  test("addContentHash handles NULL values without error") {
    import spark.implicits._
    val df = Seq((1, Option.empty[String]), (2, Some("hello"))).toDF("id", "text")
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
    assertEquals(hashed.count(), 2L)
  }

  test("addContentHash distinguishes NULL from the literal string 'NULL'") {
    import spark.implicits._
    val df = Seq(
      (1, Option.empty[String]),
      (2, Some("NULL"))
    ).toDF("id", "text")
    val hashed = MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    val hashes = hashed.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertNotEquals(hashes(0), hashes(1))
  }

  test("addContentHash produces same hash regardless of DataFrame column order") {
    import spark.implicits._
    // Simulate source with columns in one order and target with columns in different order.
    // The hash should be identical for the same data.
    val df1 = Seq((1, "hello", 42)).toDF("id", "Name", "Age")
    val df2 = Seq((1, 42, "hello")).toDF("id", "age", "name")
    val hashed1 =
      MySQLToScyllaValidator.addContentHash(df1, List("Name", "Age"), List("id"))
    val hashed2 =
      MySQLToScyllaValidator.addContentHash(df2, List("name", "age"), List("id"))
    val hash1 = hashed1.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    val hash2 = hashed2.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertEquals(hash1.toList, hash2.toList)
  }
}
