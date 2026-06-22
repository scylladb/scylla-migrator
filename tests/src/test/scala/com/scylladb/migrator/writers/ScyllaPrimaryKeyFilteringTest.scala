package com.scylladb.migrator.writers

import com.scylladb.migrator.config.Rename
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

class ScyllaPrimaryKeyFilteringTest extends munit.FunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("ScyllaPrimaryKeyFilteringTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("resolvePrimaryKeyColumns resolves renamed and case-insensitive primary key columns") {
    val schema = StructType(
      Seq(
        StructField("ID", StringType, nullable             = true),
        StructField("ClusteringKey", IntegerType, nullable = true),
        StructField("value", StringType, nullable          = true)
      )
    )

    val targetPkNames = Set("id_target", "clusteringkey")
    val renames = List(Rename("ID", "id_target"))

    val resolution = Scylla.resolvePrimaryKeyColumns(targetPkNames, renames, schema)

    assertEquals(resolution.unresolvedSourcePkNames, Set.empty[String])
    assertEquals(resolution.resolvedSourcePkNames, Set("ID", "ClusteringKey"))
    assertEquals(
      resolution.fieldIndices.toSet,
      Set(schema.fieldIndex("ID"), schema.fieldIndex("ClusteringKey"))
    )
  }

  test("requireAllPrimaryKeysResolved throws when not all primary key columns are found") {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable    = true),
        StructField("value", StringType, nullable = true)
      )
    )

    val targetPkNames = Set("id", "missing_pk")
    val resolution = Scylla.resolvePrimaryKeyColumns(targetPkNames, Nil, schema)

    val error = intercept[IllegalArgumentException] {
      Scylla.requireAllPrimaryKeysResolved(targetPkNames, resolution)
    }

    assert(error.getMessage.contains("Cannot resolve all primary key columns"))
    assert(error.getMessage.contains("missing_pk"))
  }

  test("dropRowsWithNullPrimaryKeys drops invalid rows and tracks dropped count") {
    val schema = StructType(
      Seq(
        StructField("pk1", StringType, nullable   = true),
        StructField("pk2", StringType, nullable   = true),
        StructField("value", StringType, nullable = true)
      )
    )

    val sourceRows = Seq(
      Row("a", "1", "ok-1"),
      Row(null, "2", "bad-1"),
      Row("c", null, "bad-2"),
      Row("d", "4", "ok-2")
    )
    val sourceRdd = spark.sparkContext.parallelize(sourceRows)
    val droppedRows = spark.sparkContext.longAccumulator("dropped-null-pk-rows-test")

    val filtered = Scylla.dropRowsWithNullPrimaryKeys(
      sourceRdd,
      Array(schema.fieldIndex("pk1"), schema.fieldIndex("pk2")),
      droppedRows
    )

    val keptRows =
      filtered.collect().map(row => (row.getString(0), row.getString(1), row.getString(2))).toSet
    assertEquals(keptRows, Set(("a", "1", "ok-1"), ("d", "4", "ok-2")))
    assert(droppedRows.value == 2L)
  }

  test("requireNoCaseInsensitiveColumnNameCollisions accepts unique names") {
    Scylla.requireNoCaseInsensitiveColumnNameCollisions(
      Seq("id", "Value", "payload"),
      "while testing"
    )
  }

  test("requireNoCaseInsensitiveColumnNameCollisions rejects duplicate names after renames") {
    import spark.implicits._

    val df = Seq((1, 2)).toDF("a", "b")
    val renamedSchema = df.withColumnRenamed("a", "b").schema

    val error = intercept[IllegalArgumentException] {
      Scylla.requireNoCaseInsensitiveColumnNameCollisions(
        renamedSchema.fieldNames.toSeq,
        "after applying renames before writing to ScyllaDB"
      )
    }

    assert(
      error.getMessage.contains("Column name collision detected")
    )
    assert(error.getMessage.contains("[b]"))
  }

  test("requireNoCaseInsensitiveColumnNameCollisions rejects case-only duplicates") {
    val error = intercept[IllegalArgumentException] {
      Scylla.requireNoCaseInsensitiveColumnNameCollisions(
        Seq("UserId", "userid", "value"),
        "while testing"
      )
    }

    assert(error.getMessage.contains("[UserId, userid]"))
  }

  // Regression guard: the reverse-rename lookup in resolvePrimaryKeyColumns must match target
  // primary-key names CASE-SENSITIVELY. CQL identifiers are case-sensitive when quoted, so a
  // table may legitimately contain two distinct columns whose names differ only in case
  // (e.g. "UserId" and "userid"). Lowercasing the rename `to` side (as a previously reverted
  // change did) silently collides such renames and conflates the two columns. See PR #346.
  test("resolvePrimaryKeyColumns keeps case-sensitive (quoted) primary key columns distinct") {
    val schema = StructType(
      Seq(
        StructField("src_a", StringType, nullable = true),
        StructField("src_b", StringType, nullable = true),
        StructField("value", StringType, nullable = true)
      )
    )

    // Two distinct target PK columns that differ only in case, each fed by its own rename.
    val targetPkNames = Set("UserId", "userid")
    val renames = List(Rename("src_a", "UserId"), Rename("src_b", "userid"))

    val resolution = Scylla.resolvePrimaryKeyColumns(targetPkNames, renames, schema)

    assertEquals(resolution.unresolvedSourcePkNames, Set.empty[String])
    assertEquals(resolution.resolvedSourcePkNames, Set("src_a", "src_b"))
    assertEquals(
      resolution.fieldIndices.toSet,
      Set(schema.fieldIndex("src_a"), schema.fieldIndex("src_b"))
    )
  }

  // Regression guard: a rename whose `to` differs only in case from the actual target PK name
  // must NOT be applied (exact-case match). Making it case-insensitive would change behavior for
  // quoted CQL identifiers, so this pins the contract restored after reverting that change.
  test("resolvePrimaryKeyColumns matches rename targets case-sensitively") {
    val schema = StructType(
      Seq(
        StructField("source_col", StringType, nullable = true),
        StructField("ck", IntegerType, nullable        = true)
      )
    )

    // Rename targets "MyId", but the target PK is the case-different "myid".
    val targetPkNames = Set("myid", "ck")
    val renames = List(Rename("source_col", "MyId"))

    val resolution = Scylla.resolvePrimaryKeyColumns(targetPkNames, renames, schema)

    // "myid" does not match the rename target "MyId" (case-sensitive) and has no source column,
    // so it stays unresolved; "ck" resolves directly.
    assertEquals(resolution.unresolvedSourcePkNames, Set("myid"))
    assertEquals(resolution.resolvedSourcePkNames, Set("ck"))
  }
}
