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
        StructField("ID", StringType, nullable = true),
        StructField("ClusteringKey", IntegerType, nullable = true),
        StructField("value", StringType, nullable = true)
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
        StructField("id", StringType, nullable = true),
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
        StructField("pk1", StringType, nullable = true),
        StructField("pk2", StringType, nullable = true),
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

    val keptRows = filtered.collect().map(row => (row.getString(0), row.getString(1), row.getString(2))).toSet
    assertEquals(keptRows, Set(("a", "1", "ok-1"), ("d", "4", "ok-2")))
    assert(droppedRows.value == 2L)
  }
}
