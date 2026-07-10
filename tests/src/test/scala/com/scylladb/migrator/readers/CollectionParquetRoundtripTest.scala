package com.scylladb.migrator.readers

import com.datastax.spark.connector.types.CassandraOption
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  LongType,
  MapType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}

/** Unit coverage for the Parquet restore path of per-element collection TTL/WRITETIME
  * (`explodeRowsFromPerColumnMetaCollectionAware`): array-typed `_ttl`/`_writetime` sidecars are
  * split off into collection-append passes, the base explode keeps only scalar columns, and
  * elements are re-aligned with their metadata by the same ordering as the direct Cassandra path.
  */
class CollectionParquetRoundtripTest extends munit.FunSuite {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("CollectionParquetRoundtripTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  // The Parquet restore path only trusts an array-sidecar column as a per-element collection when
  // it carries the collection-kind marker a real `preserveCollectionTimestamps` export stamps.
  private def kindMeta(kind: String) =
    new MetadataBuilder().putString(Cassandra.CollectionKindMetaKey, kind).build()

  // id (PK) + scalar name (scalar metadata) + non-frozen set tags + non-frozen map attrs.
  private val schema = StructType(
    Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("name_ttl", IntegerType),
      StructField("name_writetime", LongType),
      StructField("tags", ArrayType(IntegerType), metadata = kindMeta(Cassandra.CollectionKindSet)),
      StructField("tags_ttl", ArrayType(IntegerType)),
      StructField("tags_writetime", ArrayType(LongType)),
      StructField(
        "attrs",
        MapType(StringType, IntegerType),
        metadata = kindMeta(Cassandra.CollectionKindMap)
      ),
      StructField("attrs_ttl", ArrayType(IntegerType)),
      StructField("attrs_writetime", ArrayType(LongType))
    )
  )

  private def sampleDf = {
    // Collection values are stored in an arbitrary (non-sorted) order, mirroring the connector's
    // decode; metadata arrays are in server (sorted) order. The explode must re-sort the elements
    // to re-pair them: tags 10->100, 20->200, 30->300; attrs a->10, b->20.
    val rows = java.util.Arrays.asList(
      Row(
        "r1",
        "alice",
        0,
        1000L,
        Seq(30, 10, 20), // unsorted set
        Seq(0, 0, 0),
        Seq(100L, 200L, 300L),
        Map("b" -> 2, "a" -> 1), // unsorted map
        Seq(0, 0),
        Seq(10L, 20L)
      )
    )
    spark.createDataFrame(rows, schema)
  }

  test("base explode keeps only scalar columns; collections become append passes") {
    val (baseRdd, baseSchema, timestampColumns, appends) =
      Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, sampleDf)

    assertEquals(timestampColumns, TimestampColumns("ttl", "writetime"))

    // Base schema excludes the per-element collection columns and their sidecars.
    assertEquals(baseSchema.fieldNames.toSeq, Seq("id", "name", "ttl", "writetime"))

    val baseRows = baseRdd.collect().toList
    assertEquals(baseRows.length, 1)
    val baseRow = baseRows.head
    // PK values are plain; regular columns keep CassandraOption tri-state for the RDD write path.
    assertEquals(baseRow.getString(0), "r1")
    assertEquals(baseRow.get(1), CassandraOption.Value("alice"))
    assertEquals(baseRow.getLong(3), 1000L)

    // One append pass per non-frozen collection column, ordered by column name.
    assertEquals(appends.map(_.columnName), Seq("attrs", "tags"))
  }

  test("set append rows are element-aligned with their WRITETIMEs") {
    val (_, _, _, appends) =
      Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, sampleDf)

    val tags = appends.find(_.columnName == "tags").get
    // schema: [id, tags, ttl, writetime]
    assertEquals(tags.schema.fieldNames.toSeq, Seq("id", "tags", "ttl", "writetime"))

    val byWritetime = tags.rdd
      .collect()
      .map { r =>
        val elems = r.getSeq[Int](1).toSet
        r.getLong(3) -> elems
      }
      .toMap

    assertEquals(byWritetime(100L), Set(10))
    assertEquals(byWritetime(200L), Set(20))
    assertEquals(byWritetime(300L), Set(30))
  }

  test("map append rows are key-sorted and aligned with their WRITETIMEs") {
    val (_, _, _, appends) =
      Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, sampleDf)

    val attrs = appends.find(_.columnName == "attrs").get
    assertEquals(attrs.schema.fieldNames.toSeq, Seq("id", "attrs", "ttl", "writetime"))

    val byWritetime = attrs.rdd
      .collect()
      .map { r =>
        val m = r.getMap[String, Int](1).toMap
        r.getLong(3) -> m
      }
      .toMap

    // Keys sorted: a -> 10, b -> 20.
    assertEquals(byWritetime(10L), Map("a" -> 1))
    assertEquals(byWritetime(20L), Map("b" -> 2))
  }

  test("array-sidecar column WITHOUT the collection-kind marker is rejected (foreign Parquet)") {
    // Same shape as `sampleDf` but the collection columns carry no migrator marker, as a
    // hand-crafted / foreign Parquet (or a CQL list decoded as an array) would. The restore must
    // refuse rather than silently replay it as a set-append.
    val foreignSchema = StructType(
      Seq(
        StructField("id", StringType),
        StructField("tags", ArrayType(IntegerType)),
        StructField("tags_ttl", ArrayType(IntegerType)),
        StructField("tags_writetime", ArrayType(LongType))
      )
    )
    val df = spark.createDataFrame(
      java.util.Arrays.asList(Row("r1", Seq(10, 20), Seq(0, 0), Seq(100L, 200L))),
      foreignSchema
    )
    val ex = intercept[IllegalArgumentException] {
      Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, df)
    }
    assert(ex.getMessage.contains(Cassandra.CollectionKindMetaKey))
  }

  test("no per-element columns => base explode matches scalar path with no append passes") {
    val scalarSchema = StructType(
      Seq(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("name_ttl", IntegerType),
        StructField("name_writetime", LongType)
      )
    )
    val df = spark.createDataFrame(
      java.util.Arrays.asList(Row("r1", "alice", 0, 1000L)),
      scalarSchema
    )

    val (baseRdd, baseSchema, _, appends) =
      Cassandra.explodeRowsFromPerColumnMetaCollectionAware(spark, df)

    assert(appends.isEmpty)
    assertEquals(baseSchema.fieldNames.toSeq, Seq("id", "name", "ttl", "writetime"))
    assertEquals(baseRdd.collect().length, 1)
  }
}
