package com.scylladb.migrator.readers

class CollectionBatchingTest extends munit.FunSuite {

  test("groups consecutive items within the weight limit") {
    assertEquals(
      CollectionBatching.group[Int](Seq(1, 2, 3, 4, 5), _ => 1L, 2L),
      Seq(Seq(1, 2), Seq(3, 4), Seq(5))
    )
    assertEquals(
      CollectionBatching.group[Long](Seq(3L, 3L, 1L, 5L), identity, 6L),
      Seq(Seq(3L, 3L), Seq(1L, 5L))
    )
  }

  test("an item heavier than the limit forms a group of its own") {
    assertEquals(
      CollectionBatching.group[Long](Seq(1L, 10L, 1L), identity, 5L),
      Seq(Seq(1L), Seq(10L), Seq(1L))
    )
  }

  test("no items, no groups") {
    assertEquals(CollectionBatching.group[Int](Nil, _ => 1L, 5L), Nil)
  }

  test("batch size comes from scylla.migrator.collectionBatchPartitions") {
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[3]")
      .appName("CollectionBatchingTest")
      .getOrCreate()
    val key = "scylla.migrator.collectionBatchPartitions"
    try {
      sys.props.remove(key)
      assertEquals(
        CollectionBatching.batchPartitions(spark),
        spark.sparkContext.defaultParallelism
      )
      sys.props(key) = "7"
      assertEquals(CollectionBatching.batchPartitions(spark), 7)
      sys.props(key) = "0"
      assertEquals(CollectionBatching.batchPartitions(spark), Int.MaxValue)
      sys.props(key) = "seven"
      intercept[IllegalArgumentException](CollectionBatching.batchPartitions(spark))
    } finally sys.props.remove(key)
  }
}
