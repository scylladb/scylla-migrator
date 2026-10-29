package com.scylladb.migrator.readers

import com.datastax.spark.connector.types.IntType
import org.apache.spark.sql.Row

class CollectionAppendRowsTest extends munit.FunSuite {

  // [id, tags, tags_ttl, tags_writetime]
  private def appendRows(row: Row): Seq[Row] =
    Cassandra.collectionAppendRows(
      row,
      pkOrdinals       = Array(0),
      colOrdinal       = 1,
      ttlOrdinal       = 2,
      writetimeOrdinal = 3,
      isMap            = false,
      elementSorter    = Cassandra.mapKeyOrdering(IntType)
    )

  // [id, value, ttl, writetime, overwrite]
  private def overwriteFlag(r: Row): Boolean = r.getBoolean(4)

  test("exactly the oldest-writetime group is flagged for overwrite") {
    // Metadata arrays follow the sorted element order (10, 20, 30); the decoded set does not.
    val rows = appendRows(
      Row(1, Seq(30, 10, 20), Seq(50, 0, 0), Seq(1000L, 2000L, 3000L))
    )
    assertEquals(rows.size, 3)
    assertEquals(rows.count(overwriteFlag), 1)
    val overwrite = rows.find(overwriteFlag).get
    assertEquals(overwrite.getLong(3), 1000L)
    assertEquals(overwrite.get(1), IndexedSeq(10))
  }

  test("co-timestamped elements share one overwrite row") {
    val rows = appendRows(Row(1, Seq(1, 2), Seq(0, 0), Seq(500L, 500L)))
    assertEquals(rows.size, 1)
    assert(overwriteFlag(rows.head))
    assertEquals(rows.head.get(1), IndexedSeq(1, 2))
  }

  test("null or empty collections emit no rows") {
    assertEquals(appendRows(Row(1, null, null, null)), Nil)
    assertEquals(appendRows(Row(1, Seq.empty, Seq.empty, Seq.empty)), Nil)
  }
}
