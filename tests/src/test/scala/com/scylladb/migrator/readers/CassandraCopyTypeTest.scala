package com.scylladb.migrator.readers

import com.datastax.spark.connector.cql.{
  ClusteringColumn,
  ColumnDef,
  PartitionKeyColumn,
  RegularColumn,
  StaticColumn,
  TableDef
}
import com.datastax.spark.connector.types.{
  IntType,
  ListType,
  MapType,
  SetType,
  UDTFieldDef,
  UUIDType,
  UserDefinedType,
  VarCharType
}
import com.scylladb.migrator.config.CopyType

/** Unit coverage for [[Cassandra.determineCopyType]] focusing on the frozen vs non-frozen
  * collection behavior. Frozen collections are single-cell and flow through the timestamp
  * preservation path; non-frozen (multi-cell) collections are rejected unless the opt-in
  * `preserveCollectionTimestamps` is set (and then only supported sets/maps are accepted).
  */
class CassandraCopyTypeTest extends munit.FunSuite {

  private def tableWith(regular: ColumnDef*): TableDef =
    TableDef(
      keyspaceName      = "test",
      tableName         = "t",
      partitionKey      = Seq(ColumnDef("id", PartitionKeyColumn, VarCharType)),
      clusteringColumns = Seq.empty,
      regularColumns    = regular
    )

  test("scalar-only table with preserveTimestamps uses WithTimestampPreservation") {
    val tableDef = tableWith(ColumnDef("foo", RegularColumn, VarCharType))
    assertEquals(
      Cassandra.determineCopyType(tableDef, preserveTimesRequest = true),
      Right(CopyType.WithTimestampPreservation)
    )
  }

  test("frozen collection with preserveTimestamps is allowed (single-cell)") {
    val frozenSet = ColumnDef("tags", RegularColumn, SetType(VarCharType, isFrozen = true))
    val frozenList = ColumnDef("items", RegularColumn, ListType(IntType, isFrozen = true))
    val frozenMap =
      ColumnDef("attrs", RegularColumn, MapType(VarCharType, IntType, isFrozen = true))

    for (col <- Seq(frozenSet, frozenList, frozenMap)) {
      val tableDef = tableWith(ColumnDef("foo", RegularColumn, VarCharType), col)
      assertEquals(
        Cassandra.determineCopyType(tableDef, preserveTimesRequest = true),
        Right(CopyType.WithTimestampPreservation),
        s"Expected frozen collection column '${col.columnName}' to be allowed"
      )
    }
  }

  test("non-frozen (multi-cell) collection with preserveTimestamps is rejected") {
    val nonFrozenSet = ColumnDef("tags", RegularColumn, SetType(VarCharType))
    val nonFrozenList = ColumnDef("items", RegularColumn, ListType(IntType))
    val nonFrozenMap = ColumnDef("attrs", RegularColumn, MapType(VarCharType, IntType))

    for (col <- Seq(nonFrozenSet, nonFrozenList, nonFrozenMap)) {
      val tableDef = tableWith(ColumnDef("foo", RegularColumn, VarCharType), col)
      val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
      assert(
        result.isLeft,
        s"Expected non-frozen collection column '${col.columnName}' to be rejected, got: ${result}"
      )
      assert(
        result.left.exists(
          _.getMessage.contains(
            "TTL/Writetime preservation is unsupported for tables with non-frozen (multi-cell)"
          )
        ),
        s"Unexpected error message: ${result}"
      )
    }
  }

  test("non-frozen set/map with preserveCollectionTimestamps is allowed") {
    val nonFrozenSet = ColumnDef("tags", RegularColumn, SetType(IntType))
    val nonFrozenMap = ColumnDef("attrs", RegularColumn, MapType(VarCharType, IntType))

    for (col <- Seq(nonFrozenSet, nonFrozenMap)) {
      val tableDef = tableWith(ColumnDef("foo", RegularColumn, VarCharType), col)
      assertEquals(
        Cassandra.determineCopyType(
          tableDef,
          preserveTimesRequest           = true,
          preserveCollectionTimesRequest = true
        ),
        Right(CopyType.WithTimestampPreservation),
        s"Expected non-frozen collection column '${col.columnName}' to be allowed under opt-in"
      )
    }
  }

  test("non-frozen list is rejected even with preserveCollectionTimestamps") {
    val tableDef =
      tableWith(
        ColumnDef("foo", RegularColumn, VarCharType),
        ColumnDef("items", RegularColumn, ListType(IntType))
      )
    val result = Cassandra.determineCopyType(
      tableDef,
      preserveTimesRequest           = true,
      preserveCollectionTimesRequest = true
    )
    assert(result.isLeft, s"Expected non-frozen list to be rejected, got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("non-frozen list")),
      s"Unexpected error message: ${result}"
    )
  }

  test("non-frozen map with unsupported key type is rejected with preserveCollectionTimestamps") {
    val tableDef =
      tableWith(
        ColumnDef("foo", RegularColumn, VarCharType),
        ColumnDef("attrs", RegularColumn, MapType(UUIDType, IntType))
      )
    val result = Cassandra.determineCopyType(
      tableDef,
      preserveTimesRequest           = true,
      preserveCollectionTimesRequest = true
    )
    assert(result.isLeft, s"Expected unsupported map key type to be rejected, got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("unsupported key type")),
      s"Unexpected error message: ${result}"
    )
  }

  test("non-frozen collection without preserveTimestamps uses NoTimestampPreservation") {
    val tableDef =
      tableWith(
        ColumnDef("foo", RegularColumn, VarCharType),
        ColumnDef("tags", RegularColumn, SetType(VarCharType))
      )
    assertEquals(
      Cassandra.determineCopyType(tableDef, preserveTimesRequest = false),
      Right(CopyType.NoTimestampPreservation)
    )
  }

  test("column named 'ttl' or 'writetime' collides with reserved metadata names and is rejected") {
    for (name <- Seq("ttl", "writetime")) {
      val tableDef = tableWith(ColumnDef(name, RegularColumn, IntType))
      val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
      assert(
        result.isLeft,
        s"Expected reserved-name column '${name}' to be rejected, got: ${result}"
      )
      assert(
        result.left.exists(_.getMessage.contains("reserves the internal column names")),
        s"Unexpected error message for '${name}': ${result}"
      )
    }
  }

  test("column shaped like a '<col>_ttl'/'<col>_writetime' sidecar is rejected") {
    for (name <- Seq("foo_ttl", "foo_writetime")) {
      val tableDef =
        tableWith(
          ColumnDef("foo", RegularColumn, VarCharType),
          ColumnDef(name, RegularColumn, IntType)
        )
      val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
      assert(
        result.isLeft,
        s"Expected sidecar-colliding column '${name}' to be rejected, got: ${result}"
      )
      assert(
        result.left.exists(_.getMessage.contains("reserves the internal column names")),
        s"Unexpected error message for '${name}': ${result}"
      )
    }
  }

  test("static non-frozen collection is rejected with preserveCollectionTimestamps") {
    // A static cell belongs to the partition, not the row, so replaying it per clustering row would
    // both duplicate the appends and produce an UPDATE that CQL rejects (clustering restrictions on
    // a statement modifying only static columns).
    val tableDef = TableDef(
      keyspaceName      = "test",
      tableName         = "t",
      partitionKey      = Seq(ColumnDef("id", PartitionKeyColumn, VarCharType)),
      clusteringColumns = Seq(ColumnDef("ck", ClusteringColumn(0), IntType)),
      regularColumns    = Seq(ColumnDef("tags", StaticColumn, SetType(IntType)))
    )
    val result = Cassandra.determineCopyType(
      tableDef,
      preserveTimesRequest           = true,
      preserveCollectionTimesRequest = true
    )
    assert(result.isLeft, s"Expected static non-frozen collection to be rejected, got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("static non-frozen collection")),
      s"Unexpected error message: ${result}"
    )
  }

  test("non-frozen UDT is rejected (multi-cell but not a collection)") {
    // The connector reports `isCollection == false` for UDTs while `isMultiCell == !isFrozen`, so a
    // non-frozen UDT is invisible to the collection gate yet its TTL()/WRITETIME() still return one
    // value per subfield. Without an explicit gate it would get scalar sidecars for list metadata.
    val udt = UserDefinedType(
      name     = "addr",
      columns  = IndexedSeq(UDTFieldDef("street", VarCharType), UDTFieldDef("zip", IntType)),
      isFrozen = false
    )
    assert(!udt.isCollection, "Expected the connector to report a UDT as not a collection")
    assert(udt.isMultiCell, "Expected a non-frozen UDT to be multi-cell")

    val tableDef = tableWith(ColumnDef("home", RegularColumn, udt))
    val result = Cassandra.determineCopyType(tableDef, preserveTimesRequest = true)
    assert(result.isLeft, s"Expected non-frozen UDT to be rejected, got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("non-collection")),
      s"Unexpected error message: ${result}"
    )
  }

  test("frozen UDT with preserveTimestamps is allowed (single-cell)") {
    val frozen = UserDefinedType(
      name     = "addr",
      columns  = IndexedSeq(UDTFieldDef("street", VarCharType)),
      isFrozen = true
    )
    val tableDef = tableWith(ColumnDef("home", RegularColumn, frozen))
    assertEquals(
      Cassandra.determineCopyType(tableDef, preserveTimesRequest = true),
      Right(CopyType.WithTimestampPreservation)
    )
  }

  test("reserved-name column is harmless when preserveTimestamps is disabled") {
    val tableDef = tableWith(ColumnDef("ttl", RegularColumn, IntType))
    assertEquals(
      Cassandra.determineCopyType(tableDef, preserveTimesRequest = false),
      Right(CopyType.NoTimestampPreservation)
    )
  }
}
