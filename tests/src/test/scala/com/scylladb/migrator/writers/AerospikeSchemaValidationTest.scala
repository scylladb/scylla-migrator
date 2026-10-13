package com.scylladb.migrator.writers

import com.datastax.spark.connector.cql.{
  ClusteringColumn,
  ColumnDef,
  PartitionKeyColumn,
  RegularColumn,
  StaticColumn,
  TableDef
}
import com.datastax.spark.connector.{ types => cql }
import com.scylladb.migrator.config.{
  AerospikeInvalidRecordHandling,
  AerospikeListOverflowHandling,
  Rename
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

class AerospikeSchemaValidationTest extends munit.FunSuite {
  private val validation = AerospikeSchemaValidation

  private def table(
    keyName: String = "aero_key",
    keyType: cql.ColumnType[_] = cql.TextType,
    columns: Seq[(String, cql.ColumnType[_])] = Nil
  ): TableDef = TableDef(
    "test",
    "records",
    Seq(ColumnDef(keyName, PartitionKeyColumn, keyType)),
    Nil,
    columns.map { case (name, t) => ColumnDef(name, RegularColumn, t) }
  )

  test("type mismatch guidance distinguishes scalars from ordered lists") {
    val scalar = intercept[IllegalArgumentException] {
      validation.validateTypes(BooleanType, cql.IntType, "flag")
    }.getMessage
    assert(scalar.contains("required schema mapping"))
    assert(!scalar.contains("Lists require"))
    val list = intercept[IllegalArgumentException] {
      validation.validateTypes(ArrayType(LongType), cql.SetType(cql.BigIntType), "items")
    }.getMessage
    assert(list.contains("Lists require CQL lists to preserve order and duplicates"))
  }

  test("startup rejects double to varint mappings recursively with the affected path") {
    val cases = Seq[(DataType, cql.ColumnType[_], String)](
      (DoubleType, cql.VarIntType, "amount"),
      (ArrayType(DoubleType), cql.ListType(cql.VarIntType), "amount[]"),
      (
        MapType(DoubleType, LongType),
        cql.MapType(cql.VarIntType, cql.BigIntType),
        "amount<key>"
      ),
      (
        MapType(StringType, ArrayType(DoubleType)),
        cql.MapType(cql.TextType, cql.ListType(cql.VarIntType, isFrozen = true)),
        "amount<value>[]"
      )
    )
    for ((sourceType, targetType, path) <- cases) {
      val source =
        StructType(Seq(StructField("id", StringType), StructField("amount", sourceType)))
      val target = table("id", columns = Seq("amount" -> targetType))
      val error = intercept[IllegalArgumentException] {
        validation.prepare(source, source, "id", target)
      }
      assert(error.getMessage.contains(s"column '$path' cannot be migrated from double to varint"))
      assert(error.getMessage.contains("required schema mapping"))
    }
  }

  test("startup retains integer varint mappings and supported double conversions") {
    val cases = Seq[(DataType, cql.ColumnType[_], Any)](
      (LongType, cql.VarIntType, Long.MaxValue),
      (IntegerType, cql.VarIntType, Int.MaxValue),
      (DoubleType, cql.BigIntType, 42.0),
      (DoubleType, cql.DecimalType, 0.5)
    )
    for ((sourceType, targetType, value) <- cases) {
      val source =
        StructType(Seq(StructField("id", StringType), StructField("amount", sourceType)))
      val target = table("id", columns = Seq("amount" -> targetType))
      val columns = validation.prepare(source, source, "id", target)
      val row = Row("id", value)
      assertEquals(validation.validateRow(row, columns), Some(row))
    }
  }

  test("numeric checks prevent connector overflow, rounding, truncation and negative-zero loss") {
    for (
      (value, target) <- Seq[(Any, cql.ColumnType[_])](
                           2147483648L            -> cql.IntType,
                           256L                   -> cql.TinyIntType,
                           32768L                 -> cql.SmallIntType,
                           9007199254740993L      -> cql.DoubleType,
                           16777217L              -> cql.FloatType,
                           1.75                   -> cql.BigIntType,
                           Long.MaxValue.toDouble -> cql.BigIntType,
                           -0.0                   -> cql.BigIntType,
                           0.1                    -> cql.FloatType
                         )
    ) {
      val error = intercept[IllegalArgumentException] {
        validation.validateValue(value, target, "amount")
      }
      assert(error.getMessage.contains("amount"))
      assert(error.getMessage.contains("would change the value"))
    }
  }

  test("numeric checks preserve exactly representable conversions") {
    for (
      (value, target) <- Seq[(Any, cql.ColumnType[_])](
                           Int.MinValue.toLong     -> cql.IntType,
                           Int.MaxValue.toLong     -> cql.IntType,
                           255L                    -> cql.SmallIntType,
                           9007199254740992L       -> cql.DoubleType,
                           16777216L               -> cql.FloatType,
                           42.0                    -> cql.BigIntType,
                           -0.0                    -> cql.FloatType,
                           Double.PositiveInfinity -> cql.FloatType,
                           Double.NaN              -> cql.DoubleType,
                           Long.MaxValue           -> cql.VarIntType,
                           Long.MaxValue           -> cql.DecimalType,
                           0.5                     -> cql.DecimalType
                         )
    ) validation.validateValue(value, target, "amount")
  }

  test("double to decimal requires exact values and applies ordinary row handling") {
    validation.validateTypes(DoubleType, cql.DecimalType, "amount")
    val columns = Vector(
      validation.Column("id", cql.TextType, primaryKey        = true),
      validation.Column("amount", cql.DecimalType, primaryKey = false),
      validation.Column("neighbor", cql.TextType, primaryKey  = false)
    )
    for (value <- Seq(0.1, 1.1, math.nextUp(0.5))) {
      assertEquals(validation.validateRow(Row("id", value, "kept"), columns), None)
      assertEquals(
        validation
          .validateRow(Row("id", value, "kept"), columns, AerospikeInvalidRecordHandling.Null),
        Some(Row("id", null, "kept"))
      )
    }
    for {
      value  <- Seq(0.5, 0.25, 42.0)
      policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
    } {
      val row = Row("id", value, "kept")
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
    }
  }

  private val listPolicies = Seq(
    AerospikeListOverflowHandling.Skip,
    AerospikeListOverflowHandling.Nullify,
    AerospikeListOverflowHandling.Empty
  )
  private val ordinaryPolicies =
    Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)

  test("a single non-frozen list uses the independent inclusive shared write budget") {
    val columns = Vector(
      validation.Column("id", cql.TextType, primaryKey                    = true),
      validation.Column("items", cql.ListType(cql.BigIntType), primaryKey = false),
      validation.Column("neighbor", cql.TextType, primaryKey              = false)
    )
    val boundary = Row("id", Vector.fill(131072)(1L), "kept")
    val overflow = Row("id", Vector.fill(131073)(1L), "kept")
    for {
      ordinary <- ordinaryPolicies
      policy   <- listPolicies
    } {
      assertEquals(
        validation.validateRow(boundary, columns, ordinary, listOverflowHandling = policy),
        Some(boundary)
      )
      val errors = scala.collection.mutable.ArrayBuffer.empty[String]
      val expected = policy match {
        case AerospikeListOverflowHandling.Skip    => None
        case AerospikeListOverflowHandling.Nullify => Some(Row("id", null, "kept"))
        case AerospikeListOverflowHandling.Empty   => Some(Row("id", Seq.empty, "kept"))
      }
      assertEquals(
        validation.validateRow(overflow, columns, ordinary, errors += _, policy),
        expected
      )
      assertEquals(errors.size, 1)
      assert(errors.head.contains("column 'items'"))
      assert(errors.head.contains("shared 131072-element write budget"))
      // An earlier overflow must not consume any of the next record's budget.
      assertEquals(validation.validateRow(boundary, columns), Some(boundary))
    }
  }

  test("combined list overflow replaces every contributor independently of column order") {
    val source = StructType(
      Seq(
        StructField("id", StringType),
        StructField("first", ArrayType(LongType)),
        StructField("second", ArrayType(LongType)),
        StructField("neighbor", StringType)
      )
    )
    val target = table(
      "id",
      columns = Seq("first" -> cql.ListType(cql.BigIntType), "neighbor" -> cql.TextType)
    )
      .copy(regularColumns =
        Seq(
          ColumnDef("first", RegularColumn, cql.ListType(cql.BigIntType)),
          ColumnDef("second", StaticColumn, cql.ListType(cql.BigIntType)),
          ColumnDef("neighbor", RegularColumn, cql.TextType)
        )
      )
    val columns = validation.prepare(source, source, "id", target)
    val left = Vector.fill(65536)(1L)
    for (order <- Seq(Vector(0, 1, 2, 3), Vector(3, 2, 0, 1))) {
      val reordered = order.map(columns)
      val atLimit = Row.fromSeq(order.map(Row("id", left, left, "kept").get))
      val above = Row.fromSeq(order.map(Row("id", left, left :+ 2L, "kept").get))
      for {
        ordinary <- ordinaryPolicies
        policy   <- listPolicies
      } {
        assertEquals(
          validation.validateRow(atLimit, reordered, ordinary, listOverflowHandling = policy),
          Some(atLimit)
        )
        val expected = policy match {
          case AerospikeListOverflowHandling.Skip => None
          case AerospikeListOverflowHandling.Nullify =>
            Some(Row.fromSeq(order.map(Row("id", null, null, "kept").get)))
          case AerospikeListOverflowHandling.Empty =>
            Some(Row.fromSeq(order.map(Row("id", Seq.empty, Seq.empty, "kept").get)))
        }
        assertEquals(
          validation.validateRow(above, reordered, ordinary, listOverflowHandling = policy),
          expected
        )
      }
    }
  }

  test("frozen lists and nested frozen contents do not consume outer list write elements") {
    val large = Vector.fill(131073)(1L)
    val columns = Vector(
      validation
        .Column("frozen", cql.ListType(cql.BigIntType, isFrozen = true), primaryKey = false),
      validation.Column(
        "outer",
        cql.ListType(cql.ListType(cql.BigIntType, isFrozen = true)),
        primaryKey = false
      ),
      validation.Column(
        "mapped",
        cql.MapType(cql.TextType, cql.ListType(cql.BigIntType, isFrozen = true)),
        primaryKey = false
      )
    )
    val row = Row(large, Seq(large, large), Map("large" -> large))
    assertEquals(validation.validateRow(row, columns), Some(row))
    val emptyInner = Row(large, Vector.fill(131072)(Seq.empty[Long]), Map("large" -> large))
    assertEquals(validation.validateRow(emptyInner, columns), Some(emptyInner))
    val overflow = Row(large, Vector.fill(131073)(Seq.empty[Long]), Map("large" -> large))
    assertEquals(validation.validateRow(overflow, columns), None)
    assertEquals(
      validation
        .validateRow(overflow, columns, listOverflowHandling = AerospikeListOverflowHandling.Empty),
      Some(Row(large, Seq.empty, Map("large" -> large)))
    )
  }

  test("ordinary invalid and absent lists are excluded before overflow replacement") {
    val columns = Vector(
      validation.Column("id", cql.TextType, primaryKey                   = true),
      validation.Column("valid", cql.ListType(cql.IntType), primaryKey   = false),
      validation.Column("invalid", cql.ListType(cql.IntType), primaryKey = false),
      validation.Column("absent", cql.ListType(cql.IntType), primaryKey  = false),
      validation.Column("neighbor", cql.TextType, primaryKey             = false)
    )
    val large = Vector.fill(131073)(1L)
    // The invalid member appears at the end, after a complete list budget's worth of values.
    val invalid = large :+ 2147483648L
    for (policy <- listPolicies) {
      val row = Row("id", Vector.fill(131072)(1L), invalid, null, "kept")
      assertEquals(
        validation.validateRow(
          row,
          columns,
          AerospikeInvalidRecordHandling.Null,
          listOverflowHandling = policy
        ),
        Some(Row("id", row.get(1), null, null, "kept"))
      )
      assertEquals(validation.validateRow(row, columns, listOverflowHandling = policy), None)
    }
    assertEquals(
      validation.validateRow(
        Row("id", large, invalid, null, "kept"),
        columns,
        AerospikeInvalidRecordHandling.Null,
        listOverflowHandling = AerospikeListOverflowHandling.Empty
      ),
      Some(Row("id", Seq.empty, null, null, "kept"))
    )
    for {
      ordinary <- ordinaryPolicies
      policy   <- listPolicies
    } {
      val empty = Row("id", Seq.empty[Long], null, null, "kept")
      val expected =
        if (ordinary == AerospikeInvalidRecordHandling.Skip) None
        else Some(Row("id", null, null, null, "kept"))
      assertEquals(
        validation.validateRow(empty, columns, ordinary, listOverflowHandling = policy),
        expected
      )
      assertEquals(
        validation.validateRow(
          Row(null, large, null, null, "kept"),
          columns,
          ordinary,
          listOverflowHandling = policy
        ),
        None
      )
    }
  }

  test("recursive validation catches loss in list members and map keys") {
    val nested = cql.MapType(cql.TextType, cql.ListType(cql.IntType, isFrozen = true))
    validation.validateValue(Map("ok" -> Seq(42L)), nested, "payload")
    intercept[IllegalArgumentException] {
      validation.validateValue(Map("bad" -> Seq(2147483648L)), nested, "payload")
    }
    // The connector used to narrow both keys to zero, silently losing one entry.
    intercept[IllegalArgumentException] {
      validation.validateValue(
        Map(0L -> "first", 4294967296L -> "second"),
        cql.MapType(cql.IntType, cql.TextType),
        "payload"
      )
    }
  }

  test("target map key collisions are detected using the converted key contents") {
    val values = Map(Array[Byte](1, 2) -> "first", Array[Byte](1, 2) -> "second")
    val error = intercept[IllegalArgumentException] {
      validation.validateValue(values, cql.MapType(cql.BlobType, cql.TextType), "payload")
    }
    assert(error.getMessage.contains("distinct map keys"))
  }

  test("lists cannot implicitly become sets or scalar text, preserving order and duplicates") {
    for (target <- Seq(cql.SetType(cql.BigIntType), cql.TextType))
      intercept[IllegalArgumentException] {
        validation.validateTypes(ArrayType(LongType), target, "ordered")
      }
    validation.validateTypes(ArrayType(LongType), cql.ListType(cql.BigIntType), "ordered")
    validation.validateValue(Seq(2L, 1L, 2L), cql.ListType(cql.BigIntType), "ordered")
  }

  test("string-to-numeric coercion requires explicit source conversion") {
    intercept[IllegalArgumentException] {
      validation.validateTypes(StringType, cql.BigIntType, "value")
    }
    validation.validateTypes(LongType, cql.TextType, "value")
    validation.validateTypes(IntegerType, cql.BigIntType, "generation")
  }

  test("null collection members fail and top-level absent bins remain null") {
    validation.validateValue(null, cql.ListType(cql.TextType), "missing")
    intercept[IllegalArgumentException] {
      validation.validateValue(Seq("value", null), cql.ListType(cql.TextType), "payload")
    }
    intercept[IllegalArgumentException] {
      validation.validateValue(
        Map("key" -> null),
        cql.MapType(cql.TextType, cql.TextType),
        "payload"
      )
    }
  }

  test("empty collections require frozen columns to remain distinct from absent bins") {
    for (
      (empty, target) <- Seq(
                           Seq.empty[String]         -> cql.ListType(cql.TextType),
                           Map.empty[String, String] -> cql.MapType(cql.TextType, cql.TextType)
                         )
    ) {
      val error = intercept[IllegalArgumentException] {
        validation.validateValue(empty, target, "payload")
      }
      assert(error.getMessage.contains("frozen<"))
    }
    validation.validateValue(Seq.empty[String], cql.ListType(cql.TextType, true), "payload")
    validation.validateValue(
      Map.empty[String, String],
      cql.MapType(cql.TextType, cql.TextType, true),
      "payload"
    )
    validation.validateValue(
      Seq(Seq.empty[String]),
      cql.ListType(cql.ListType(cql.TextType, true)),
      "payload"
    )
  }

  test("record identity keeps its exact type and participates in the target primary key") {
    val source =
      StructType(Seq(StructField("aero_key", StringType), StructField("value", StringType)))
    val good = table(columns = Seq("value" -> cql.TextType))
    validation.prepare(source, source, "aero_key", good)
    intercept[IllegalArgumentException] {
      validation.prepare(
        source,
        source,
        "aero_key",
        table(keyType = cql.BigIntType, columns = Seq("value" -> cql.TextType))
      )
    }
    intercept[IllegalArgumentException] {
      validation.prepare(
        source,
        source,
        "aero_key",
        table("value", columns = Seq("aero_key" -> cql.TextType))
      )
    }
  }

  test("custom key column and chained renames resolve primary keys positionally") {
    val source =
      StructType(Seq(StructField("record_id", LongType), StructField("value", StringType)))
    val renamed = Scylla.renameSchemaFields(
      source,
      List(Rename("record_id", "intermediate"), Rename("intermediate", "id"))
    )
    val columns = validation.prepare(
      source,
      renamed,
      "record_id",
      table("id", cql.BigIntType, Seq("value" -> cql.TextType))
    )
    val row = Row(42L, "value")
    assertEquals(validation.validateRow(row, columns), Some(row))
    assertEquals(validation.validateRow(Row(null, "value"), columns), None)
  }

  test("a source solr_query bin is accepted after renaming and rejected under the internal name") {
    val source =
      StructType(Seq(StructField("aero_key", StringType), StructField("solr_query", StringType)))
    val renamed = Scylla.renameSchemaFields(source, List(Rename("solr_query", "query")))
    val columns =
      validation.prepare(source, renamed, "aero_key", table(columns = Seq("query" -> cql.TextType)))
    assertEquals(
      validation.validateRow(Row("key", "payload"), columns),
      Some(Row("key", "payload"))
    )
    intercept[IllegalArgumentException] {
      validation.prepare(
        source,
        source,
        "aero_key",
        table(columns = Seq("solr_query" -> cql.TextType))
      )
    }
  }

  test("missing target columns and primary keys fail before reading rows") {
    val source =
      StructType(Seq(StructField("aero_key", StringType), StructField("value", StringType)))
    intercept[IllegalArgumentException] {
      validation.prepare(source, source, "aero_key", table())
    }
    val target = table(columns = Seq("value" -> cql.TextType)).copy(
      partitionKey = Seq(
        ColumnDef("aero_key", PartitionKeyColumn, cql.TextType),
        ColumnDef("extra", PartitionKeyColumn, cql.TextType)
      )
    )
    intercept[IllegalArgumentException] {
      validation.prepare(source, source, "aero_key", target)
    }
  }

  test("ASCII validation rejects characters that the target cannot represent") {
    validation.validateValue("plain", cql.AsciiType, "label")
    intercept[IllegalArgumentException] {
      validation.validateValue("café", cql.AsciiType, "label")
    }
  }

  test("static values need the record identity in the partition key to avoid overwriting records") {
    val source = StructType(
      Seq(
        StructField("aero_key", StringType),
        StructField("group", StringType),
        StructField("value", StringType)
      )
    )
    val unsafe = TableDef(
      "test",
      "records",
      Seq(ColumnDef("group", PartitionKeyColumn, cql.TextType)),
      Seq(ColumnDef("aero_key", ClusteringColumn(0), cql.TextType)),
      Seq(ColumnDef("value", StaticColumn, cql.TextType))
    )
    intercept[IllegalArgumentException] {
      validation.prepare(source, source, "aero_key", unsafe)
    }
    val safe = unsafe.copy(
      partitionKey      = Seq(ColumnDef("aero_key", PartitionKeyColumn, cql.TextType)),
      clusteringColumns = Seq(ColumnDef("group", ClusteringColumn(0), cql.TextType))
    )
    validation.prepare(source, source, "aero_key", safe)
  }

  test("record policy skips or nulls complete invalid target fields and preserves neighbors") {
    val invalidFields = Seq[(Any, cql.ColumnType[_])](
      2147483648L             -> cql.IntType,
      Double.NaN              -> cql.DecimalType,
      "café"                  -> cql.AsciiType,
      Seq(1L, 2147483648L)    -> cql.ListType(cql.IntType),
      Seq.empty[Long]         -> cql.ListType(cql.BigIntType),
      Map.empty[String, Long] -> cql.MapType(cql.TextType, cql.BigIntType),
      Map(Array[Byte](1) -> "one", Array[Byte](1) -> "two") ->
        cql.MapType(cql.BlobType, cql.TextType)
    )
    for ((bad, target) <- invalidFields) {
      val columns = Vector(
        validation.Column("id", cql.TextType, primaryKey   = true),
        validation.Column("bad", target, primaryKey        = false),
        validation.Column("good", cql.TextType, primaryKey = false)
      )
      val row = Row("id", bad, "preserved")
      assertEquals(validation.validateRow(row, columns), None)
      assertEquals(
        validation.validateRow(row, columns, AerospikeInvalidRecordHandling.Null),
        Some(Row("id", null, "preserved"))
      )
      assertEquals(row.get(1), bad)
    }
  }

  test("null or invalid secondary primary keys skip in both modes after chained renames") {
    val source = StructType(
      Seq(
        StructField("identity", StringType),
        StructField("secondary", LongType)
      )
    )
    val renamed = Scylla.renameSchemaFields(
      source,
      List(Rename("secondary", "temporary"), Rename("temporary", "clustering"))
    )
    val target = table("identity").copy(
      clusteringColumns = Seq(ColumnDef("clustering", ClusteringColumn(0), cql.IntType))
    )
    val columns = validation.prepare(source, renamed, "identity", target)
    for {
      policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
      bad    <- Seq(null, 2147483648L)
    } assertEquals(validation.validateRow(Row("id", bad), columns, policy), None)
  }

  test("unexpected row access failures remain fatal in either mode") {
    val error = new IllegalStateException("unexpected row failure")
    val row = new org.apache.spark.sql.catalyst.expressions.GenericRow(Array[Any]("id")) {
      override def get(index: Int): Any = throw error
    }
    val columns = Vector(validation.Column("id", cql.TextType, primaryKey = true))
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null))
      assertEquals(
        intercept[IllegalStateException](validation.validateRow(row, columns, policy)),
        error
      )
  }

  test("unexpected numeric conversion failures remain fatal") {
    val error = new IllegalStateException("unexpected numeric implementation failure")
    val number = new Number {
      override def intValue(): Int = 0
      override def longValue(): Long = 0L
      override def floatValue(): Float = 0.0f
      override def doubleValue(): Double = 0.0
      override def toString: String = throw error
    }
    val columns = Vector(validation.Column("value", cql.DecimalType, primaryKey = false))
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null))
      assertEquals(
        intercept[IllegalStateException] {
          validation.validateRow(Row(number), columns, policy)
        },
        error
      )
  }

  test("Spark external array rows validate empty and lossy lists recursively") {
    def external(value: Any, dataType: DataType): Row = {
      val encoder = ExpressionEncoder(
        StructType(
          Seq(
            StructField("id", StringType),
            StructField("payload", dataType),
            StructField("neighbor", StringType)
          )
        )
      ).resolveAndBind()
      encoder.createDeserializer()(encoder.createSerializer()(Row("id", value, "preserved")))
    }
    val cases = Seq[(Any, DataType, cql.ColumnType[_])](
      (Seq.empty[Long], ArrayType(LongType), cql.ListType(cql.BigIntType)),
      (Seq(1L, 2147483648L), ArrayType(LongType), cql.ListType(cql.IntType)),
      (
        Seq(Seq(1L, 2147483648L)),
        ArrayType(ArrayType(LongType)),
        cql.ListType(cql.ListType(cql.IntType, isFrozen = true))
      ),
      (
        Map("key" -> Seq(1L, 2147483648L)),
        MapType(StringType, ArrayType(LongType)),
        cql.MapType(cql.TextType, cql.ListType(cql.IntType, isFrozen = true))
      ),
      (
        Seq(Seq.empty[Long]),
        ArrayType(ArrayType(LongType)),
        cql.ListType(cql.ListType(cql.BigIntType))
      )
    )
    for ((value, sourceType, targetType) <- cases) {
      val row = external(value, sourceType)
      sourceType match {
        case _: ArrayType => assert(row.get(1).isInstanceOf[scala.collection.mutable.ArraySeq[_]])
        case _: MapType =>
          val nested = row.get(1).asInstanceOf[scala.collection.Map[_, _]].values.head
          assert(nested.isInstanceOf[scala.collection.mutable.ArraySeq[_]])
        case _ => fail("test case must contain a list")
      }
      val columns = Vector(
        validation.Column("id", cql.TextType, primaryKey       = true),
        validation.Column("payload", targetType, primaryKey    = false),
        validation.Column("neighbor", cql.TextType, primaryKey = false)
      )
      assertEquals(validation.validateRow(row, columns), None)
      assertEquals(
        validation.validateRow(row, columns, AerospikeInvalidRecordHandling.Null),
        Some(Row("id", null, "preserved"))
      )
    }
    val frozen = external(Seq.empty[Long], ArrayType(LongType))
    val columns = Vector(
      validation.Column("id", cql.TextType, primaryKey = true),
      validation
        .Column("payload", cql.ListType(cql.BigIntType, isFrozen = true), primaryKey = false),
      validation.Column("neighbor", cql.TextType, primaryKey                         = false)
    )
    assertEquals(validation.validateRow(frozen, columns), Some(frozen))
  }

  test("empty single-column text and binary partition keys skip under either policy") {
    val exhausted = java.nio.ByteBuffer.wrap(Array[Byte](1))
    exhausted.position(exhausted.limit())
    for (
      (sourceType, targetType, values) <-
        Seq(
          (StringType, cql.TextType, Seq[Any]("")),
          (BinaryType, cql.BlobType, Seq[Any](Array.emptyByteArray, exhausted))
        )
    ) {
      val source = StructType(
        Seq(
          StructField("record_id", sourceType),
          StructField("value", StringType)
        )
      )
      val renamed = Scylla.renameSchemaFields(
        source,
        List(Rename("record_id", "intermediate"), Rename("intermediate", "id"))
      )
      val columns = validation.prepare(
        source,
        renamed,
        "record_id",
        table("id", targetType, Seq("value" -> cql.TextType))
      )
      assert(columns.head.singlePartitionKey)
      assert(!columns(1).singlePartitionKey)
      for {
        policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
        empty  <- values
      } assertEquals(validation.validateRow(Row(empty, "must not write"), columns, policy), None)
      assertEquals(exhausted.position(), exhausted.limit())
    }
  }

  test("nonempty binary partition keys use buffer remaining bytes") {
    val columns =
      Vector(
        validation.Column(
          "id",
          cql.BlobType,
          primaryKey         = true,
          singlePartitionKey = true,
          partitionKey       = true
        )
      )
    val bytes = java.nio.ByteBuffer.wrap(Array[Byte](0, 1))
    bytes.position(1)
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      assertEquals(validation.validateRow(Row(bytes), columns, policy), Some(Row(bytes)))
      val row = Row(Array[Byte](0))
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
    }
    assertEquals(bytes.position(), 1)
  }

  test("single partition sizes count encoded UTF-8 bytes and Scylla component framing") {
    val source = StructType(Seq(StructField("identity", StringType)))
    val columns = validation.prepare(source, source, "identity", table("identity"))
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null))
      for (valid <- Seq("x" * 65533, "é" * 32766 + "x")) {
        val row = Row(valid)
        assertEquals(validation.validateRow(row, columns, policy), Some(row))
        val errors = scala.collection.mutable.ArrayBuffer.empty[String]
        assertEquals(validation.validateRow(Row(valid + "x"), columns, policy, errors += _), None)
        assertEquals(errors.size, 1)
        assert(errors.head.contains("partition key exceeds 65535 bytes"))
      }
  }

  test("blob partition sizes use remaining bytes without changing buffer positions or limits") {
    val source = StructType(Seq(StructField("identity", BinaryType)))
    val columns = validation.prepare(source, source, "identity", table("identity", cql.BlobType))
    val backing = java.nio.ByteBuffer.wrap(new Array[Byte](70000))
    backing.position(100)
    backing.limit(65634)
    val slice = backing.slice()
    slice.position(1)
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      val row = Row(slice)
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
      assertEquals(validation.validateRow(Row(backing), columns, policy), None)
      assertEquals(
        validation.validateRow(Row(new Array[Byte](65533)), columns, policy).isDefined,
        true
      )
      assertEquals(validation.validateRow(Row(new Array[Byte](65534)), columns, policy), None)
    }
    assertEquals(backing.position(), 100)
    assertEquals(backing.limit(), 65634)
    assertEquals(slice.position(), 1)
    assertEquals(slice.limit(), 65534)
  }

  test("composite sizes follow renamed partition membership rather than row order or clustering") {
    val source = StructType(
      Seq(
        StructField("value", StringType),
        StructField("second", StringType),
        StructField("identity", StringType),
        StructField("first", StringType)
      )
    )
    val renamed = Scylla.renameSchemaFields(
      source,
      List(Rename("first", "temporary"), Rename("temporary", "partition_one"))
    )
    val target = TableDef(
      "test",
      "records",
      Seq(
        ColumnDef("partition_one", PartitionKeyColumn, cql.TextType),
        ColumnDef("second", PartitionKeyColumn, cql.TextType)
      ),
      Seq(ColumnDef("identity", ClusteringColumn(0), cql.TextType)),
      Seq(ColumnDef("value", RegularColumn, cql.TextType))
    )
    val columns = validation.prepare(source, renamed, "identity", target)
    assertEquals(columns.map(_.partitionKey), Vector(false, true, false, true))
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      val row = Row("v" * 70000, "b" * 32766, "identity", "a" * 32765)
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
      assertEquals(
        validation
          .validateRow(Row(row.get(0), row.get(1), row.get(2), "a" * 32766), columns, policy),
        None
      )
    }
  }

  test("partition sizes use the target numeric encoding after exact conversion") {
    val source =
      StructType(Seq(StructField("identity", StringType), StructField("number", LongType)))
    for (
      (targetType, payloadBytes) <-
        Seq(cql.TinyIntType -> 1, cql.VarIntType -> 1, cql.DecimalType -> 5)
    ) {
      val target = table("identity").copy(partitionKey =
        Seq(
          ColumnDef("identity", PartitionKeyColumn, cql.TextType),
          ColumnDef("number", PartitionKeyColumn, targetType)
        )
      )
      val columns = validation.prepare(source, source, "identity", target)
      for (
        policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
      ) {
        val text = "x" * (65531 - payloadBytes)
        val row = Row(text, 42L)
        assertEquals(validation.validateRow(row, columns, policy), Some(row))
        assertEquals(validation.validateRow(Row(text + "x", 42L), columns, policy), None)
      }
    }
  }

  test("single clustering sizes count encoded UTF-8 and blob bytes with component framing") {
    for (
      (sourceType, targetType, values) <-
        Seq(
          (StringType, cql.TextType, Seq[Any]("x" * 65533, "é" * 32766 + "x")),
          (BinaryType, cql.BlobType, Seq[Any](new Array[Byte](65533)))
        )
    ) {
      val source = StructType(
        Seq(StructField("identity", StringType), StructField("component", sourceType))
      )
      val target = table("identity").copy(clusteringColumns =
        Seq(ColumnDef("component", ClusteringColumn(0), targetType))
      )
      val columns = validation.prepare(source, source, "identity", target)
      for {
        policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
        value  <- values
      } {
        val row = Row("id", value)
        assertEquals(validation.validateRow(row, columns, policy), Some(row))
        val oversized = value match {
          case text: String       => text + "x"
          case bytes: Array[Byte] => bytes :+ 0.toByte
        }
        val errors = scala.collection.mutable.ArrayBuffer.empty[String]
        assertEquals(
          validation.validateRow(Row("id", oversized), columns, policy, errors += _),
          None
        )
        assertEquals(errors.size, 1)
        assert(errors.head.contains("clustering key exceeds 65535 bytes"))
      }
    }
    val columns = Vector(
      validation.Column("identity", cql.TextType, primaryKey  = true, partitionKey = true),
      validation.Column("component", cql.BlobType, primaryKey = true)
    )
    val backing = java.nio.ByteBuffer.wrap(new Array[Byte](70000))
    backing.position(100)
    backing.limit(65634)
    val slice = backing.slice()
    slice.position(1)
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      val row = Row("id", slice)
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
      assertEquals(validation.validateRow(Row("id", backing), columns, policy), None)
    }
    assertEquals(backing.position(), 100)
    assertEquals(backing.limit(), 65634)
    assertEquals(slice.position(), 1)
    assertEquals(slice.limit(), 65534)
  }

  test("renamed composite clustering sizes are independent of partition size and source order") {
    val source = StructType(
      Seq(
        StructField("second", StringType),
        StructField("value", StringType),
        StructField("identity", StringType),
        StructField("first", StringType)
      )
    )
    val renamed = Scylla.renameSchemaFields(
      source,
      List(Rename("first", "temporary"), Rename("temporary", "clustering_one"))
    )
    val target = table("identity", columns = Seq("value" -> cql.TextType)).copy(
      clusteringColumns = Seq(
        ColumnDef("clustering_one", ClusteringColumn(0), cql.TextType),
        ColumnDef("second", ClusteringColumn(1), cql.TextType)
      )
    )
    val columns = validation.prepare(source, renamed, "identity", target)
    assertEquals(columns.map(_.primaryKey), Vector(true, false, true, true))
    assertEquals(columns.map(_.partitionKey), Vector(false, false, true, false))
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      // Each group is exactly 65535 encoded bytes; the regular value has no key-size limit.
      val row = Row("b" * 32766, "v" * 70000, "p" * 65533, "a" * 32765)
      assertEquals(validation.validateRow(row, columns, policy), Some(row))
      assertEquals(
        validation
          .validateRow(Row(row.get(0), row.get(1), row.get(2), "a" * 32766), columns, policy),
        None
      )
      assertEquals(
        validation
          .validateRow(Row(row.get(0), row.get(1), "p" * 65534, row.get(3)), columns, policy),
        None
      )
      val emptyComponents = Row("", "value", "partition", "")
      assertEquals(validation.validateRow(emptyComponents, columns, policy), Some(emptyComponents))
    }
  }

  test("frozen partition collections count their encoded framing and support empty nested values") {
    val source = StructType(
      Seq(StructField("identity", StringType), StructField("items", ArrayType(StringType)))
    )
    val target = table("items", cql.ListType(cql.TextType, isFrozen = true))
      .copy(clusteringColumns = Seq(ColumnDef("identity", ClusteringColumn(0), cql.TextType)))
    val columns = validation.prepare(source, source, "identity", target)
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      for (items <- Seq(Seq.empty[String], Seq("x" * 65525))) {
        val row = Row("id", items)
        assertEquals(validation.validateRow(row, columns, policy), Some(row))
      }
      assertEquals(validation.validateRow(Row("id", Seq("x" * 65526)), columns, policy), None)
    }
    val nestedSource = StructType(
      Seq(
        StructField("identity", StringType),
        StructField("items", ArrayType(MapType(StringType, ArrayType(LongType))))
      )
    )
    for (leafType <- Seq[cql.ColumnType[_]](cql.IntType, cql.VarIntType, cql.DecimalType)) {
      val nestedTarget = target.copy(partitionKey =
        Seq(
          ColumnDef(
            "items",
            PartitionKeyColumn,
            cql.ListType(
              cql.MapType(cql.TextType, cql.ListType(leafType, isFrozen = true), isFrozen = true),
              isFrozen = true
            )
          )
        )
      )
      val nestedColumns = validation.prepare(nestedSource, nestedSource, "identity", nestedTarget)
      // Reuse the nested-value matrix for a clustering column with its own declared codec.
      val clusteringTarget = nestedTarget.copy(
        partitionKey = Seq(ColumnDef("identity", PartitionKeyColumn, cql.TextType)),
        clusteringColumns =
          Seq(ColumnDef("items", ClusteringColumn(0), nestedTarget.partitionKey.head.columnType))
      )
      val clusteringColumns =
        validation.prepare(nestedSource, nestedSource, "identity", clusteringTarget)
      for (
        items <- Seq(
                   Seq(Map.empty[String, Seq[Long]]),
                   Seq(Map("key" -> Seq.empty[Long])),
                   Seq(Map.empty[String, Seq[Long]], Map("key" -> Seq(42L))),
                   Seq(Map("key" -> Seq.empty[Long]), Map("key" -> Seq(42L)))
                 )
      ) {
        val row = Row("id", items)
        for (
          policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)
        ) {
          assertEquals(validation.validateRow(row, nestedColumns, policy), Some(row))
          assertEquals(validation.validateRow(row, clusteringColumns, policy), Some(row))
        }
      }
    }
  }

  test("empty composite partition components and clustering keys remain valid") {
    val compositeSource = StructType(
      Seq(
        StructField("identity", StringType),
        StructField("component", StringType)
      )
    )
    val compositeTarget = table("identity").copy(partitionKey =
      Seq(
        ColumnDef("identity", PartitionKeyColumn, cql.TextType),
        ColumnDef("component", PartitionKeyColumn, cql.TextType)
      )
    )
    val compositeColumns =
      validation.prepare(compositeSource, compositeSource, "identity", compositeTarget)
    assert(compositeColumns.forall(!_.singlePartitionKey))
    val clusteringSource = StructType(
      Seq(
        StructField("identity", StringType),
        StructField("partition", StringType)
      )
    )
    val clusteringTarget = table("partition").copy(clusteringColumns =
      Seq(
        ColumnDef("identity", ClusteringColumn(0), cql.TextType)
      )
    )
    val clusteringColumns =
      validation.prepare(clusteringSource, clusteringSource, "identity", clusteringTarget)
    assert(!clusteringColumns.head.singlePartitionKey)
    assert(clusteringColumns(1).singlePartitionKey)
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null)) {
      assertEquals(validation.validateRow(Row("", ""), compositeColumns, policy), Some(Row("", "")))
      assertEquals(
        validation.validateRow(Row("", "partition"), clusteringColumns, policy),
        Some(Row("", "partition"))
      )
      assertEquals(validation.validateRow(Row("identity", ""), clusteringColumns, policy), None)
    }
  }

}
