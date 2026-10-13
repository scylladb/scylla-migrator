package com.scylladb.migrator.readers

import com.scylladb.migrator.config.AerospikeKeyMode._

import org.apache.spark.sql.types._

class AerospikeTypesTest extends munit.FunSuite {

  test("HLL values preserve their bytes") {
    val bytes = Array[Byte](0, 1, 127, -128, -1)
    val hll = new com.aerospike.client.Value.HLLValue(bytes)
    assertEquals(
      AerospikeTypes.convertValue(hll, BinaryType).asInstanceOf[Array[Byte]].toSeq,
      bytes.toSeq
    )
    assertEquals(AerospikeTypes.convertValue(hll, StringType), "00017f80ff")
  }

  test(
    "ByteBuffer values copy remaining bytes without changing heap, direct, or read-only buffers"
  ) {
    val padded = Array[Byte](99, 0, 127, -128, -1, 88)
    val heap = java.nio.ByteBuffer.wrap(padded)
    val direct = java.nio.ByteBuffer.allocateDirect(padded.length)
    direct.put(padded)
    for (buffer <- Seq(heap, direct)) {
      buffer.position(1)
      buffer.limit(5)
    }
    val buffers = Seq(
      heap,
      heap.asReadOnlyBuffer(),
      heap.slice(),
      direct,
      direct.asReadOnlyBuffer(),
      direct.slice(),
      direct.slice().asReadOnlyBuffer()
    )
    for (buffer <- buffers) {
      val position = buffer.position()
      val limit = buffer.limit()
      buffer.mark()
      val converted = AerospikeTypes.convertValue(buffer, BinaryType).asInstanceOf[Array[Byte]]
      assertEquals(converted.toSeq, Seq[Byte](0, 127, -128, -1))
      assertEquals(AerospikeTypes.convertValue(buffer, StringType), "007f80ff")
      assertEquals(buffer.position(), position)
      assertEquals(buffer.limit(), limit)
      buffer.reset()
      converted(0) = 42
      assertEquals(AerospikeTypes.convertValue(buffer, StringType), "007f80ff")
    }
    val empty = java.nio.ByteBuffer.allocate(0)
    assertEquals(AerospikeTypes.convertValue(empty, StringType), "")
    assertEquals(
      AerospikeTypes.convertValue(empty, BinaryType).asInstanceOf[Array[Byte]].toSeq,
      Seq.empty[Byte]
    )
  }

  test("ordered map ByteBuffer keys preserve binary contents and render as hex in text") {
    val map = new java.util.TreeMap[java.nio.ByteBuffer, String]()
    map.put(java.nio.ByteBuffer.wrap(Array[Byte](1, 2)), "first")
    map.put(java.nio.ByteBuffer.wrap(Array[Byte](3, 4)), "second")
    val schema = MapType(BinaryType, StringType, valueContainsNull = true)
    assertEquals(schema, MapType(BinaryType, StringType, valueContainsNull = true))
    val converted = AerospikeTypes.convertValue(map, schema).asInstanceOf[Map[Array[Byte], String]]
    assertEquals(
      converted.map { case (key, value) => key.toSeq -> value },
      Map(
        Seq[Byte](1, 2) -> "first",
        Seq[Byte](3, 4) -> "second"
      )
    )
    assertEquals(AerospikeTypes.convertValue(map, StringType), "{0102=first, 0304=second}")
  }

  test("map conversion rejects ByteBuffer keys colliding with byte array contents") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put(Array[Byte](1, 2), "array")
    map.put(java.nio.ByteBuffer.wrap(Array[Byte](1, 2)), "buffer")
    for (keyType <- Seq(BinaryType, StringType)) {
      val error = intercept[IllegalStateException] {
        AerospikeTypes.convertValue(map, MapType(keyType, StringType, true))
      }
      assert(error.getMessage.contains("Distinct Aerospike map keys"))
    }
  }

  test("map conversion rejects colliding numeric and string keys") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put(java.lang.Long.valueOf(1), "numeric")
    map.put("1", "text")
    val schema = MapType(StringType, StringType, valueContainsNull = true)
    val error = intercept[IllegalStateException] {
      AerospikeTypes.convertValue(map, schema)
    }
    assert(error.getMessage.contains("Distinct Aerospike map keys"))
  }

  test("map conversion preserves noncolliding mixed keys") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put(java.lang.Long.valueOf(1), "numeric")
    map.put("2", "text")
    assertEquals(
      AerospikeTypes.convertValue(map, MapType(StringType, StringType, valueContainsNull = true)),
      Map("1" -> "numeric", "2" -> "text")
    )
  }

  test("map conversion compares binary keys by contents as Spark does") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put(Array[Byte](1, 2), "first")
    map.put(Array[Byte](1, 2), "second")
    intercept[IllegalStateException] {
      AerospikeTypes.convertValue(map, MapType(BinaryType, StringType, true))
    }
  }

  // --- convertValue ---

  test("convertValue: null returns null") {
    for (schema <- Seq(StringType, LongType, ArrayType(LongType), MapType(StringType, LongType)))
      assertEquals(AerospikeTypes.convertValue(null, schema), null)
  }

  test("convertValue: Long passthrough") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), LongType), 42L)
  }

  test("convertValue: Long to String coercion") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), StringType), "42")
  }

  test("convertValue: Long to Double preserves exactly representable integers") {
    val boundary = 1L << 53
    val values = Seq(0L, 42L, boundary - 1, boundary, boundary + 2, 1L << 62, Long.MinValue)
    for (value <- values ++ values.filter(_ != Long.MinValue).map(-_)) {
      val converted = AerospikeTypes.convertValue(java.lang.Long.valueOf(value), DoubleType)
      assertEquals(converted, java.lang.Double.valueOf(value.toDouble))
      assertEquals(
        new java.math.BigDecimal(converted.asInstanceOf[Double]).toBigIntegerExact,
        java.math.BigInteger.valueOf(value)
      )
    }
  }

  test("convertValue: Long to Double rejects rounded integers including Long.MaxValue") {
    val boundary = 1L << 53
    for (value <- Seq(boundary + 1, -boundary - 1, Long.MaxValue, Long.MinValue + 1)) {
      val error = intercept[IllegalStateException] {
        AerospikeTypes.convertValue(java.lang.Long.valueOf(value), DoubleType)
      }
      assert(error.getMessage.contains("cannot be represented exactly as DoubleType"))
      assert(error.getMessage.contains("required schema mapping"))
      assert(!error.getMessage.contains(value.toString))
    }
  }

  test("convertValue: String passthrough") {
    assertEquals(AerospikeTypes.convertValue("hello", StringType), "hello")
  }

  test("convertValue: byte array passthrough") {
    val bytes = Array[Byte](1, 2, 3)
    val result = AerospikeTypes.convertValue(bytes, BinaryType).asInstanceOf[Array[Byte]]
    assertEquals(result.toList, bytes.toList)
  }

  test("convertValue: byte array to StringType converts to hex") {
    val bytes = Array[Byte](0x0a, 0x1b, 0x2c)
    val result = AerospikeTypes.convertValue(bytes, StringType)
    assertEquals(result, "0a1b2c")
  }

  test("convertValue: List conversion") {
    val list = java.util.Arrays.asList("a", "b")
    val result = AerospikeTypes.convertValue(list, ArrayType(StringType, containsNull = true))
    assertEquals(result, Seq("a", "b"))
  }

  test("convertValue: List serialized to string when expectedType is StringType") {
    val list = java.util.Arrays.asList(java.lang.Long.valueOf(1L))
    val result = AerospikeTypes.convertValue(list, StringType)
    assertEquals(result, "[1]")
  }

  test("convertValue: Map conversion") {
    val map = new java.util.HashMap[String, String]()
    map.put("k", "v")
    val result = AerospikeTypes.convertValue(
      map,
      MapType(StringType, StringType, valueContainsNull = true)
    )
    assertEquals(result, Map("k" -> "v"))
  }

  test("convertValue: Map serialized to string when expectedType is StringType") {
    val map = new java.util.HashMap[String, String]()
    map.put("k", "v")
    val result = AerospikeTypes.convertValue(map, StringType)
    assertEquals(result, "{k=v}")
  }

  test("convertValue: nested Map with Long values coerced to String") {
    val map = new java.util.HashMap[String, Any]()
    map.put("num", java.lang.Long.valueOf(99L))
    val result = AerospikeTypes.convertValue(
      map,
      MapType(StringType, StringType, valueContainsNull = true)
    )
    assertEquals(result, Map("num" -> "99"))
  }

  test("convertValue: Integer to LongType") {
    val result = AerospikeTypes.convertValue(java.lang.Integer.valueOf(42), LongType)
    assertEquals(result, java.lang.Long.valueOf(42L))
  }

  test("convertValue: Integer to StringType") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Integer.valueOf(42), StringType), "42")
  }

  test("convertValue: Integer to DoubleType") {
    assertEquals(
      AerospikeTypes.convertValue(java.lang.Integer.valueOf(42), DoubleType),
      java.lang.Double.valueOf(42.0)
    )
  }

  test("convertValue: Float to DoubleType") {
    val result = AerospikeTypes.convertValue(java.lang.Float.valueOf(1.5f), DoubleType)
    assertEquals(result, java.lang.Double.valueOf(1.5))
  }

  test("convertValue: Float to StringType") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Float.valueOf(1.5f), StringType), "1.5")
  }

  test("convertValue: Double passthrough") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Double.valueOf(3.14), DoubleType), 3.14)
  }

  test("convertValue: Boolean passthrough") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Boolean.TRUE, BooleanType), true)
  }

  test("convertValue: Boolean to StringType") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Boolean.TRUE, StringType), "true")
  }

  test("convertValue: unexpected type coerces to String for a string column") {
    // java.math.BigDecimal is not a recognized Aerospike type — representable only as text
    assertEquals(AerospikeTypes.convertValue(new java.math.BigDecimal("42"), StringType), "42")
  }

  test("convertValue: incompatible values stop conversion without exposing source data") {
    val cases = Seq[(Any, DataType)](
      new java.math.BigDecimal("42")                 -> LongType,
      "private-source-content"                       -> LongType,
      Array[Byte](1, 2, 3)                           -> LongType,
      java.lang.Boolean.TRUE                         -> LongType,
      java.lang.Long.valueOf(42L)                    -> BinaryType,
      java.lang.Double.valueOf(1.5)                  -> LongType,
      java.util.Arrays.asList[Any](42L)              -> LongType,
      java.util.Collections.singletonMap("key", 42L) -> LongType
    )
    for ((value, expectedType) <- cases) {
      val error = intercept[IllegalStateException] {
        AerospikeTypes.convertValue(value, expectedType)
      }
      assert(error.getMessage.contains(value.getClass.getName))
      assert(error.getMessage.contains(expectedType.toString))
      assert(!error.getMessage.contains("private-source-content"))
    }
  }

  test("convertValue: a map key that fails conversion stops conversion") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(10L))
    map.put("not-a-long", java.lang.Long.valueOf(20L))
    val error = intercept[IllegalStateException] {
      AerospikeTypes.convertValue(map, MapType(LongType, LongType, valueContainsNull = true))
    }
    assert(error.getMessage.contains("map key[1]"))
    assert(!error.getMessage.contains("not-a-long"))
  }

  test("convertValue: a map value that fails conversion stops conversion") {
    val map = new java.util.HashMap[Any, Any]()
    map.put("k", "not-a-long")
    val error = intercept[IllegalStateException] {
      AerospikeTypes.convertValue(map, MapType(StringType, LongType, valueContainsNull = true))
    }
    assert(error.getMessage.contains("map value[0]"))
    assert(!error.getMessage.contains("not-a-long"))
  }

  test("convertValue: null collection members stop conversion regardless of Spark nullability") {
    for (nullable <- Seq(true, false)) {
      val list = java.util.Arrays.asList[Any](42L, null)
      val nullKey = java.util.Collections.singletonMap[Any, Any](null, 42L)
      val nullValue = java.util.Collections.singletonMap[Any, Any]("key", null)
      val cases = Seq[(Any, DataType, String)](
        (list, ArrayType(LongType, nullable), "value[1]"),
        (nullKey, MapType(StringType, LongType, nullable), "map key[0]"),
        (nullValue, MapType(StringType, LongType, nullable), "map value[0]")
      )
      for ((value, schema, path) <- cases) {
        val error = intercept[IllegalStateException] {
          AerospikeTypes.convertValue(value, schema)
        }
        assert(error.getMessage.contains(path))
        assert(error.getMessage.contains("CQL collections cannot store"))
      }
    }
  }

  test("convertValue: nested collection failures report the full path") {
    for (member <- Seq(null, "private-source-content")) {
      val map = java.util.Collections.singletonMap("key", java.util.Arrays.asList[Any](42L, member))
      val list = java.util.Arrays.asList[Any](map)
      val schema = ArrayType(MapType(StringType, ArrayType(LongType, true), true), true)
      val error = intercept[IllegalStateException] {
        AerospikeTypes.convertValue(list, schema)
      }
      assert(error.getMessage.contains("value[0].map value[0][1]"))
      assert(!error.getMessage.contains("private-source-content"))
    }
  }

  test("StringType collections render nested binary contents instead of array identities") {
    def collection(): java.util.List[Any] = {
      val map = new java.util.LinkedHashMap[Any, Any]()
      map.put(Array[Byte](0, -1), Array[Byte](1, 2))
      map.put("nested", java.util.Arrays.asList[Any](Array[Byte](3), null))
      java.util.Arrays.asList[Any](Array[Byte](4), map)
    }
    val expected = "[04, {00ff=0102, nested=[03, null]}]"
    assertEquals(AerospikeTypes.convertValue(collection(), StringType), expected)
    // Separate instances with the same contents must produce identical text.
    assertEquals(AerospikeTypes.convertValue(collection(), StringType), expected)

  }

  test("StringType collections preserve Java scalar formatting and nested punctuation") {
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put("text", "hello")
    map.put(42L, java.util.Arrays.asList[Any](1, 2L, 3.5f, 4.25d, true, null))
    map.put("empty", new java.util.ArrayList[Any]())
    map.put("map", new java.util.LinkedHashMap[Any, Any]())
    assertEquals(AerospikeTypes.convertValue(map, StringType), map.toString)
    assertEquals(
      AerospikeTypes.convertValue(map, StringType),
      "{text=hello, 42=[1, 2, 3.5, 4.25, true, null], empty=[], map={}}"
    )
  }

  test("StringType nested HLL and GeoJSON values use the same text as scalar bins") {
    val hll = new com.aerospike.client.Value.HLLValue(Array[Byte](0, 127, -1))
    val json = """{"type":"Point","coordinates":[1,2]}"""
    val geo = new com.aerospike.client.Value.GeoJSONValue(json)
    val map = new java.util.LinkedHashMap[Any, Any]()
    map.put("hll", hll)
    map.put("geo", geo)
    val list = java.util.Arrays.asList[Any](map)
    assertEquals(AerospikeTypes.convertValue(list, StringType), s"[{hll=007fff, geo=$json}]")
    assertEquals(AerospikeTypes.convertValue(hll, StringType), "007fff")
    assertEquals(AerospikeTypes.convertValue(geo, StringType), json)
  }

  test("explicit string types preserve large integers and mixed numeric values") {
    assertEquals(AerospikeTypes.convertValue(9007199254740993L, StringType), "9007199254740993")
    assertEquals(AerospikeTypes.convertValue(1.5, StringType), "1.5")
  }

  // --- extractKey ---

  test("stored modes preserve original string, integer, and binary keys") {
    val bytes = Array[Byte](0, 1, -1)
    assertEquals(
      AerospikeTypes.extractKey(new com.aerospike.client.Key("ns", "set", "42"), StringKey),
      "42"
    )
    assertEquals(
      AerospikeTypes.extractKey(new com.aerospike.client.Key("ns", "set", 42L), IntegerKey),
      42L
    )
    val result =
      AerospikeTypes.extractKey(new com.aerospike.client.Key("ns", "set", bytes), BinaryKey)
    assertEquals(result.asInstanceOf[Array[Byte]].toSeq, bytes.toSeq)
  }

  test("stored modes reject every mismatched key, including numeric 42 versus string 42") {
    val keys = Seq(
      StringKey  -> new com.aerospike.client.Key("ns", "set", "42"),
      IntegerKey -> new com.aerospike.client.Key("ns", "set", 42L),
      BinaryKey  -> new com.aerospike.client.Key("ns", "set", Array[Byte](42))
    )
    for {
      (mode, _)         <- keys
      (actualMode, key) <- keys if mode != actualMode
    } {
      val ex = intercept[IllegalStateException](AerospikeTypes.extractKey(key, mode))
      assert(ex.getMessage.contains("keyMode"))
      assert(ex.getMessage.contains("digest"))
    }
  }

  test("stored modes reject missing keys instead of falling back to colliding digest values") {
    val digest = Array.fill[Byte](20)(42)
    val missing = new com.aerospike.client.Key("ns", digest, "set", null)
    val hex = java.util.HexFormat.of().formatHex(digest)
    assertEquals(
      AerospikeTypes.extractKey(new com.aerospike.client.Key("ns", "set", hex), StringKey),
      hex
    )
    val binary =
      AerospikeTypes.extractKey(new com.aerospike.client.Key("ns", "set", digest), BinaryKey)
    assertEquals(binary.asInstanceOf[Array[Byte]].toSeq, digest.toSeq)
    for (mode <- Seq(StringKey, IntegerKey, BinaryKey)) {
      val ex = intercept[IllegalStateException](AerospikeTypes.extractKey(missing, mode))
      assert(ex.getMessage.contains("no stored user key"))
    }
  }

  test("digest mode uses the same hex representation for stored and absent mixed keys") {
    val keys = Seq(
      new com.aerospike.client.Key("ns", "set", "42"),
      new com.aerospike.client.Key("ns", "set", 42L),
      new com.aerospike.client.Key("ns", "set", Array[Byte](42))
    )
    val values = keys.map { key =>
      val expected = java.util.HexFormat.of().formatHex(key.digest)
      assertEquals(AerospikeTypes.extractKey(key, Digest), expected)
      val missing = new com.aerospike.client.Key("ns", key.digest, "set", null)
      assertEquals(AerospikeTypes.extractKey(missing, Digest), expected)
      assert(expected.matches("[0-9a-f]{40}"))
      expected
    }
    assertEquals(values.distinct.size, keys.size)
    // A stored string equal to another record's digest must itself be digested.
    val digestStringKey = new com.aerospike.client.Key("ns", "set", values.head)
    assert(AerospikeTypes.extractKey(digestStringKey, Digest) != values.head)
  }

  // --- parseType ---

  test("parseType: string") {
    assertEquals(AerospikeTypes.parseType("string"), StringType)
  }

  test("parseType: text alias") {
    assertEquals(AerospikeTypes.parseType("text"), StringType)
  }

  test("parseType: long") {
    assertEquals(AerospikeTypes.parseType("long"), LongType)
  }

  test("parseType: bigint alias") {
    assertEquals(AerospikeTypes.parseType("bigint"), LongType)
  }

  test("parseType: double") {
    assertEquals(AerospikeTypes.parseType("double"), DoubleType)
  }

  test("parseType: binary") {
    assertEquals(AerospikeTypes.parseType("binary"), BinaryType)
  }

  test("parseType: blob alias") {
    assertEquals(AerospikeTypes.parseType("blob"), BinaryType)
  }

  test("parseType: boolean") {
    assertEquals(AerospikeTypes.parseType("boolean"), BooleanType)
  }

  test("parseType: bool alias") {
    assertEquals(AerospikeTypes.parseType("bool"), BooleanType)
  }

  test("parseType: case insensitive") {
    assertEquals(AerospikeTypes.parseType("STRING"), StringType)
    assertEquals(AerospikeTypes.parseType("Long"), LongType)
  }

  test("parseType: schema types are independent of the default locale") {
    val previous = java.util.Locale.getDefault
    try {
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"))
      assertEquals(AerospikeTypes.parseType("INT"), LongType)
      assertEquals(AerospikeTypes.parseType("LIST<STRING>"), ArrayType(StringType, true))
      assertEquals(
        AerospikeTypes.parseType("MAP<STRING,LIST<INTEGER>>"),
        MapType(StringType, ArrayType(LongType, true), true)
      )
    } finally java.util.Locale.setDefault(previous)
  }

  test("parseType: int alias") {
    assertEquals(AerospikeTypes.parseType("int"), LongType)
  }

  test("parseType: integer alias") {
    assertEquals(AerospikeTypes.parseType("integer"), LongType)
  }

  test("parseType: list<string>") {
    assertEquals(
      AerospikeTypes.parseType("list<string>"),
      ArrayType(StringType, containsNull = true)
    )
  }

  test("parseType: list<long>") {
    assertEquals(
      AerospikeTypes.parseType("list<long>"),
      ArrayType(LongType, containsNull = true)
    )
  }

  test("parseType: map<string,long>") {
    assertEquals(
      AerospikeTypes.parseType("map<string,long>"),
      MapType(StringType, LongType, valueContainsNull = true)
    )
  }

  test("parseType: map<string,double>") {
    assertEquals(
      AerospikeTypes.parseType("map<string,double>"),
      MapType(StringType, DoubleType, valueContainsNull = true)
    )
  }

  test("parseType: nested list<map<string,long>>") {
    assertEquals(
      AerospikeTypes.parseType("list<map<string,long>>"),
      ArrayType(MapType(StringType, LongType, valueContainsNull = true), containsNull = true)
    )
  }

  test("parseType: collection types are case insensitive") {
    assertEquals(
      AerospikeTypes.parseType("List<String>"),
      ArrayType(StringType, containsNull = true)
    )
    assertEquals(
      AerospikeTypes.parseType("MAP<STRING,LONG>"),
      MapType(StringType, LongType, valueContainsNull = true)
    )
  }

  test("parseType: unknown type throws") {
    intercept[IllegalArgumentException] {
      AerospikeTypes.parseType("timestamp")
    }
  }

  test("parseType: invalid map type throws") {
    intercept[IllegalArgumentException] {
      AerospikeTypes.parseType("map<string>")
    }
  }
}
