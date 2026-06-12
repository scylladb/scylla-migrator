package com.scylladb.migrator.readers

import org.apache.spark.sql.types._

class AerospikeTypesTest extends munit.FunSuite {

  // --- inferSparkType ---

  test("inferSparkType: Long") {
    assertEquals(AerospikeTypes.inferSparkType(java.lang.Long.valueOf(42L)), LongType)
  }

  test("inferSparkType: Integer maps to LongType") {
    assertEquals(AerospikeTypes.inferSparkType(java.lang.Integer.valueOf(42)), LongType)
  }

  test("inferSparkType: Double") {
    assertEquals(AerospikeTypes.inferSparkType(java.lang.Double.valueOf(3.14)), DoubleType)
  }

  test("inferSparkType: Float maps to DoubleType") {
    assertEquals(AerospikeTypes.inferSparkType(java.lang.Float.valueOf(1.5f)), DoubleType)
  }

  test("inferSparkType: String") {
    assertEquals(AerospikeTypes.inferSparkType("hello"), StringType)
  }

  test("inferSparkType: byte array") {
    assertEquals(AerospikeTypes.inferSparkType(Array[Byte](1, 2, 3)), BinaryType)
  }

  test("inferSparkType: Boolean maps to BooleanType") {
    assertEquals(AerospikeTypes.inferSparkType(java.lang.Boolean.TRUE), BooleanType)
  }

  test("inferSparkType: List of strings") {
    val list = java.util.Arrays.asList("a", "b", "c")
    assertEquals(
      AerospikeTypes.inferSparkType(list),
      ArrayType(StringType, containsNull = true)
    )
  }

  test("inferSparkType: List of longs") {
    val list = java.util.Arrays.asList(
      java.lang.Long.valueOf(1L),
      java.lang.Long.valueOf(2L)
    )
    assertEquals(
      AerospikeTypes.inferSparkType(list),
      ArrayType(LongType, containsNull = true)
    )
  }

  test("inferSparkType: List with mixed types falls back to StringType elements") {
    val list = new java.util.ArrayList[Any]()
    list.add(java.lang.Long.valueOf(1L))
    list.add("hello")
    assertEquals(
      AerospikeTypes.inferSparkType(list),
      ArrayType(StringType, containsNull = true)
    )
  }

  test("inferSparkType: empty list falls back to StringType elements") {
    val list = new java.util.ArrayList[Any]()
    assertEquals(
      AerospikeTypes.inferSparkType(list),
      ArrayType(StringType, containsNull = true)
    )
  }

  test("inferSparkType: Map of string to string") {
    val map = new java.util.HashMap[String, String]()
    map.put("k", "v")
    assertEquals(
      AerospikeTypes.inferSparkType(map),
      MapType(StringType, StringType, valueContainsNull = true)
    )
  }

  test("inferSparkType: Map of string to long") {
    val map = new java.util.HashMap[Any, Any]()
    map.put("k", java.lang.Long.valueOf(42L))
    assertEquals(
      AerospikeTypes.inferSparkType(map),
      MapType(StringType, LongType, valueContainsNull = true)
    )
  }

  test("inferSparkType: nested list inside map") {
    val inner = java.util.Arrays.asList("a", "b")
    val map = new java.util.HashMap[Any, Any]()
    map.put("list", inner)
    val result = AerospikeTypes.inferSparkType(map)
    assertEquals(
      result,
      MapType(StringType, ArrayType(StringType, containsNull = true), valueContainsNull = true)
    )
  }

  // --- convertValue ---

  test("convertValue: null returns null") {
    assertEquals(AerospikeTypes.convertValue(null, StringType), null)
  }

  test("convertValue: Long passthrough") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), LongType), 42L)
  }

  test("convertValue: Long to String coercion") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), StringType), "42")
  }

  test("convertValue: Long to Double widening") {
    assertEquals(
      AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), DoubleType),
      java.lang.Double.valueOf(42.0)
    )
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

  test("convertValue: unexpected type coerces to String") {
    // java.math.BigDecimal is not a recognized Aerospike type — should be coerced to String
    assertEquals(AerospikeTypes.convertValue(new java.math.BigDecimal("42"), LongType), "42")
  }

  // --- mergeTypes ---

  test("mergeTypes: identical scalar types") {
    assertEquals(AerospikeTypes.mergeTypes(LongType, LongType), LongType)
    assertEquals(AerospikeTypes.mergeTypes(DoubleType, DoubleType), DoubleType)
    assertEquals(AerospikeTypes.mergeTypes(StringType, StringType), StringType)
  }

  test("mergeTypes: Long and Double widen to Double") {
    assertEquals(AerospikeTypes.mergeTypes(LongType, DoubleType), DoubleType)
    assertEquals(AerospikeTypes.mergeTypes(DoubleType, LongType), DoubleType)
  }

  test("mergeTypes: identical ArrayTypes") {
    val result = AerospikeTypes.mergeTypes(
      ArrayType(LongType, containsNull = false),
      ArrayType(LongType, containsNull = true)
    )
    assertEquals(result, ArrayType(LongType, containsNull = true))
  }

  test("mergeTypes: different ArrayType element types fall back to StringType") {
    val result = AerospikeTypes.mergeTypes(
      ArrayType(LongType, containsNull   = false),
      ArrayType(StringType, containsNull = false)
    )
    assertEquals(result, ArrayType(StringType, containsNull = false))
  }

  test("mergeTypes: identical MapTypes") {
    val result = AerospikeTypes.mergeTypes(
      MapType(StringType, LongType, valueContainsNull = false),
      MapType(StringType, LongType, valueContainsNull = true)
    )
    assertEquals(result, MapType(StringType, LongType, valueContainsNull = true))
  }

  test("mergeTypes: MapType value types Long and Double widen to Double") {
    val result = AerospikeTypes.mergeTypes(
      MapType(StringType, LongType, valueContainsNull   = false),
      MapType(StringType, DoubleType, valueContainsNull = false)
    )
    assertEquals(result, MapType(StringType, DoubleType, valueContainsNull = false))
  }

  test("mergeTypes: scalar type conflict falls back to StringType") {
    assertEquals(AerospikeTypes.mergeTypes(LongType, StringType), StringType)
  }

  test("mergeTypes: ArrayType vs MapType falls back to StringType") {
    assertEquals(
      AerospikeTypes.mergeTypes(
        ArrayType(StringType, containsNull                = true),
        MapType(StringType, StringType, valueContainsNull = true)
      ),
      StringType
    )
  }

  // --- extractKey ---

  test("extractKey: userKey present with StringType") {
    val key = new com.aerospike.client.Key("ns", "set", "mykey")
    assertEquals(AerospikeTypes.extractKey(key, StringType), "mykey")
  }

  test("extractKey: userKey present with LongType") {
    val key = new com.aerospike.client.Key("ns", "set", 42L)
    assertEquals(AerospikeTypes.extractKey(key, LongType), 42L)
  }

  test("extractKey: Long userKey coerced to StringType") {
    val key = new com.aerospike.client.Key("ns", "set", 42L)
    assertEquals(AerospikeTypes.extractKey(key, StringType), "42")
  }

  test("extractKey: null userKey falls back to hex digest") {
    // Construct a key with a known digest by providing a string key, then verify
    // that when userKey is absent the digest is returned as hex.
    // Use the byte[] constructor to avoid ambiguity, then extractKey sees userKey=null.
    val digest = Array[Byte](
      0x0a,
      0x1b,
      0x2c,
      0x3d,
      0x4e,
      0x5f,
      0x60,
      0x71,
      0x82.toByte,
      0x93.toByte,
      0xa4.toByte,
      0xb5.toByte,
      0xc6.toByte,
      0xd7.toByte,
      0xe8.toByte,
      0xf9.toByte,
      0x01,
      0x23,
      0x45,
      0x67
    )
    val key = new com.aerospike.client.Key("ns", digest, null, null)
    val result = AerospikeTypes.extractKey(key, StringType).asInstanceOf[String]
    assert(result.matches("[0-9a-f]+"), s"Expected hex digest, got: $result")
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
