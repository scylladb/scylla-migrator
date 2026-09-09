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

  test("convertValue: unexpected type coerces to String for a string column") {
    // java.math.BigDecimal is not a recognized Aerospike type — representable only as text
    assertEquals(AerospikeTypes.convertValue(new java.math.BigDecimal("42"), StringType), "42")
  }

  test("convertValue: unexpected type yields null for a non-string column") {
    // Returning "42" here would blow up in Spark's row encoder for a LongType column.
    assertEquals(AerospikeTypes.convertValue(new java.math.BigDecimal("42"), LongType), null)
  }

  test("convertValue: String in a Long column yields null") {
    // Bin type drifted after schema sampling; must not reach the encoder as a String.
    assertEquals(AerospikeTypes.convertValue("hello", LongType), null)
  }

  test("convertValue: byte array in a Long column yields null") {
    assertEquals(AerospikeTypes.convertValue(Array[Byte](1, 2, 3), LongType), null)
  }

  test("convertValue: Boolean in a Long column yields null") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Boolean.TRUE, LongType), null)
  }

  test("convertValue: Long in a Binary column yields null") {
    assertEquals(AerospikeTypes.convertValue(java.lang.Long.valueOf(42L), BinaryType), null)
  }

  test("convertValue: map entry whose key fails conversion is dropped, not nulled") {
    // Spark map keys are non-nullable, so a drifted key must not become a null key.
    val map = new java.util.HashMap[Any, Any]()
    map.put(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(10L))
    map.put("not-a-long", java.lang.Long.valueOf(20L))
    val result = AerospikeTypes
      .convertValue(map, MapType(LongType, LongType, valueContainsNull = true))
      .asInstanceOf[Map[Any, Any]]
    assertEquals(result, Map[Any, Any](1L -> 10L))
    assert(!result.contains(null), "a null map key must never be emitted")
  }

  test("convertValue: two drifted map keys do not collapse into one entry") {
    // Both keys would convert to null and `.toMap` would silently keep only one.
    val map = new java.util.HashMap[Any, Any]()
    map.put("a", java.lang.Long.valueOf(1L))
    map.put("b", java.lang.Long.valueOf(2L))
    val result = AerospikeTypes
      .convertValue(map, MapType(LongType, LongType, valueContainsNull = true))
      .asInstanceOf[Map[Any, Any]]
    assertEquals(result, Map.empty[Any, Any])
  }

  test("convertValue: map values that fail conversion are still nulled, keys preserved") {
    val map = new java.util.HashMap[Any, Any]()
    map.put("k", "not-a-long")
    val result = AerospikeTypes
      .convertValue(map, MapType(StringType, LongType, valueContainsNull = true))
      .asInstanceOf[Map[Any, Any]]
    assertEquals(result, Map[Any, Any]("k" -> null))
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

  test("extractKey: byte[] userKey under BinaryType passes through") {
    val bytes = Array[Byte](1, 2, 3)
    val key = new com.aerospike.client.Key("ns", "set", bytes)
    val result = AerospikeTypes.extractKey(key, BinaryType).asInstanceOf[Array[Byte]]
    assertEquals(result.toSeq, bytes.toSeq)
  }

  test("extractKey: non-binary userKey under BinaryType fails instead of reaching the encoder") {
    // Sample saw only byte[] keys; a later String key is not representable as BinaryType.
    val key = new com.aerospike.client.Key("ns", "set", "a-string-key")
    val ex = intercept[IllegalStateException] {
      AerospikeTypes.extractKey(key, BinaryType)
    }
    assert(
      ex.getMessage.contains("does not match the discovered key type"),
      s"Unexpected message: ${ex.getMessage}"
    )
  }

  test("extractKey: key type not matching the inferred type fails with guidance") {
    // Sampling inferred LongType but this record has a String key: aero_key is non-nullable,
    // so this must fail loudly instead of handing a String to a LongType field.
    val key = new com.aerospike.client.Key("ns", "set", "not-a-long")
    val ex = intercept[IllegalStateException] {
      AerospikeTypes.extractKey(key, LongType)
    }
    assert(
      ex.getMessage.contains("does not match the discovered key type"),
      s"Unexpected message: ${ex.getMessage}"
    )
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
