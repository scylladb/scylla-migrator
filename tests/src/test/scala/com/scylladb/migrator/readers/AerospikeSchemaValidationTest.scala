package com.scylladb.migrator.readers

import scala.collection.immutable.ListMap

class AerospikeSchemaValidationTest extends munit.FunSuite {

  test("validateSchemaAgainstBins: schema subset of bins passes") {
    val schema = ListMap("foo" -> "string", "bar" -> "long")
    val bins = Seq("foo", "bar", "baz")
    // Should not throw
    Aerospike.validateSchemaAgainstBins(schema, bins)
  }

  test("validateSchemaAgainstBins: exact match passes") {
    val schema = ListMap("foo" -> "string")
    val bins = Seq("foo")
    Aerospike.validateSchemaAgainstBins(schema, bins)
  }

  test("validateSchemaAgainstBins: schema declares bins not in filter throws") {
    val schema = ListMap("foo" -> "string", "extra" -> "long")
    val bins = Seq("foo")
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateSchemaAgainstBins(schema, bins)
    }
    assert(ex.getMessage.contains("extra"), s"Expected 'extra' in message, got: ${ex.getMessage}")
    assert(
      ex.getMessage.contains("not in the 'bins' filter"),
      s"Expected guidance in message, got: ${ex.getMessage}"
    )
  }

  test("validateSchemaAgainstBins: multiple extra bins listed in error") {
    val schema = ListMap("a" -> "string", "b" -> "long", "c" -> "double")
    val bins = Seq("a")
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateSchemaAgainstBins(schema, bins)
    }
    assert(
      ex.getMessage.contains("2 bin(s)"),
      s"Expected '2 bin(s)' in message, got: ${ex.getMessage}"
    )
  }

  test("validateSchemaAgainstBins: empty bins filter with non-empty schema throws") {
    val schema = ListMap("foo" -> "string")
    val bins = Seq.empty[String]
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateSchemaAgainstBins(schema, bins)
    }
    assert(ex.getMessage.contains("foo"))
  }
}
