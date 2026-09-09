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

  test("validateSchemaAgainstReservedNames: ordinary bin names pass") {
    Aerospike.validateSchemaAgainstReservedNames(ListMap("foo" -> "string", "bar" -> "long"))
  }

  test("validateSchemaAgainstReservedNames: declaring aero_key throws") {
    // Without this guard the key column would be appended a second time.
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateSchemaAgainstReservedNames(ListMap("aero_key" -> "long"))
    }
    assert(
      ex.getMessage.contains("aero_key"),
      s"Expected 'aero_key' in message, got: ${ex.getMessage}"
    )
  }

  test("validateBinsAgainstReservedNames: ordinary bin names pass") {
    Aerospike.validateBinsAgainstReservedNames(Seq("foo", "bar"))
  }

  test("validateBinsAgainstReservedNames: reserved name in the bins filter throws") {
    // Left unchecked, the reserved name is dropped from the schema and an emptied bin filter
    // is sent to Aerospike as "no filter", inverting the intent into fetching every bin.
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateBinsAgainstReservedNames(Seq("aero_key"))
    }
    assert(ex.getMessage.contains("aero_key"), s"Unexpected message: ${ex.getMessage}")
  }

  test("validateBinsAgainstReservedNames: reports every reserved name listed") {
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateBinsAgainstReservedNames(Seq("foo", "aero_ttl", "aero_generation"))
    }
    assert(ex.getMessage.contains("aero_ttl"), s"Unexpected message: ${ex.getMessage}")
    assert(ex.getMessage.contains("aero_generation"), s"Unexpected message: ${ex.getMessage}")
  }

  test("validateSchemaAgainstReservedNames: metadata column names throw") {
    val ex = intercept[IllegalArgumentException] {
      Aerospike.validateSchemaAgainstReservedNames(
        ListMap("foo" -> "string", "aero_ttl" -> "long", "aero_generation" -> "long")
      )
    }
    assert(ex.getMessage.contains("aero_ttl"), s"Unexpected message: ${ex.getMessage}")
    assert(ex.getMessage.contains("aero_generation"), s"Unexpected message: ${ex.getMessage}")
  }
}
