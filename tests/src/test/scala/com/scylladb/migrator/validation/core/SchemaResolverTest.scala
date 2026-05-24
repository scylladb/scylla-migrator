package com.scylladb.migrator.validation.core

import com.scylladb.migrator.config.Rename
import munit.FunSuite

class SchemaResolverTest extends FunSuite {

  test("buildCaseInsensitiveRenameMap compiles mixed case mapping") {
    val renames = List(
      Rename("Foo", "bar"),
      Rename("bAz", "qux")
    )
    val map = SchemaResolver.buildCaseInsensitiveRenameMap(renames)
    assertEquals(map.get("foo"), Some("bar"))
    assertEquals(map.get("baz"), Some("qux"))
  }

  test("buildCaseInsensitiveRenameMap rejects conflicting case-insensitive renames") {
    val renames = List(
      Rename("Foo", "bar"),
      Rename("foo", "other")
    )
    intercept[RuntimeException] {
      SchemaResolver.buildCaseInsensitiveRenameMap(renames)
    }
  }

  test("escapeSparkColumnName escapes backticks correctly") {
    assertEquals(SchemaResolver.escapeSparkColumnName("foo"), "`foo`")
    assertEquals(SchemaResolver.escapeSparkColumnName("foo`bar"), "`foo``bar`")
  }

  test("resolveFieldName returns case insensitive match") {
    val fields = Array("Id", "Name", "age")
    assertEquals(SchemaResolver.resolveFieldName(fields, "id"), "Id")
    assertEquals(SchemaResolver.resolveFieldName(fields, "NAME"), "Name")
    assertEquals(SchemaResolver.resolveFieldName(fields, "Age"), "age")
  }

  test("resolveFieldName throws exception if not found") {
    val fields = Array("Id", "Name")
    intercept[RuntimeException] {
      SchemaResolver.resolveFieldName(fields, "missing")
    }
  }
}
