package com.scylladb.migrator.config

class RenameTest extends munit.FunSuite {

  test("buildCaseInsensitiveMap creates lowercase-keyed map") {
    val renames = List(Rename("UserId", "user_id"), Rename("Email", "email_addr"))
    val result = Rename.buildCaseInsensitiveMap(renames)
    assertEquals(result, Map("userid" -> "user_id", "email" -> "email_addr"))
  }

  test("buildCaseInsensitiveMap detects conflicting case-insensitive mappings") {
    val renames = List(Rename("Foo", "bar"), Rename("foo", "baz"))
    val error = intercept[RuntimeException] {
      Rename.buildCaseInsensitiveMap(renames)
    }
    assert(error.getMessage.contains("conflicting case-insensitive mappings"))
    assert(error.getMessage.contains("foo"))
  }

  test("buildCaseInsensitiveMap allows same-target duplicates (idempotent)") {
    val renames = List(Rename("Foo", "bar"), Rename("foo", "bar"))
    val result = Rename.buildCaseInsensitiveMap(renames)
    assertEquals(result, Map("foo" -> "bar"))
  }

  test("buildCaseInsensitiveMap empty list returns empty map") {
    assertEquals(Rename.buildCaseInsensitiveMap(Nil), Map.empty[String, String])
  }

  test("resolveRename returns mapped value when present") {
    val map = Map("userid" -> "user_id")
    assertEquals(Rename.resolveRename(map, "UserId"), "user_id")
    assertEquals(Rename.resolveRename(map, "userid"), "user_id")
    assertEquals(Rename.resolveRename(map, "USERID"), "user_id")
  }

  test("resolveRename returns identity when not in map") {
    val map = Map("userid" -> "user_id")
    assertEquals(Rename.resolveRename(map, "Email"), "Email")
  }

  test("detectTargetCollisions finds multiple sources mapping to same target") {
    val renames = List(Rename("a", "X"), Rename("b", "x"))
    val errors = Rename.detectTargetCollisions(renames)
    assertEquals(errors.size, 1)
    assert(errors.head.contains("a"))
    assert(errors.head.contains("b"))
  }

  test("detectTargetCollisions returns empty for clean renames") {
    val renames = List(Rename("a", "x"), Rename("b", "y"))
    assertEquals(Rename.detectTargetCollisions(renames), Nil)
  }

  test("detectTargetCollisions empty list returns empty") {
    assertEquals(Rename.detectTargetCollisions(Nil), Nil)
  }
}
