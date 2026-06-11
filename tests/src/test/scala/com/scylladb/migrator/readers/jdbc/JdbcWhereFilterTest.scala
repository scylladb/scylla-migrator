package com.scylladb.migrator.readers.jdbc

class JdbcWhereFilterTest extends munit.FunSuite {

  test("requireNonBlank rejects empty and whitespace-only filters") {
    intercept[RuntimeException](JdbcWhereFilter.requireNonBlank(""))
    intercept[RuntimeException](JdbcWhereFilter.requireNonBlank("   \t  "))
  }

  test("requireNonBlank accepts non-blank filters") {
    JdbcWhereFilter.requireNonBlank("id > 0")
  }

  test("requireNoControlCharacters rejects newlines and null bytes") {
    intercept[RuntimeException](JdbcWhereFilter.requireNoControlCharacters("id > 0\n"))
    intercept[RuntimeException](JdbcWhereFilter.requireNoControlCharacters("id > 0\u0000"))
  }

  test("requireNoControlCharacters accepts normal SQL filters") {
    JdbcWhereFilter.requireNoControlCharacters("id > 0 AND name = 'foo'")
  }

  test("requireNoControlCharacters rejects Unicode line/paragraph separators (U+2028, U+2029)") {
    // Regression: `Character.isISOControl` covers only Cc; U+2028 (Zl) and U+2029 (Zp) pass.
    // Several loggers treat them as newlines, opening a log-forging / multi-line-injection
    // vector if accepted by the filter.
    intercept[RuntimeException](JdbcWhereFilter.requireNoControlCharacters("id > 0\u2028DROP"))
    intercept[RuntimeException](JdbcWhereFilter.requireNoControlCharacters("id > 0\u2029DROP"))
  }

  test("redactedLogMessage does not expose the filter contents but includes its length") {
    val filter = "email = 'user@example.com' AND api_token = 'secret'"
    val msg = JdbcWhereFilter.redactedLogMessage("MySQL", filter)

    assert(msg.contains("MySQL"))
    assert(msg.contains("content redacted"))
    assert(msg.contains(filter.length.toString))
    assert(!msg.contains(filter))
  }

  test("redactedLogMessage embeds the backend name verbatim") {
    val msg = JdbcWhereFilter.redactedLogMessage("Postgres", "ok")
    assert(msg.startsWith("Applying configured WHERE filter to Postgres read"))
  }

  test("validateKeywords rejects keyword matches after running the dialect stripper") {
    val pattern = """\b(drop|truncate)\b""".r
    val identityStrip: String => String = identity
    val err = intercept[RuntimeException] {
      JdbcWhereFilter.validateKeywords("id = 1; DROP TABLE users", pattern, identityStrip, "MySQL")
    }
    assert(err.getMessage.contains("for MySQL"))
    assert(err.getMessage.contains("'drop'"))
  }

  test("validateKeywords trusts the supplied stripper to mask out quoted contents") {
    // A backend's stripper is responsible for removing literal-string contents. The helper does
    // not double-check; if the stripper hides 'DROP' inside a quoted string, the scan accepts it.
    val pattern = """\b(drop)\b""".r
    val maskQuoted: String => String = _.replaceAll("'[^']*'", "''")
    JdbcWhereFilter.validateKeywords("name = 'DROP'", pattern, maskQuoted, "MySQL")
  }

  test("validateKeywords accepts safe filters") {
    val pattern = """\b(drop|delete)\b""".r
    JdbcWhereFilter.validateKeywords("id > 0 AND created_at < NOW()", pattern, identity, "MySQL")
  }
}
