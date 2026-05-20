package com.scylladb.migrator.readers.jdbc

import java.util.Locale

/** Backend-neutral structural checks for user-supplied JDBC WHERE filters.
  *
  * Each JDBC backend builds its own dialect-specific keyword/comment scanner because executable SQL
  * comment syntax and identifier-escape semantics differ. This object covers the parts that are
  * universally safe to enforce — blank-filter rejection, control-character rejection, a uniform
  * redacted-log message — plus a pluggable scanner ([[validateKeywords]]) so each backend can
  * compose its dangerous-keyword regex and dialect-specific comment/quote stripper through a single
  * audit point rather than re-implementing the scan inline.
  */
object JdbcWhereFilter {

  /** Reject a blank or whitespace-only filter. Throws `RuntimeException` (via `sys.error`) because
    * the caller treats config-bypass as a programmatic error.
    */
  def requireNonBlank(filter: String): Unit =
    if (filter.trim.isEmpty)
      sys.error(
        "WHERE clause is empty or blank. Remove the 'where' key or provide a valid filter."
      )

  /** Reject filters that contain any ASCII control characters (newlines, NUL, etc.). Control
    * characters in SQL filters typically indicate copy/paste corruption or injection attempts. Also
    * rejects Unicode Line Separator (U+2028) and Paragraph Separator (U+2029): these are
    * categorized Zl/Zp and therefore pass `Character.isISOControl`, but several loggers and SQL
    * tools treat them as newlines, opening a log-forging or multi-line-injection vector if
    * accepted.
    */
  def requireNoControlCharacters(filter: String): Unit =
    if (filter.exists(c => c.isControl || c == '\u2028' || c == '\u2029'))
      sys.error(
        s"WHERE clause contains control characters (newlines, null bytes, etc.) which are not allowed. " +
          s"Filter length: ${filter.length} characters"
      )

  /** Build the redacted log message emitted right before a filter is forwarded to JDBC. Reveals
    * only the filter length so secrets embedded in the predicate do not leak into log aggregators.
    */
  def redactedLogMessage(backendName: String, filter: String): String =
    s"Applying configured WHERE filter to $backendName read " +
      s"(content redacted, ${filter.length} characters)"

  /** Pluggable dangerous-keyword scan for backend-specific WHERE filters.
    *
    * Each JDBC backend supplies its own dangerous-keyword regex and its own dialect-specific
    * function that removes comments and the contents of quoted strings/identifiers from the filter
    * before the regex scan. Centralizing the scan structure here makes the security check a
    * composition concern (regex + stripper) rather than a duplicate full implementation in each
    * backend, so future backends inherit the defense by construction.
    *
    * @param filter
    *   user-supplied filter (untrusted).
    * @param dangerousPattern
    *   regex of forbidden keywords, e.g. `\b(drop|delete|truncate|...)\b`. Matched against the
    *   stripped lower-cased filter.
    * @param stripDialect
    *   backend-specific function that removes comments and quoted-string contents from `filter`,
    *   leaving only SQL structure for the regex to scan. Backends supply their own to handle
    *   dialect comment syntax (`--`, `/*!...*/`, `$$..$$`, etc.).
    * @param dialectName
    *   human-readable backend label embedded in the error message.
    */
  def validateKeywords(
    filter: String,
    dangerousPattern: scala.util.matching.Regex,
    stripDialect: String => String,
    dialectName: String
  ): Unit = {
    val filterForScan = stripDialect(filter).toLowerCase(Locale.ROOT)
    dangerousPattern.findFirstIn(filterForScan).foreach { keyword =>
      sys.error(
        s"WHERE clause contains potentially dangerous SQL keyword(s) for $dialectName: " +
          s"matched '$keyword'. " +
          "DDL/DML keywords (DROP, DELETE, TRUNCATE, ALTER, CREATE, EXEC, EXECUTE, UNION, " +
          "INTO, OUTFILE, DUMPFILE, LOAD_FILE, BENCHMARK, SLEEP, GRANT, REVOKE) " +
          "are not allowed in WHERE filters."
      )
    }
  }
}
