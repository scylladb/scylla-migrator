package com.scylladb.migrator.readers.jdbc

class JdbcMetadataTest extends munit.FunSuite {

  test("escapeMetadataPattern escapes wildcards and the escape character itself") {
    assertEquals(
      JdbcMetadata.escapeMetadataPattern("""user_events_2024%backup\archive""", "\\"),
      """user\_events\_2024\%backup\\archive"""
    )
  }

  test("escapeMetadataPattern falls back to '\\\\' when the search escape is null") {
    assertEquals(
      JdbcMetadata.escapeMetadataPattern("user_table", null),
      """user\_table"""
    )
  }

  test(
    "escapeMetadataPattern returns the pattern unchanged when the driver does not use a character escape"
  ) {
    // Drivers that return an empty `getSearchStringEscape()` (notably SQL Server, which uses
    // bracket escaping like `[_]`) signal "no escape character available". Forcing a `\` fallback
    // here would inject literal backslashes into the metadata pattern and the JDBC metadata
    // lookup would fail to match the intended table.
    assertEquals(
      JdbcMetadata.escapeMetadataPattern("user_table", ""),
      "user_table"
    )
    assertEquals(
      JdbcMetadata.escapeMetadataPattern("events_2024%backup", ""),
      "events_2024%backup"
    )
  }

  test("escapeMetadataPattern preserves table names with no wildcards") {
    assertEquals(
      JdbcMetadata.escapeMetadataPattern("events", "\\"),
      "events"
    )
  }
}
