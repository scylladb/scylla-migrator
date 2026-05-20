package com.scylladb.migrator.readers.jdbc

import java.sql.Connection
import scala.util.Using

/** Backend-neutral helpers for inspecting JDBC `DatabaseMetaData`.
  *
  * The functions here intentionally take a `java.sql.Connection` rather than a typed source
  * settings object so they can be reused by any future JDBC backend.
  */
object JdbcMetadata {

  /** Escape JDBC metadata-pattern wildcards (`%`, `_`) before passing the value to
    * `DatabaseMetaData.getColumns` / `getTables`.
    *
    * The driver's search escape character is provided so the function works across drivers:
    *   - `null` → defaults to `\` (per JDBC spec when the driver returns `null` from
    *     `getSearchStringEscape`).
    *   - empty string → treated as "driver does NOT use a character-escape mechanism" (e.g. SQL
    *     Server, which uses bracket escaping like `[_]`). The pattern is returned unchanged so the
    *     caller can apply its own escape strategy. Forcing a backslash fallback here would inject
    *     literal `\` characters that bracket-escaping drivers do not recognize, producing
    *     wrong-pattern matches against `getColumns`.
    *   - any non-empty string → used as the escape character.
    *
    * Failing to escape literal `%` / `_` would let metadata calls unexpectedly match sibling tables
    * — e.g. `events_2024` matching `events_2024` AND `eventsX2024`.
    */
  def escapeMetadataPattern(pattern: String, escape: String): String = {
    if (escape == null) return escapeMetadataPattern(pattern, "\\")
    if (escape.isEmpty) return pattern
    val searchEscape = escape
    pattern
      .replace(searchEscape, s"${searchEscape}${searchEscape}")
      .replace("%", s"$searchEscape%")
      .replace("_", s"${searchEscape}_")
  }

  /** Resolved partition-column lookup against `connection.getMetaData.getColumns`.
    *
    * @param connection
    *   open JDBC connection; the caller owns its lifecycle.
    * @param catalog
    *   catalog name to scope the lookup (e.g. database name).
    * @param table
    *   raw (un-escaped) table name; will be wildcard-escaped before the JDBC call.
    * @param configuredColumn
    *   user-supplied partition column name (may be back-tick quoted).
    * @param normalizedColumn
    *   `configuredColumn` with backend-specific quoting stripped, used for the case-insensitive
    *   match against the returned column list.
    * @param onMissing
    *   produces the error to throw when the column is absent; the function passes the list of
    *   discovered column names so the message can list them.
    * @param classifier
    *   builds a [[JdbcPartitionBounds.PartitionColumnMetadata]] from the resolved
    *   `(columnName, jdbcType, jdbcTypeName)` triple, allowing each backend to plug in its
    *   `classifyPartitionColumnType` logic.
    */
  def resolvePartitionColumn(
    connection: Connection,
    catalog: String,
    table: String,
    configuredColumn: String,
    normalizedColumn: String,
    onMissing: List[String] => Nothing,
    classifier: (String, Int, String) => JdbcPartitionBounds.PartitionColumnMetadata
  ): JdbcPartitionBounds.PartitionColumnMetadata = {
    val tablePattern =
      escapeMetadataPattern(
        table,
        Option(connection.getMetaData.getSearchStringEscape).getOrElse("\\")
      )

    Using.resource(connection.getMetaData.getColumns(catalog, null, tablePattern, "%")) {
      resultSet =>
        val columns = Iterator
          .continually(resultSet.next())
          .takeWhile(identity)
          .map { _ =>
            (
              resultSet.getString("COLUMN_NAME"),
              resultSet.getInt("DATA_TYPE"),
              resultSet.getString("TYPE_NAME")
            )
          }
          .toList

        columns
          .find(_._1.equalsIgnoreCase(normalizedColumn))
          .map { case (columnName, jdbcType, jdbcTypeName) =>
            val _ = configuredColumn // retained for clarity at call sites
            classifier(columnName, jdbcType, jdbcTypeName)
          }
          .getOrElse(onMissing(columns.map(_._1)))
    }
  }
}
