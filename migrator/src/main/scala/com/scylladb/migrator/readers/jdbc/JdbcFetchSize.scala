package com.scylladb.migrator.readers.jdbc

/** JDBC fetch-size bounds and warning text shared by JDBC source readers.
  *
  * `MaxFetchSize` is an upper bound that callers should reject at config-validation time.
  * `RecommendedMaxFetchSize` is a soft ceiling used to emit a runtime warning, since "safe" upper
  * bounds depend on per-row size and JVM heap, which the migrator cannot inspect in advance.
  *
  * '''Provenance''': the current numeric ceilings are calibrated against MySQL Connector/J's
  * `useCursorFetch=true` server-side cursor memory behavior. They are conservative for most JDBC
  * drivers but each future backend (PostgreSQL, Oracle, SQL Server) should validate the ceiling
  * against its driver's fetch semantics before adopting these constants verbatim — PostgreSQL, for
  * instance, requires `autoCommit=false` for server-side cursors to take effect, and Oracle's
  * prefetch model has different memory characteristics. A `FetchSizePolicy` case class will be
  * introduced when the second JDBC backend is added; until then, treat these constants as
  * MySQL-tuned defaults.
  */
object JdbcFetchSize {

  /** Hard upper bound. Values above risk OOM in the JDBC driver or Spark executors. */
  val MaxFetchSize: Int = 100000

  /** Soft upper bound. Above this, callers should warn that large rows (TEXT/BLOB) may exhaust the
    * per-connection memory budget.
    */
  val RecommendedMaxFetchSize: Int = 10000

  /** Build the runtime warning message emitted when `fetchSize > RecommendedMaxFetchSize`. */
  def aboveRecommendedWarning(fetchSize: Int): String =
    s"fetchSize ($fetchSize) exceeds the recommended maximum of $RecommendedMaxFetchSize. " +
      "For tables with large rows (TEXT/BLOB columns) this may cause excessive memory usage " +
      "per JDBC connection. Consider lowering fetchSize to the 1000-10000 range."
}
