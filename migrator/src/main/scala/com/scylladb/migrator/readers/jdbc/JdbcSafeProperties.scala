package com.scylladb.migrator.readers.jdbc

import java.util.Locale

/** Backend-neutral primitives for filtering user-supplied JDBC connection properties.
  *
  * Each backend supplies its own driver-specific `dangerousKeys` and `urlEmbeddedKeys` sets and
  * composes them with the shared [[SparkReservedJdbcKeys]] constant. Backends keep their own
  * warning messages so log text does not regress when this skeleton is introduced.
  */
object JdbcSafeProperties {

  /** Spark's `JDBCOptions` reserved keys. Spark uses these option names internally on the JDBC
    * DataSource; user-supplied entries with these names would silently override migrator-managed
    * behavior. Kept aligned with Apache Spark `org.apache.spark.sql.execution.datasources.jdbc`.
    */
  val SparkReservedJdbcKeys: Set[String] = Set(
    "url",
    "dbtable",
    "query",
    "driver",
    "partitioncolumn",
    "lowerbound",
    "upperbound",
    "numpartitions",
    "querytimeout",
    "fetchsize",
    "truncate",
    "cascadetruncate",
    "createtableoptions",
    "createtablecolumntypes",
    "customschema",
    "batchsize",
    "isolationlevel",
    "sessioninitstatement",
    "pushdownpredicate",
    "pushdownaggregate",
    "pushdownlimit",
    "pushdownoffset",
    "pushdowntablesample",
    "keytab",
    "principal",
    "tablecomment",
    "refreshkrb5config",
    "connectionprovider",
    "preparequery",
    "prefertimestampntz",
    "hint",
    "user",
    "password"
  )

  /** Result of classifying a single user property key against the three blocklists. */
  sealed trait KeyClassification
  object KeyClassification {

    /** Key is safe to pass through to the JDBC driver. */
    case object Allowed extends KeyClassification

    /** Key collides with a Spark-reserved JDBCOptions name. */
    case object Reserved extends KeyClassification

    /** Key is on the backend-specific dangerous list (e.g. local-file-read attack vectors). */
    case object Dangerous extends KeyClassification

    /** Key is already embedded in the JDBC URL and would be ignored by the driver.
      *
      * `customGuidance`, when non-empty, replaces the generic "already set in the JDBC URL" tail of
      * the operator-facing warning. Backends use it to redirect the user toward a higher-level
      * config field (for example: "use source.zeroDateTimeBehavior instead of
      * connectionProperties"). Keeping this string as data here — rather than as a per-backend
      * `if`/`when` branch on the caller — means each backend declares its dialect-specific
      * messaging in one declarative map rather than scattering casing assumptions across the
      * delegation layer.
      */
    final case class UrlEmbedded(customGuidance: Option[String]) extends KeyClassification
  }

  /** Classify a key against the reserved/dangerous/url-embedded sets.
    *
    * Order: Reserved > Dangerous > UrlEmbedded > Allowed (matches the legacy MySQL filter).
    *
    * The check is case-insensitive via per-character `String.equalsIgnoreCase` rather than via
    * `toLowerCase(Locale.ROOT)`-then-`Set.contains`. The two strategies disagree on Unicode
    * special-casing: `"İ".toLowerCase(Locale.ROOT)` returns `"i\u0307"` (2 chars), so a crafted key
    * like `allowLoadLocal\u0130nfile` would have been normalized to a 16-char string that does not
    * appear in the blocklist, while JDBC drivers (notably MySQL Connector/J) compare property names
    * with per-character `equalsIgnoreCase` and would still accept the key. Per- character matching
    * closes that bypass.
    */
  def classifyKey(
    key: String,
    dangerousKeys: Set[String],
    urlEmbeddedKeys: Set[String],
    urlEmbeddedGuidance: Map[String, String] = Map.empty
  ): KeyClassification =
    if (containsCaseInsensitive(SparkReservedJdbcKeys, key)) KeyClassification.Reserved
    else if (containsCaseInsensitive(dangerousKeys, key)) KeyClassification.Dangerous
    else if (containsCaseInsensitive(urlEmbeddedKeys, key))
      KeyClassification.UrlEmbedded(lookupCaseInsensitive(urlEmbeddedGuidance, key))
    else KeyClassification.Allowed

  private def containsCaseInsensitive(keys: Set[String], key: String): Boolean =
    keys.exists(_.equalsIgnoreCase(key))

  private def lookupCaseInsensitive(map: Map[String, String], key: String): Option[String] =
    map.collectFirst { case (k, v) if k.equalsIgnoreCase(key) => v }

  /** Detect insecure URL schemes commonly supplied to TLS keystore properties. Returns the scheme
    * name on a match, `None` otherwise.
    *
    * Covered variants:
    *   - any `file:`-prefixed URI (`file://...`, `file:/etc/passwd`, `file:C:/...`),
    *   - `jar:file:...` nesting,
    *   - Windows UNC paths (`\\server\share`) — flagged as `"file"` because they resolve via the
    *     local filesystem,
    *   - plain `http://...`,
    *   - `jar:http://...` nesting — a JAR URL over plain HTTP is still insecure for keystore
    *     material because the JDK fetches the outer archive over an unencrypted channel.
    */
  def insecureUrlScheme(value: String): Option[String] = {
    val normalized = value.trim.toLowerCase(Locale.ROOT)
    if (
      normalized.startsWith("file:") ||
      normalized.startsWith("jar:file:") ||
      normalized.startsWith("\\\\")
    ) Some("file")
    else if (
      normalized.startsWith("http://") ||
      normalized.startsWith("jar:http://")
    ) Some("http")
    else None
  }

  /** Heuristic to decide whether a key likely points at a TLS keystore URL (so the caller can run
    * [[insecureUrlScheme]] against its value).
    */
  def looksLikeTlsKeystoreUrlKey(key: String): Boolean = {
    val lower = key.toLowerCase(Locale.ROOT)
    (lower.contains("keystore") || lower.contains("cert")) && lower.contains("url")
  }
}
