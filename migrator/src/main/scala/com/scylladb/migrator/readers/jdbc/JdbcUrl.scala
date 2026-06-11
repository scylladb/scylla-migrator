package com.scylladb.migrator.readers.jdbc

import com.scylladb.migrator.config.HostValidation

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

/** Backend-neutral primitives for assembling JDBC URLs safely.
  *
  * Each function intentionally encapsulates a single check so callers can compose them with their
  * backend-specific URL scheme, query parameters, and identifier quoting rules.
  */
object JdbcUrl {

  /** Database-name guard preventing URL metacharacters from leaking into the JDBC URL path segment.
    * Matches the original `[a-zA-Z0-9_$\-]+` regex used by the MySQL reader.
    */
  val SimpleDatabaseNamePattern: String = "[a-zA-Z0-9_$\\-]+"

  /** Reject a database name that is not a simple identifier. Throws `IllegalArgumentException` with
    * an explanatory message, matching the legacy MySQL reader contract.
    */
  def requireSimpleDatabaseName(name: String): Unit =
    require(
      name.matches(SimpleDatabaseNamePattern),
      s"Invalid database name '$name'. " +
        "Must contain only alphanumeric characters, underscores, dollar signs, or hyphens. " +
        "URL-significant characters (/, ?, #, &) are not allowed."
    )

  /** Reject a host that is empty or contains URL metacharacters. Accepts hostnames, IPv4, and IPv6
    * (bracketed or bare), delegating syntactic checks to
    * [[com.scylladb.migrator.config.HostValidation]].
    */
  def requireValidHost(host: String): Unit = {
    require(host.nonEmpty, "host must not be empty")
    require(
      HostValidation.isValidHostname(host) || HostValidation.isValidIPv6Host(host),
      s"Invalid host '$host': must be a hostname, IPv4, or IPv6 address. " +
        "URL metacharacters (/, ?, #, &, @) are not allowed."
    )
  }

  /** Bracket a bare IPv6 host so it is safe to embed before the `:port` separator in a JDBC URL.
    * Already-bracketed and non-IPv6 hosts pass through unchanged. The decision is structural
    * (presence of `:`), which is sufficient because [[requireValidHost]] runs first.
    */
  def bracketIPv6Host(host: String): String =
    if (host.contains(':') && !host.startsWith("[")) s"[$host]"
    else host

  /** URL-encode a single value using UTF-8, matching `java.net.URLEncoder` semantics. */
  def urlEncode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8)

  /** Assemble an ordered `key=value&...` query string with both keys and values URL-encoded. */
  def encodeQueryParams(params: Seq[(String, String)]): String =
    params
      .map { case (k, v) => s"${urlEncode(k)}=${urlEncode(v)}" }
      .mkString("&")
}
