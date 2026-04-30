package com.scylladb.migrator.config

/** Shared validation utilities for validating hostnames and IP addresses. Extracted from
  * [[com.scylladb.migrator.readers.MySQL]] and [[com.scylladb.migrator.config.SourceSettings]] to
  * avoid duplication.
  */
object HostValidation {

  /** Matches a hostname or IPv4 address: alphanumeric segments separated by dots or hyphens. Must
    * start with an alphanumeric character to avoid matching dot-only or hyphen-only strings.
    * Underscores are intentionally allowed for Docker/internal hostnames despite being technically
    * invalid per RFC 952/1123.
    *
    * Note: this regex is deliberately permissive. Its primary purpose is to reject URL
    * metacharacters (/, ?, #, &, @) that could break JDBC URL construction, not to enforce strict
    * RFC 952/1123 hostname rules. Strings like "host..name" or "192.168.1." will pass but will be
    * rejected by MySQL at connection time.
    */
  def isValidHostname(host: String): Boolean =
    host.matches("""[a-zA-Z0-9][a-zA-Z0-9_.\-]*""")

  /** Matches an IPv6 address, optionally wrapped in square brackets. Uses
    * [[java.net.InetAddress.getByName]] for structural validation after a character-class
    * pre-filter. The pre-filter ensures only hex digits, colons, and dots (for IPv4-mapped
    * addresses) are present, which prevents `getByName` from performing DNS lookups.
    */
  def isValidIPv6Host(host: String): Boolean = {
    val inner =
      if (host.startsWith("[") && host.endsWith("]")) host.slice(1, host.length - 1)
      else host
    // Pre-filter: only allow characters that can appear in IPv6 addresses.
    // This prevents InetAddress.getByName from triggering DNS lookups on hostnames.
    if (
      inner.isEmpty ||
      !inner.matches("""[0-9a-fA-F:.]+""") ||
      !inner.contains(':') ||
      !inner.exists(c => c.isDigit || ('a' <= c.toLower && c.toLower <= 'f'))
    )
      return false
    try {
      val addr = java.net.InetAddress.getByName(inner)
      // InetAddress.getByName parses IPv4-mapped IPv6 addresses (e.g., ::ffff:192.168.1.1) as
      // Inet4Address. Since our pre-filter already confirmed the input contains a colon and
      // only IPv6-legal characters, accept both Inet6Address and Inet4Address here.
      addr.isInstanceOf[java.net.Inet6Address] || addr.isInstanceOf[java.net.Inet4Address]
    } catch {
      case _: Exception => false
    }
  }
}
