package com.scylladb.migrator.config

/** Shared validation utilities for validating hostnames and IP addresses. Extracted from
  * [[com.scylladb.migrator.readers.MySQL]] and [[com.scylladb.migrator.config.SourceSettings]] to
  * avoid duplication.
  */
object HostValidation {
  private val UrlMetaCharacters = Set('/', '?', '#', '&', '@')

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

  def validatePort(label: String, port: Int): List[String] =
    if (port < 1 || port > 65535)
      List(s"$label port must be between 1 and 65535, got: $port")
    else Nil

  def validateHostOrIp(label: String, host: String): List[String] = {
    val trimmed = host.trim
    if (trimmed.isEmpty)
      List(s"$label host must not be empty")
    else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      val inner = trimmed.slice(1, trimmed.length - 1)
      if (!inner.contains(':'))
        List(s"$label IPv4 addresses must not be wrapped in brackets. Use '$inner' instead.")
      else if (isValidIPv6Host(trimmed)) Nil
      else List(invalidHostMessage(label, trimmed))
    } else if (isValidHostname(trimmed) || isValidIPv6Host(trimmed)) Nil
    else List(invalidHostMessage(label, trimmed))
  }

  def validateEndpoint(label: String, host: String, port: Int): List[String] =
    validateEndpointHost(label, host) ++ validatePort(label, port)

  def renderEndpointHost(host: String): String = {
    val trimmed = host.trim.stripSuffix("/")
    splitHttpScheme(trimmed) match {
      case Some((scheme, authority)) => s"$scheme${bracketBareIPv6(authority)}"
      case None                      => s"http://${bracketBareIPv6(trimmed)}"
    }
  }

  private def splitHttpScheme(host: String): Option[(String, String)] = {
    val lower = host.toLowerCase(java.util.Locale.ROOT)
    if (lower.startsWith("http://"))
      Some(host.take("http://".length) -> host.drop("http://".length))
    else if (lower.startsWith("https://"))
      Some(host.take("https://".length) -> host.drop("https://".length))
    else None
  }

  private def bracketBareIPv6(host: String): String =
    if (host.contains(':') && !host.startsWith("[") && isValidIPv6Host(host)) s"[$host]"
    else host

  private def validateEndpointHost(label: String, host: String): List[String] = {
    val trimmed = host.trim
    if (trimmed.isEmpty)
      return List(s"$label endpoint host must not be empty")

    val (authority, hadScheme) = splitHttpScheme(trimmed) match {
      case Some((_, remainder)) => remainder.stripSuffix("/") -> true
      case None                 => trimmed.stripSuffix("/")   -> false
    }

    if (
      authority.isEmpty ||
      authority.exists(UrlMetaCharacters.contains)
    )
      return List(
        s"$label endpoint host must be a bare hostname/IP, or an http(s) URL without userinfo, path, query, or fragment"
      )

    if (authority.startsWith("[") && authority.endsWith("]")) {
      if (isValidIPv6Host(authority)) Nil
      else List(invalidEndpointHostMessage(label, host))
    } else if (authority.contains(':')) {
      if (!hadScheme && isValidIPv6Host(authority)) Nil
      else
        List(
          s"$label endpoint host must not include a port because 'port' is configured separately"
        )
    } else if (isValidHostname(authority)) Nil
    else List(invalidEndpointHostMessage(label, host))
  }

  private def invalidHostMessage(label: String, host: String): String = {
    val rejectedHost =
      if (host.exists(UrlMetaCharacters.contains)) ""
      else s" '$host'"

    s"Invalid $label host$rejectedHost: must be a hostname, IPv4, or IPv6 address. " +
      "URL metacharacters (/, ?, #, &, @) are not allowed."
  }

  private def invalidEndpointHostMessage(label: String, host: String): String =
    s"Invalid $label endpoint host '$host': must be a hostname, IPv4, or IPv6 address, " +
      "optionally prefixed with http:// or https://. URL metacharacters (/, ?, #, &, @) are not allowed."
}
