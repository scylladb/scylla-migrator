package com.scylladb.migrator.readers.jdbc

class JdbcSafePropertiesTest extends munit.FunSuite {

  private val DangerousKeys = Set("allowloadlocalinfile", "autodeserialize")
  private val UrlEmbeddedKeys = Set("maxallowedpacket")

  test("classifyKey returns Reserved for Spark JDBCOptions keys, case-insensitively") {
    assertEquals(
      JdbcSafeProperties.classifyKey("DBTABLE", DangerousKeys, UrlEmbeddedKeys),
      JdbcSafeProperties.KeyClassification.Reserved
    )
  }

  test("classifyKey returns Dangerous for backend-supplied dangerous keys") {
    assertEquals(
      JdbcSafeProperties.classifyKey("allowLoadLocalInfile", DangerousKeys, UrlEmbeddedKeys),
      JdbcSafeProperties.KeyClassification.Dangerous
    )
  }

  test("classifyKey returns UrlEmbedded for backend-supplied url-embedded keys") {
    assertEquals(
      JdbcSafeProperties.classifyKey("maxAllowedPacket", DangerousKeys, UrlEmbeddedKeys),
      JdbcSafeProperties.KeyClassification.UrlEmbedded(None): JdbcSafeProperties.KeyClassification
    )
  }

  test("classifyKey attaches backend-supplied guidance to UrlEmbedded matches case-insensitively") {
    val guidance = Map(
      "maxAllowedPacket" -> "use source.maxAllowedPacket instead of connectionProperties"
    )
    assertEquals(
      JdbcSafeProperties
        .classifyKey("MAXALLOWEDPACKET", DangerousKeys, UrlEmbeddedKeys, guidance),
      JdbcSafeProperties.KeyClassification
        .UrlEmbedded(
          Some("use source.maxAllowedPacket instead of connectionProperties")
        ): JdbcSafeProperties.KeyClassification
    )
  }

  test("classifyKey returns UrlEmbedded(None) when no guidance map entry matches") {
    val guidance = Map("zeroDateTimeBehavior" -> "use source.zeroDateTimeBehavior")
    assertEquals(
      JdbcSafeProperties
        .classifyKey("maxAllowedPacket", DangerousKeys, UrlEmbeddedKeys, guidance),
      JdbcSafeProperties.KeyClassification.UrlEmbedded(None): JdbcSafeProperties.KeyClassification
    )
  }

  test("classifyKey returns Allowed for arbitrary user-supplied keys") {
    assertEquals(
      JdbcSafeProperties.classifyKey("cachePrepStmts", DangerousKeys, UrlEmbeddedKeys),
      JdbcSafeProperties.KeyClassification.Allowed
    )
  }

  test("classifyKey prefers Reserved over Dangerous when both would match") {
    val overlapping = JdbcSafeProperties.SparkReservedJdbcKeys.head
    assertEquals(
      JdbcSafeProperties.classifyKey(overlapping, Set(overlapping), Set(overlapping)),
      JdbcSafeProperties.KeyClassification.Reserved
    )
  }

  test("classifyKey closes Turkish-İ blocklist bypass (U+0130 substitution)") {
    // Regression: previously, `key.toLowerCase(Locale.ROOT)` expanded U+0130 (`İ`) into
    // `"i\u0307"`, so an attacker-supplied `allowLoadLocal\u0130nfile` would normalize to a
    // 16-character string that did NOT appear in DangerousKeys, while a JDBC driver doing
    // per-character `equalsIgnoreCase` against `"allowLoadLocalInfile"` would still accept the
    // crafted key. The fix switches matching to `equalsIgnoreCase` on the raw key.
    val crafted = "allowLoadLocal\u0130nfile"
    assertEquals(
      JdbcSafeProperties.classifyKey(crafted, DangerousKeys, UrlEmbeddedKeys),
      JdbcSafeProperties.KeyClassification.Dangerous
    )
  }

  test("insecureUrlScheme detects file:// and http:// regardless of casing and whitespace") {
    assertEquals(JdbcSafeProperties.insecureUrlScheme("file:///etc/passwd"), Some("file"))
    assertEquals(JdbcSafeProperties.insecureUrlScheme(" HTTP://attacker"), Some("http"))
    assertEquals(JdbcSafeProperties.insecureUrlScheme("https://safe"), None)
    assertEquals(JdbcSafeProperties.insecureUrlScheme("/local/path"), None)
  }

  test("insecureUrlScheme detects expanded file URI and jar:file: variants") {
    // Regression: prior `startsWith("file://")` missed RFC-3986 file URIs that use a single
    // slash or a Windows drive letter, as well as nested jar:file: URLs that JDK URL handlers
    // will resolve through the file: scheme.
    assertEquals(JdbcSafeProperties.insecureUrlScheme("file:/etc/passwd"), Some("file"))
    assertEquals(JdbcSafeProperties.insecureUrlScheme("file:C:/keystore.jks"), Some("file"))
    assertEquals(
      JdbcSafeProperties.insecureUrlScheme("jar:file:///opt/app.jar!/k.jks"),
      Some("file")
    )
    assertEquals(JdbcSafeProperties.insecureUrlScheme("\\\\fileserver\\share\\k.jks"), Some("file"))
  }

  test("insecureUrlScheme detects jar:http:// as insecure") {
    assertEquals(
      JdbcSafeProperties.insecureUrlScheme("jar:http://attacker.com/evil.jar!/keystore.jks"),
      Some("http")
    )
    assertEquals(
      JdbcSafeProperties.insecureUrlScheme("JAR:HTTP://attacker.com/evil.jar!/keystore.jks"),
      Some("http")
    )
  }

  test("looksLikeTlsKeystoreUrlKey targets *keystore*url* and *cert*url* keys") {
    assert(JdbcSafeProperties.looksLikeTlsKeystoreUrlKey("trustCertificateKeyStoreUrl"))
    assert(JdbcSafeProperties.looksLikeTlsKeystoreUrlKey("clientCertificateKeyStoreUrl"))
    assert(!JdbcSafeProperties.looksLikeTlsKeystoreUrlKey("trustCertificateKeyStorePassword"))
    assert(!JdbcSafeProperties.looksLikeTlsKeystoreUrlKey("cachePrepStmts"))
  }
}
