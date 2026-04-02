package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ Credentials, HostValidation, SourceSettings }

class MySQLReaderTest extends munit.FunSuite {

  // --- escapeIdentifier tests ---

  test("escapeIdentifier wraps name in backticks") {
    assertEquals(MySQL.escapeIdentifier("users"), "`users`")
  }

  test("escapeIdentifier doubles embedded backticks") {
    assertEquals(MySQL.escapeIdentifier("foo`bar"), "`foo``bar`")
  }

  test("escapeIdentifier handles multiple embedded backticks") {
    assertEquals(MySQL.escapeIdentifier("a`b`c"), "`a``b``c`")
  }

  test("escapeIdentifier handles name that is only a backtick") {
    assertEquals(MySQL.escapeIdentifier("`"), "````")
  }

  test("escapeIdentifier rejects empty name") {
    intercept[IllegalArgumentException] {
      MySQL.escapeIdentifier("")
    }
  }

  // --- buildJdbcUrl tests ---

  private def mysqlSource(
    host: String = "localhost",
    port: Int = 3306,
    database: String = "mydb",
    table: String = "mytable",
    connectionProperties: Option[Map[String, String]] = None
  ): SourceSettings.MySQL =
    SourceSettings.MySQL(
      host                 = host,
      port                 = port,
      database             = database,
      table                = table,
      credentials          = Credentials("user", "pass"),
      primaryKey           = None,
      partitionColumn      = None,
      numPartitions        = None,
      lowerBound           = None,
      upperBound           = None,
      fetchSize            = 1000,
      where                = None,
      connectionProperties = connectionProperties
    )

  test("buildJdbcUrl produces correct URL with defaults") {
    val url = MySQL.buildJdbcUrl(mysqlSource())
    assert(url.startsWith("jdbc:mysql://localhost:3306/mydb?"))
    assert(url.contains("zeroDateTimeBehavior=CONVERT_TO_NULL"))
    assert(url.contains("tinyInt1IsBit=false"))
    assert(url.contains("useCursorFetch=true"))
    assert(url.contains(s"maxAllowedPacket=${MySQL.DefaultMaxAllowedPacketBytes}"))
  }

  test("buildJdbcUrl uses custom maxAllowedPacket from connectionProperties") {
    val url = MySQL.buildJdbcUrl(
      mysqlSource(connectionProperties = Some(Map("maxAllowedPacket" -> "1024")))
    )
    assert(url.contains("maxAllowedPacket=1024"))
    assert(!url.contains(MySQL.DefaultMaxAllowedPacketBytes.toString))
  }

  test("buildJdbcUrl uses maxAllowedPacket case-insensitively") {
    val url = MySQL.buildJdbcUrl(
      mysqlSource(connectionProperties = Some(Map("maxallowedpacket" -> "2048")))
    )
    assert(url.contains("maxAllowedPacket=2048"))
    assert(!url.contains(MySQL.DefaultMaxAllowedPacketBytes.toString))
  }

  test("buildJdbcUrl rejects non-numeric maxAllowedPacket") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(connectionProperties = Some(Map("maxAllowedPacket" -> "notanumber")))
      )
    }
  }

  test("buildJdbcUrl rejects empty maxAllowedPacket") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(connectionProperties = Some(Map("maxAllowedPacket" -> "")))
      )
    }
  }

  test("buildJdbcUrl rejects zero maxAllowedPacket") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(connectionProperties = Some(Map("maxAllowedPacket" -> "0")))
      )
    }
  }

  test("buildJdbcUrl rejects empty host") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = ""))
    }
  }

  test("buildJdbcUrl rejects host with URL injection characters") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(host = "evil.com:3306/fakedb?allowLoadLocalInfile=true&user=root#")
      )
    }
  }

  test("buildJdbcUrl rejects host with fragment injection") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "evil.com#"))
    }
  }

  test("buildJdbcUrl rejects host with query string injection") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "evil.com?foo=bar"))
    }
  }

  test("buildJdbcUrl accepts IPv6 host and wraps in brackets") {
    val url = MySQL.buildJdbcUrl(mysqlSource(host = "::1"))
    assert(url.contains("jdbc:mysql://[::1]:3306/mydb"))
  }

  test("buildJdbcUrl accepts already-bracketed IPv6 host") {
    val url = MySQL.buildJdbcUrl(mysqlSource(host = "[::1]"))
    assert(url.contains("jdbc:mysql://[::1]:3306/mydb"))
  }

  test("buildJdbcUrl accepts IPv4-mapped IPv6 host") {
    val url = MySQL.buildJdbcUrl(mysqlSource(host = "::ffff:192.168.1.1"))
    assert(url.contains("jdbc:mysql://[::ffff:192.168.1.1]:3306/mydb"))
  }

  test("buildJdbcUrl accepts bracketed IPv4-mapped IPv6 host") {
    val url = MySQL.buildJdbcUrl(mysqlSource(host = "[::ffff:10.0.0.1]"))
    assert(url.contains("jdbc:mysql://[::ffff:10.0.0.1]:3306/mydb"))
  }

  test("buildJdbcUrl rejects unbalanced opening bracket") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "[::1"))
    }
  }

  test("buildJdbcUrl rejects unbalanced closing bracket") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "::1]"))
    }
  }

  test("buildJdbcUrl rejects invalid database name") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(database = "bad;name"))
    }
  }

  test("buildJdbcUrl accepts database with dollar sign") {
    val url = MySQL.buildJdbcUrl(mysqlSource(database = "my$db"))
    assert(url.contains("/my$db?"))
  }

  test("buildJdbcUrl accepts database with hyphen") {
    val url = MySQL.buildJdbcUrl(mysqlSource(database = "my-database"))
    assert(url.contains("/my-database?"))
  }

  test("buildJdbcUrl includes custom port") {
    val url = MySQL.buildJdbcUrl(mysqlSource(port = 3307))
    assert(url.contains(":3307/"))
  }

  test("buildJdbcUrl rejects colon-only host like ::::") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "::::"))
    }
  }

  test("buildJdbcUrl rejects dot-only host like ...") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(mysqlSource(host = "..."))
    }
  }

  // --- isValidHostname tests ---

  test("isValidHostname accepts regular hostname") {
    assert(HostValidation.isValidHostname("db.example.com"))
  }

  test("isValidHostname accepts IPv4 address") {
    assert(HostValidation.isValidHostname("192.168.1.1"))
  }

  test("isValidHostname accepts localhost") {
    assert(HostValidation.isValidHostname("localhost"))
  }

  test("isValidHostname rejects dot-only string") {
    assert(!HostValidation.isValidHostname("..."))
  }

  test("isValidHostname rejects hyphen-only string") {
    assert(!HostValidation.isValidHostname("-foo"))
  }

  test("isValidHostname rejects empty string") {
    assert(!HostValidation.isValidHostname(""))
  }

  // --- isValidIPv6Host tests ---

  test("isValidIPv6Host accepts ::1") {
    assert(HostValidation.isValidIPv6Host("::1"))
  }

  test("isValidIPv6Host accepts bracketed ::1") {
    assert(HostValidation.isValidIPv6Host("[::1]"))
  }

  test("isValidIPv6Host accepts full IPv6 address") {
    assert(HostValidation.isValidIPv6Host("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
  }

  test("isValidIPv6Host accepts IPv4-mapped IPv6") {
    assert(HostValidation.isValidIPv6Host("::ffff:192.168.1.1"))
  }

  test("isValidIPv6Host rejects colon-only string ::::") {
    assert(!HostValidation.isValidIPv6Host("::::"))
  }

  test("isValidIPv6Host rejects dot-only string ...") {
    assert(!HostValidation.isValidIPv6Host("..."))
  }

  test("isValidIPv6Host rejects empty string") {
    assert(!HostValidation.isValidIPv6Host(""))
  }

  test("isValidIPv6Host rejects empty brackets") {
    assert(!HostValidation.isValidIPv6Host("[]"))
  }

  test("isValidIPv6Host rejects structurally invalid 9-group address") {
    assert(!HostValidation.isValidIPv6Host("1:2:3:4:5:6:7:8:9"))
  }
}
