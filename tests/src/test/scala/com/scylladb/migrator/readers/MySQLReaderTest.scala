package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{
  Credentials,
  HostValidation,
  SourceSettings,
  SparkSecretRedaction
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Locale

class MySQLReaderTest extends munit.FunSuite {

  private lazy val spark: SparkSession = {
    val sparkConf = new SparkConf(false)
    SparkSecretRedaction.ensureMigratorRedactionRegex(sparkConf)
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[1]")
      .appName("MySQLReaderTest")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

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

  test("escapeMetadataPattern escapes wildcard characters and the escape character itself") {
    assertEquals(
      MySQL.escapeMetadataPattern("""user_events_2024%backup\archive""", "\\"),
      """user\_events\_2024\%backup\\archive"""
    )
  }

  test("partitionedReadConsistencyWarning explains the lack of a global snapshot") {
    val warning = MySQL.partitionedReadConsistencyWarning(
      mysqlSource(database = "app", table = "user_events")
    )

    assert(warning.contains("app.user_events"))
    assert(warning.contains("do not provide a single global snapshot"))
    assert(warning.contains("Concurrent source writes"))
    assert(warning.contains("quiesce"))
  }

  // --- buildJdbcUrl tests ---

  private def mysqlSource(
    host: String = "localhost",
    port: Int = 3306,
    database: String = "mydb",
    table: String = "mytable",
    partitionColumn: Option[String] = None,
    numPartitions: Option[Int] = None,
    lowerBound: Option[String] = None,
    upperBound: Option[String] = None,
    zeroDateTimeBehavior: SourceSettings.MySQL.ZeroDateTimeBehavior =
      SourceSettings.MySQL.ZeroDateTimeBehavior.Exception,
    connectionProperties: Option[Map[String, String]] = None
  ): SourceSettings.MySQL =
    SourceSettings.MySQL(
      host                 = host,
      port                 = port,
      database             = database,
      table                = table,
      credentials          = Credentials("user", "pass"),
      primaryKey           = None,
      partitionColumn      = partitionColumn,
      numPartitions        = numPartitions,
      lowerBound           = lowerBound.map(SourceSettings.MySQL.PartitionBound(_)),
      upperBound           = upperBound.map(SourceSettings.MySQL.PartitionBound(_)),
      zeroDateTimeBehavior = zeroDateTimeBehavior,
      fetchSize            = 1000,
      where                = None,
      connectionProperties = connectionProperties
    )

  test("redactionRegexCoversKeys accepts Spark default redaction coverage") {
    assert(
      MySQL.redactionRegexCoversKeys(
        MySQL.DefaultSensitiveRedactionRegex,
        Seq("password", "trustStorePassword")
      )
    )
  }

  test("redactionRegexCoversKeys accepts apiKey and access key variants") {
    assert(
      MySQL.redactionRegexCoversKeys(
        MySQL.DefaultSensitiveRedactionRegex,
        Seq("apiKey", "access_key", "access-key", "privateKey")
      )
    )
  }

  test("redactionRegexCoversKeys rejects regex that misses password keys") {
    assert(
      !MySQL.redactionRegexCoversKeys("(?i)token", Seq("password")),
      "regex that only matches token must not be accepted for password-bearing options"
    )
  }

  test("isSensitiveOptionKey recognizes underscore and hyphen variants") {
    assert(MySQL.isSensitiveOptionKey("apiKey"))
    assert(MySQL.isSensitiveOptionKey("access_key"))
    assert(MySQL.isSensitiveOptionKey("access-key"))
    assert(MySQL.isSensitiveOptionKey("privateKey"))
  }

  test("ensureSensitiveReaderOptionsAreRedacted accepts default coverage for apiKey variants") {
    MySQL.ensureSensitiveReaderOptionsAreRedacted(
      spark,
      Seq("apiKey", "access_key", "access-key"),
      "test JDBC reader"
    )
  }

  test("buildJdbcUrl produces correct URL with defaults") {
    val url = MySQL.buildJdbcUrl(mysqlSource())
    assert(url.startsWith("jdbc:mysql://localhost:3306/mydb?"))
    assert(url.contains("zeroDateTimeBehavior=EXCEPTION"))
    assert(url.contains("tinyInt1IsBit=false"))
    assert(url.contains("useCursorFetch=true"))
    assert(url.contains(s"maxAllowedPacket=${MySQL.DefaultMaxAllowedPacketBytes}"))
    assert(url.contains(s"connectionTimeZone=${MySQL.DefaultConnectionTimeZoneId}"))
    assert(url.contains("forceConnectionTimeZoneToSession=true"))
  }

  test("buildJdbcUrl encodes named Spark time zones and forces them onto the MySQL session") {
    val url = MySQL.buildJdbcUrl(
      mysqlSource(),
      connectionTimeZoneId = "America/Los_Angeles"
    )
    assert(url.contains("connectionTimeZone=America%2FLos_Angeles"))
    assert(url.contains("forceConnectionTimeZoneToSession=true"))
  }

  test("buildJdbcUrl uses explicit zeroDateTimeBehavior override") {
    val url = MySQL.buildJdbcUrl(
      mysqlSource(
        zeroDateTimeBehavior = SourceSettings.MySQL.ZeroDateTimeBehavior.ConvertToNull
      )
    )
    assert(url.contains("zeroDateTimeBehavior=CONVERT_TO_NULL"))
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

  test("buildJdbcUrl rejects injected connectTimeout value") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(
          connectionProperties = Some(Map("connectTimeout" -> "5000&allowLoadLocalInfile=true"))
        )
      )
    }
  }

  test("buildJdbcUrl rejects injected socketTimeout value") {
    intercept[IllegalArgumentException] {
      MySQL.buildJdbcUrl(
        mysqlSource(
          connectionProperties = Some(Map("socketTimeout" -> "600000&allowMultiQueries=true"))
        )
      )
    }
  }

  test("buildJdbcUrl accepts zero connectTimeout and socketTimeout") {
    val url = MySQL.buildJdbcUrl(
      mysqlSource(
        connectionProperties = Some(
          Map(
            "connectTimeout" -> "0",
            "socketTimeout"  -> "0"
          )
        )
      )
    )
    assert(url.contains("connectTimeout=0"))
    assert(url.contains("socketTimeout=0"))
  }

  test("jdbcReadProperties carries credentials and fetchsize in JDBC properties") {
    val props = MySQL.jdbcReadProperties(mysqlSource())

    assertEquals(props.getProperty("user"), "user")
    assertEquals(props.getProperty("password"), "pass")
    assertEquals(props.getProperty("driver"), "com.mysql.cj.jdbc.Driver")
    assertEquals(props.getProperty("fetchsize"), "1000")
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

  test("readDataframe rejects unsafe partitionColumn from programmatic config") {
    val error = intercept[IllegalArgumentException] {
      MySQL.readDataframe(
        spark,
        mysqlSource(
          partitionColumn = Some("id; DROP TABLE users --"),
          numPartitions   = Some(4),
          lowerBound      = Some("1"),
          upperBound      = Some("100")
        )
      )
    }
    assert(error.getMessage.contains("partitionColumn"))
  }

  test("MySQL source dataframes do not support savepoints") {
    val sourceDF = MySQL.sourceDataFrame(spark.emptyDataFrame)

    assert(!sourceDF.savepointsSupported)
    assertEquals(sourceDF.timestampColumns, None)
  }

  test("readDataframe rejects partitioned reads without both bounds from programmatic config") {
    val error = intercept[IllegalArgumentException] {
      MySQL.readDataframe(
        spark,
        mysqlSource(
          partitionColumn = Some("id"),
          numPartitions   = Some(4),
          lowerBound      = Some("1"),
          upperBound      = None
        )
      )
    }
    assert(error.getMessage.contains("Both lowerBound and upperBound"))
  }

  test("safeConnectionProperties blocks dangerous keys under Turkish default locale") {
    val safeProps = withDefaultLocale(new Locale("tr", "TR")) {
      MySQL.safeConnectionProperties(
        mysqlSource(
          connectionProperties = Some(
            Map(
              "allowLoadLocalInfile" -> "true",
              "cachePrepStmts"       -> "true"
            )
          )
        )
      )
    }

    assertEquals(safeProps, Map("cachePrepStmts" -> "true"))
  }

  test("safeConnectionProperties filters JDBC keys already embedded in the URL") {
    val safeProps = MySQL.safeConnectionProperties(
      mysqlSource(
        connectionProperties = Some(
          Map(
            "zeroDateTimeBehavior"             -> "ROUND",
            "tinyInt1IsBit"                    -> "true",
            "maxAllowedPacket"                 -> "1024",
            "useCursorFetch"                   -> "false",
            "connectTimeout"                   -> "1",
            "socketTimeout"                    -> "2",
            "connectionTimeZone"               -> "LOCAL",
            "forceConnectionTimeZoneToSession" -> "false",
            "cachePrepStmts"                   -> "true"
          )
        )
      )
    )

    assertEquals(safeProps, Map("cachePrepStmts" -> "true"))
  }

  test("validatePartitionBoundsForColumnType accepts numeric bounds for numeric columns") {
    MySQL.validatePartitionBoundsForColumnType(
      partitionColumn = "id",
      jdbcTypeName    = "BIGINT",
      columnType      = MySQL.PartitionColumnType.Numeric,
      lowerBound      = SourceSettings.MySQL.PartitionBound("1"),
      upperBound      = SourceSettings.MySQL.PartitionBound("100"),
      timeZoneId      = "UTC"
    )
  }

  test("validatePartitionBoundsForColumnType accepts ISO date bounds for DATE columns") {
    MySQL.validatePartitionBoundsForColumnType(
      partitionColumn = "created_on",
      jdbcTypeName    = "DATE",
      columnType      = MySQL.PartitionColumnType.Date,
      lowerBound      = SourceSettings.MySQL.PartitionBound("2024-01-01"),
      upperBound      = SourceSettings.MySQL.PartitionBound("2024-02-01"),
      timeZoneId      = "UTC"
    )
  }

  test("validatePartitionBoundsForColumnType accepts timestamp bounds for TIMESTAMP columns") {
    MySQL.validatePartitionBoundsForColumnType(
      partitionColumn = "created_at",
      jdbcTypeName    = "TIMESTAMP",
      columnType      = MySQL.PartitionColumnType.Timestamp,
      lowerBound      = SourceSettings.MySQL.PartitionBound("2024-01-01 00:00:00"),
      upperBound      = SourceSettings.MySQL.PartitionBound("2024-01-02 00:00:00"),
      timeZoneId      = "UTC"
    )
  }

  test("validatePartitionBoundsForColumnType rejects epoch-millis bounds for DATE columns") {
    val error = intercept[RuntimeException] {
      MySQL.validatePartitionBoundsForColumnType(
        partitionColumn = "created_on",
        jdbcTypeName    = "DATE",
        columnType      = MySQL.PartitionColumnType.Date,
        lowerBound      = SourceSettings.MySQL.PartitionBound("1704067200000"),
        upperBound      = SourceSettings.MySQL.PartitionBound("1704153600000"),
        timeZoneId      = "UTC"
      )
    }
    assert(error.getMessage.contains("Epoch-millisecond bounds are not supported"))
  }

  test("validatePartitionBoundsForColumnType rejects epoch-millis bounds for TIMESTAMP columns") {
    val error = intercept[RuntimeException] {
      MySQL.validatePartitionBoundsForColumnType(
        partitionColumn = "created_at",
        jdbcTypeName    = "TIMESTAMP",
        columnType      = MySQL.PartitionColumnType.Timestamp,
        lowerBound      = SourceSettings.MySQL.PartitionBound("1704067200000"),
        upperBound      = SourceSettings.MySQL.PartitionBound("1704153600000"),
        timeZoneId      = "UTC"
      )
    }
    assert(error.getMessage.contains("Epoch-millisecond bounds are not supported"))
  }

  test("partitionedReadOptions uses the resolved metadata column name for Spark JDBC") {
    val options = MySQL
      .partitionedReadOptions(
        configuredPartitionColumn = "`1column`",
        partitionColumnInfo = MySQL.PartitionColumnMetadata(
          columnName   = "1column",
          jdbcTypeName = "BIGINT",
          columnType   = MySQL.PartitionColumnType.Numeric
        ),
        numPartitions = 4,
        lowerBound    = SourceSettings.MySQL.PartitionBound("1"),
        upperBound    = SourceSettings.MySQL.PartitionBound("100")
      )
      .toMap

    assertEquals(options("partitionColumn"), "1column")
    assertNotEquals(options("partitionColumn"), "`1column`")
    assertEquals(options("numPartitions"), "4")
    assertEquals(options("lowerBound"), "1")
    assertEquals(options("upperBound"), "100")
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

  // --- validateWhereClause tests ---

  test("validateWhereClause accepts blocked keywords inside string literals") {
    MySQL.validateWhereClause("event_type = 'delete' AND message LIKE '%union%'")
  }

  test("validateWhereClause accepts blocked keywords inside quoted identifiers") {
    MySQL.validateWhereClause("`delete` = 1 AND \"union\" = 'ok'")
  }

  test("validateWhereClause rejects dangerous keywords outside quotes") {
    val error = intercept[RuntimeException] {
      MySQL.validateWhereClause("id = 1 UNION SELECT 1")
    }
    assert(error.getMessage.contains("union"))
  }

  test("validateWhereClause rejects comment-obfuscated dangerous keywords") {
    val error = intercept[RuntimeException] {
      MySQL.validateWhereClause("id = 1 UNI/**/ON SELECT 1")
    }
    assert(error.getMessage.contains("union"))
  }

  test("validateWhereClause rejects MySQL executable comments") {
    val error = intercept[RuntimeException] {
      MySQL.validateWhereClause("id = 1 /*!50000 UNION SELECT 1 */")
    }
    assert(error.getMessage.contains("executable comments"))
  }

  test("validateWhereClause accepts MySQL executable comment marker inside string literals") {
    MySQL.validateWhereClause("message = '/*!50000 UNION SELECT 1 */'")
  }

  test("redactedWhereFilterLogMessage does not expose the filter contents") {
    val filter = "email = 'user@example.com' AND api_token = 'secret'"
    val message = MySQL.redactedWhereFilterLogMessage(filter)

    assert(message.contains("content redacted"))
    assert(message.contains(filter.length.toString))
    assert(!message.contains(filter))
  }

  private def withDefaultLocale[A](locale: Locale)(f: => A): A = {
    val original = Locale.getDefault
    Locale.setDefault(locale)
    try f
    finally Locale.setDefault(original)
  }
}
