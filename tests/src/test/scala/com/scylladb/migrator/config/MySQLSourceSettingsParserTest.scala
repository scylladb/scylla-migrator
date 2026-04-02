package com.scylladb.migrator.config

import io.circe.yaml
import java.nio.file.Files
import java.nio.charset.StandardCharsets

class MySQLSourceSettingsParserTest extends munit.FunSuite {

  test("valid MySQL source config with all fields") {
    val config =
      """type: mysql
        |host: db.example.com
        |port: 3306
        |database: mydb
        |table: users
        |credentials:
        |  username: admin
        |  password: secret
        |primaryKey:
        |  - id
        |fetchSize: 500
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.host, "db.example.com")
    assertEquals(result.port, 3306)
    assertEquals(result.database, "mydb")
    assertEquals(result.table, "users")
    assertEquals(result.credentials.username, "admin")
    assertEquals(result.credentials.password, "secret")
    assertEquals(result.primaryKey, Some(List("id")))
    assertEquals(result.fetchSize, 500)
    assertEquals(result.where, None)
    assertEquals(result.connectionProperties, None)
    assertEquals(result.partitionColumn, None)
    assertEquals(result.numPartitions, None)
    assertEquals(result.lowerBound, None)
    assertEquals(result.upperBound, None)
  }

  test("valid MySQL source config with optional fields") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3307
        |database: testdb
        |table: orders
        |credentials:
        |  username: root
        |  password: "rootpass"
        |fetchSize: 1000
        |where: "status = 'active'"
        |partitionColumn: id
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 10000
        |connectionProperties:
        |  connectTimeout: "5000"
        |  socketTimeout: "30000"
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.host, "localhost")
    assertEquals(result.port, 3307)
    assertEquals(result.where, Some("status = 'active'"))
    assertEquals(result.partitionColumn, Some("id"))
    assertEquals(result.numPartitions, Some(4))
    assertEquals(result.lowerBound, Some(0L))
    assertEquals(result.upperBound, Some(10000L))
    assertEquals(
      result.connectionProperties,
      Some(Map("connectTimeout" -> "5000", "socketTimeout" -> "30000"))
    )
  }

  test("MySQL source config without optional primaryKey") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.primaryKey, None)
  }

  test("MySQL source config missing required credentials fails") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |fetchSize: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail without credentials")
  }

  test("partitionColumn without numPartitions fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when partitionColumn is set without numPartitions")
  }

  test("numPartitions without partitionColumn fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |numPartitions: 4
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when numPartitions is set without partitionColumn")
  }

  test("partitioned read without bounds fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |numPartitions: 4
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when partitioned read is configured without bounds")
  }

  test("lowerBound >= upperBound fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |numPartitions: 4
        |lowerBound: 100
        |upperBound: 50
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when lowerBound >= upperBound")
  }

  test("lowerBound == upperBound fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |numPartitions: 4
        |lowerBound: 100
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when lowerBound == upperBound")
  }

  test("port 0 fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 0
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when port is 0")
  }

  test("port 70000 fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 70000
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when port is out of range")
  }

  test("fetchSize 0 fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 0
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when fetchSize is 0")
  }

  test("negative fetchSize fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: -1
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when fetchSize is negative")
  }

  test("numPartitions 0 fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |numPartitions: 0
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when numPartitions is 0")
  }

  test("negative numPartitions fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: id
        |numPartitions: -1
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when numPartitions is negative")
  }

  test("partitionColumn with SQL injection characters fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: "id; DROP TABLE users --"
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when partitionColumn contains invalid characters")
  }

  test("backtick-quoted partitionColumn with embedded newline fails at parse time") {
    // A newline inside a backtick-quoted identifier is syntactically invalid in MySQL and
    // could be used to smuggle a second SQL statement into the JDBC partition column path.
    // The YAML \n escape in a double-quoted scalar produces a real newline character.
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: "`col\nAND 1=1`"
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when backtick-quoted partitionColumn contains a newline")
  }

  test("partitionColumn with valid identifier passes") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: user_id
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.partitionColumn, Some("user_id"))
  }

  test("partitionColumn with backtick-quoted identifier passes") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: "`1column`"
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.partitionColumn, Some("`1column`"))
  }

  test("empty backtick-quoted partitionColumn fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100
        |partitionColumn: "``"
        |numPartitions: 4
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when backtick-quoted partitionColumn is empty")
  }

  test("fetchSize defaults to 1000 when omitted") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.fetchSize, 1000)
  }

  test("empty host fails at parse time") {
    val config =
      """type: mysql
        |host: ""
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when host is empty")
  }

  test("empty database fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: ""
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when database is empty")
  }

  test("empty table fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: ""
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when table is empty")
  }

  test("table with control characters (newline) fails at parse time") {
    val parsed = yaml.parser
      .parse(
        "type: mysql\nhost: localhost\nport: 3306\ndatabase: mydb\ntable: \"bad\\ntable\"\n" +
          "credentials:\n  username: user\n  password: pass\n"
      )
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when table name contains control characters")
  }

  test("table with invalid characters (semicolon) fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: "bad;table"
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when table name contains semicolon")
  }

  test("table name with dollar sign and underscore passes") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: my_table$1
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.table, "my_table$1")
  }

  test("empty username fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: ""
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when username is empty")
  }

  test("empty password fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: ""
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when password is empty")
  }

  test("host with invalid characters fails at parse time") {
    val config =
      """type: mysql
        |host: "db.com/evil"
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when host contains URL metacharacters")
  }

  test("database with invalid characters fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: "my/db"
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when database contains invalid characters")
  }

  test("where clause with control characters fails at parse time") {
    val parsed = yaml.parser
      .parse(
        "type: mysql\nhost: localhost\nport: 3306\ndatabase: mydb\ntable: data\n" +
          "credentials:\n  username: user\n  password: pass\n" +
          "where: \"status = 'active'\\n--\"\n"
      )
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when where clause contains control characters")
  }

  test("fetchSize larger than 100000 fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |fetchSize: 100001
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when fetchSize is larger than 100000")
  }

  test("IPv6 host in brackets passes") {
    val config =
      """type: mysql
        |host: "[2001:db8::1]"
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(result.host, "[2001:db8::1]")
  }

  test("connectionProperties with dangerous key fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |connectionProperties:
        |  allowLoadLocalInFile: "true"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when connectionProperties contains a dangerous key")
  }

  test("connectionProperties with safe keys passes at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |connectionProperties:
        |  connectTimeout: "5000"
        |  socketTimeout: "30000"
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(
      result.connectionProperties,
      Some(Map("connectTimeout" -> "5000", "socketTimeout" -> "30000"))
    )
  }

  test("empty where clause fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |where: ""
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when where clause is empty")
  }

  test("blank where clause fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |where: "   "
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when where clause is blank")
  }

  test("redacted password fails at parse time with savepoint hint") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: "<redacted>"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when password is '<redacted>'")
    assert(
      parsed.left.exists(_.getMessage.contains("savepoint")),
      "Error message should mention savepoint file"
    )
  }

  private def parseSourceSettings(yamlContent: String): SourceSettings.MySQL =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[SourceSettings]) match {
      case Left(error)                         => throw error
      case Right(source: SourceSettings.MySQL) => source
      case Right(other) => fail(s"Expected MySQL settings but got ${other.getClass.getSimpleName}")
    }
}

class ValidationConfigParserTest extends munit.FunSuite {

  test("Validation config without hashColumns uses default None") {
    val config =
      """compareTimestamps: true
        |ttlToleranceMillis: 60000
        |writetimeToleranceMillis: 1000
        |failuresToFetch: 100
        |floatingPointTolerance: 0.001
        |timestampMsTolerance: 0
        |""".stripMargin

    val result = parseValidation(config)
    assertEquals(result.hashColumns, None)
    assertEquals(result.compareTimestamps, true)
    assertEquals(result.failuresToFetch, 100)
  }

  test("Validation config with hashColumns") {
    val config =
      """compareTimestamps: false
        |ttlToleranceMillis: 0
        |writetimeToleranceMillis: 0
        |failuresToFetch: 50
        |floatingPointTolerance: 0.0
        |timestampMsTolerance: 100
        |hashColumns:
        |  - data_blob
        |  - large_text
        |""".stripMargin

    val result = parseValidation(config)
    assertEquals(result.hashColumns, Some(List("data_blob", "large_text")))
    assertEquals(result.timestampMsTolerance, 100L)
  }

  test("Validation config with empty hashColumns") {
    val config =
      """compareTimestamps: false
        |ttlToleranceMillis: 0
        |writetimeToleranceMillis: 0
        |failuresToFetch: 10
        |floatingPointTolerance: 0.0
        |timestampMsTolerance: 0
        |hashColumns: []
        |""".stripMargin

    val result = parseValidation(config)
    assertEquals(result.hashColumns, Some(Nil))
  }

  test("failuresToFetch 0 fails at parse time") {
    val config =
      """compareTimestamps: false
        |ttlToleranceMillis: 0
        |writetimeToleranceMillis: 0
        |failuresToFetch: 0
        |floatingPointTolerance: 0.0
        |timestampMsTolerance: 0
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[Validation])
    assert(parsed.isLeft, "Should fail when failuresToFetch is 0")
  }

  test("negative failuresToFetch fails at parse time") {
    val config =
      """compareTimestamps: false
        |ttlToleranceMillis: 0
        |writetimeToleranceMillis: 0
        |failuresToFetch: -1
        |floatingPointTolerance: 0.0
        |timestampMsTolerance: 0
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[Validation])
    assert(parsed.isLeft, "Should fail when failuresToFetch is negative")
  }

  private def parseValidation(yamlContent: String): Validation =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[Validation]) match {
      case Left(error)  => throw error
      case Right(value) => value
    }
}

class MySQLConfigSerializationTest extends munit.FunSuite {

  private val mysqlSource = SourceSettings.MySQL(
    host            = "db.example.com",
    port            = 3306,
    database        = "mydb",
    table           = "users",
    credentials     = Credentials("admin", "secret"),
    primaryKey      = Some(List("id", "tenant_id")),
    partitionColumn = Some("id"),
    numPartitions   = Some(4),
    lowerBound      = Some(1L),
    upperBound      = Some(100000L),
    fetchSize       = 500,
    where           = Some("status = 'active'"),
    connectionProperties = Some(
      Map(
        "connectTimeout"     -> "5000",
        "socketTimeout"      -> "30000",
        "trustStorePassword" -> "s3cr3t"
      )
    )
  )

  private val config1 = MigratorConfig(
    source = mysqlSource,
    target = TargetSettings.Scylla(
      host                          = "scylla-server",
      port                          = 9042,
      localDC                       = Some("datacenter1"),
      credentials                   = None,
      sslOptions                    = None,
      keyspace                      = "test_keyspace",
      table                         = "test_table",
      connections                   = Some(16),
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "LOCAL_QUORUM"
    ),
    renames          = Some(List(Rename("old_col", "new_col"))),
    savepoints       = Savepoints(300, "/app/savepoints"),
    skipTokenRanges  = None,
    skipSegments     = None,
    skipParquetFiles = None,
    validation = Some(
      Validation(
        compareTimestamps        = false,
        ttlToleranceMillis       = 0,
        writetimeToleranceMillis = 0,
        failuresToFetch          = 100,
        floatingPointTolerance   = 0.001,
        timestampMsTolerance     = 500,
        hashColumns              = Some(List("data_blob", "large_text"))
      )
    )
  )

  test("rendered YAML contains redacted credentials and sensitive connectionProperties") {
    val renderedYaml = config1.render

    assert(renderedYaml.contains("<redacted>"), "rendered YAML should contain <redacted>")
    assert(!renderedYaml.contains("secret"), "rendered YAML should not contain the plain password")
    assert(
      !renderedYaml.contains("s3cr3t"),
      "rendered YAML should not contain sensitive connectionProperty values"
    )
    // Non-sensitive connectionProperties should survive
    assert(renderedYaml.contains("5000"), "rendered YAML should contain connectTimeout value")
    assert(renderedYaml.contains("30000"), "rendered YAML should contain socketTimeout value")
  }

  test("loading rendered YAML fails because credentials are redacted") {
    val renderedYaml = config1.render
    val tempFile = Files.createTempFile("mysql-roundtrip-config", ".yaml")
    try {
      Files.write(tempFile, renderedYaml.getBytes(StandardCharsets.UTF_8))
      val error = intercept[Throwable](MigratorConfig.loadFrom(tempFile.toString))
      assert(
        error.getMessage.contains("savepoint"),
        "Error should mention savepoint file"
      )
    } finally
      Files.delete(tempFile)
  }

  test("non-credential fields survive YAML encode/decode round-trip") {
    // Parse the rendered YAML as raw JSON/YAML to verify non-credential fields
    // without going through the full config loader (which rejects redacted passwords).
    val renderedYaml = config1.render
    val json = yaml.parser.parse(renderedYaml).toOption.get
    val sourceCursor = json.hcursor.downField("source")

    assertEquals(sourceCursor.get[String]("host").toOption, Some("db.example.com"))
    assertEquals(sourceCursor.get[Int]("port").toOption, Some(3306))
    assertEquals(sourceCursor.get[String]("database").toOption, Some("mydb"))
    assertEquals(sourceCursor.get[String]("table").toOption, Some("users"))
    assertEquals(sourceCursor.get[Int]("fetchSize").toOption, Some(500))
    assertEquals(sourceCursor.get[String]("where").toOption, Some("status = 'active'"))
    assertEquals(
      sourceCursor.get[List[String]]("primaryKey").toOption,
      Some(List("id", "tenant_id"))
    )
    assertEquals(sourceCursor.get[String]("partitionColumn").toOption, Some("id"))
    assertEquals(sourceCursor.get[Int]("numPartitions").toOption, Some(4))

    // Verify connectionProperties redaction at the YAML level
    val props = sourceCursor.downField("connectionProperties")
    assertEquals(props.get[String]("connectTimeout").toOption, Some("5000"))
    assertEquals(props.get[String]("socketTimeout").toOption, Some("30000"))
    assertEquals(props.get[String]("trustStorePassword").toOption, Some("<redacted>"))

    // Verify validation config
    val validationCursor = json.hcursor.downField("validation")
    assertEquals(
      validationCursor.get[List[String]]("hashColumns").toOption,
      Some(List("data_blob", "large_text"))
    )
    assertEquals(validationCursor.get[Int]("failuresToFetch").toOption, Some(100))
    assertEquals(validationCursor.get[Long]("timestampMsTolerance").toOption, Some(500L))

    // Verify renames
    assertEquals(
      json.hcursor.get[List[Rename]]("renames").toOption,
      Some(List(Rename("old_col", "new_col")))
    )
  }
}
