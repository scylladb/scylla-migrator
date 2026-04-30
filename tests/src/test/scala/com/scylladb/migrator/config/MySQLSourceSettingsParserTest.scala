package com.scylladb.migrator.config

import io.circe.Json
import io.circe.yaml
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale

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
    assertEquals(result.zeroDateTimeBehavior, SourceSettings.MySQL.ZeroDateTimeBehavior.Exception)
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
    assertEquals(result.lowerBound, Some(SourceSettings.MySQL.PartitionBound("0")))
    assertEquals(result.upperBound, Some(SourceSettings.MySQL.PartitionBound("10000")))
    assertEquals(
      result.connectionProperties,
      Some(Map("connectTimeout" -> "5000", "socketTimeout" -> "30000"))
    )
  }

  test(
    "valid MySQL source config supports temporal partition bounds and explicit zero-date policy"
  ) {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: testdb
        |table: events
        |credentials:
        |  username: root
        |  password: "rootpass"
        |partitionColumn: created_at
        |numPartitions: 2
        |lowerBound: "2024-01-01 00:00:00"
        |upperBound: "2024-01-02 00:00:00"
        |zeroDateTimeBehavior: CONVERT_TO_NULL
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(
      result.lowerBound,
      Some(SourceSettings.MySQL.PartitionBound("2024-01-01 00:00:00"))
    )
    assertEquals(
      result.upperBound,
      Some(SourceSettings.MySQL.PartitionBound("2024-01-02 00:00:00"))
    )
    assertEquals(
      result.zeroDateTimeBehavior,
      SourceSettings.MySQL.ZeroDateTimeBehavior.ConvertToNull
    )
  }

  test("valid MySQL source config accepts same-day timestamp partition bounds") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: testdb
        |table: events
        |credentials:
        |  username: root
        |  password: "rootpass"
        |partitionColumn: created_at
        |numPartitions: 2
        |lowerBound: "2024-01-01 00:00:00"
        |upperBound: "2024-01-01 12:00:00"
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(
      result.lowerBound,
      Some(SourceSettings.MySQL.PartitionBound("2024-01-01 00:00:00"))
    )
    assertEquals(
      result.upperBound,
      Some(SourceSettings.MySQL.PartitionBound("2024-01-01 12:00:00"))
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

  test("invalid MySQL primaryKey values fail at parse time") {
    List(
      ("[]", "at least one"),
      ("""[" "]""", "blank"),
      ("[id, ID]", "duplicate")
    ).foreach { case (primaryKeyYaml, expectedError) =>
      val config =
        s"""type: mysql
           |host: localhost
           |port: 3306
           |database: mydb
           |table: data
           |credentials:
           |  username: user
           |  password: pass
           |primaryKey: $primaryKeyYaml
           |fetchSize: 100
           |""".stripMargin

      val parsed = yaml.parser
        .parse(config)
        .flatMap(_.as[SourceSettings])
      assert(parsed.isLeft, s"Should fail for primaryKey: $primaryKeyYaml")
      assert(parsed.left.exists(_.getMessage.contains(expectedError)))
    }
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

  test("invalid zeroDateTimeBehavior fails at parse time") {
    val config =
      """type: mysql
        |host: localhost
        |port: 3306
        |database: mydb
        |table: data
        |credentials:
        |  username: user
        |  password: pass
        |zeroDateTimeBehavior: INVALID
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail on an unsupported zeroDateTimeBehavior value")
  }

  test("dangerous connectionProperties stay blocked under Turkish default locale") {
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
        |connectionProperties:
        |  allowLoadLocalInfile: "true"
        |""".stripMargin

    val parsed = withDefaultLocale(new Locale("tr", "TR")) {
      yaml.parser
        .parse(config)
        .flatMap(_.as[SourceSettings])
    }

    assert(parsed.isLeft, "Should reject blocked keys regardless of default locale")
    assert(parsed.left.exists(_.getMessage.contains("blocked security-sensitive keys")))
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

  test("partition bounds without partitioning fail at parse time") {
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
        |lowerBound: 0
        |upperBound: 100
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when bounds are set without partitioning")
    assert(parsed.left.exists(_.getMessage.contains("lowerBound and upperBound")))
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

  test("DATE lowerBound after upperBound fails at parse time") {
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
        |partitionColumn: created_on
        |numPartitions: 4
        |lowerBound: "2024-02-01"
        |upperBound: "2024-01-01"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when DATE lowerBound >= upperBound")
  }

  test("TIMESTAMP lowerBound equal to upperBound fails at parse time") {
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
        |partitionColumn: created_at
        |numPartitions: 4
        |lowerBound: "2024-01-01 12:00:00"
        |upperBound: "2024-01-01 12:00:00"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when TIMESTAMP lowerBound >= upperBound")
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
    List(
      "allowLoadLocalInFile",
      "allowLoadLocalInfileInPath"
    ).foreach { dangerousKey =>
      val config =
        s"""type: mysql
           |host: localhost
           |port: 3306
           |database: mydb
           |table: data
           |credentials:
           |  username: user
           |  password: pass
           |connectionProperties:
           |  $dangerousKey: "true"
           |""".stripMargin

      val parsed = yaml.parser
        .parse(config)
        .flatMap(_.as[SourceSettings])
      assert(
        parsed.isLeft,
        s"Should fail when connectionProperties contains dangerous key $dangerousKey"
      )
    }
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

  test("connectionProperties with injected connectTimeout fails at parse time") {
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
        |  connectTimeout: "5000&allowLoadLocalInfile=true"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when connectTimeout contains injected URL parameters")
  }

  test("connectionProperties with injected socketTimeout fails at parse time") {
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
        |  socketTimeout: "600000&allowMultiQueries=true"
        |""".stripMargin

    val parsed = yaml.parser
      .parse(config)
      .flatMap(_.as[SourceSettings])
    assert(parsed.isLeft, "Should fail when socketTimeout contains injected URL parameters")
  }

  test("connectionProperties with zero timeouts pass at parse time") {
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
        |  connectTimeout: "0"
        |  socketTimeout: "0"
        |""".stripMargin

    val result = parseSourceSettings(config)
    assertEquals(
      result.connectionProperties,
      Some(Map("connectTimeout" -> "0", "socketTimeout" -> "0"))
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

  private def withDefaultLocale[A](locale: Locale)(f: => A): A = {
    val original = Locale.getDefault
    Locale.setDefault(locale)
    try f
    finally Locale.setDefault(original)
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

  test("negative validation tolerances fail at parse time") {
    List(
      "ttlToleranceMillis",
      "writetimeToleranceMillis",
      "timestampMsTolerance",
      "floatingPointTolerance"
    ).foreach { fieldName =>
      val values = Map(
        "ttlToleranceMillis"       -> "0",
        "writetimeToleranceMillis" -> "0",
        "timestampMsTolerance"     -> "0",
        "floatingPointTolerance"   -> "0.0"
      ).updated(fieldName, "-1")
      val config =
        s"""compareTimestamps: false
           |ttlToleranceMillis: ${values("ttlToleranceMillis")}
           |writetimeToleranceMillis: ${values("writetimeToleranceMillis")}
           |failuresToFetch: 1
           |floatingPointTolerance: ${values("floatingPointTolerance")}
           |timestampMsTolerance: ${values("timestampMsTolerance")}
           |""".stripMargin

      val parsed = yaml.parser
        .parse(config)
        .flatMap(_.as[Validation])
      assert(parsed.isLeft, s"Should fail when $fieldName is negative")
      assert(parsed.left.exists(_.getMessage.contains(fieldName)))
    }
  }

  test("invalid hashColumns fail at parse time") {
    List(
      ("""[data_blob, " "]""", "blank"),
      ("[data_blob, DATA_BLOB]", "duplicate")
    ).foreach { case (hashColumnsYaml, expectedError) =>
      val config =
        s"""compareTimestamps: false
           |ttlToleranceMillis: 0
           |writetimeToleranceMillis: 0
           |failuresToFetch: 1
           |floatingPointTolerance: 0.0
           |timestampMsTolerance: 0
           |hashColumns: $hashColumnsYaml
           |""".stripMargin

      val parsed = yaml.parser
        .parse(config)
        .flatMap(_.as[Validation])
      assert(parsed.isLeft, s"Should fail for hashColumns: $hashColumnsYaml")
      assert(parsed.left.exists(_.getMessage.contains(expectedError)))
    }
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
    lowerBound      = Some(SourceSettings.MySQL.PartitionBound("1")),
    upperBound      = Some(SourceSettings.MySQL.PartitionBound("100000")),
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

  private val resumableConfig = MigratorConfig(
    source = SourceSettings.Cassandra(
      host               = "cassandra-source",
      port               = 9042,
      localDC            = Some("dc1"),
      credentials        = Some(Credentials("source-user", "source-secret")),
      sslOptions         = None,
      keyspace           = "source_ks",
      table              = "source_table",
      splitCount         = Some(8),
      connections        = Some(4),
      fetchSize          = 512,
      preserveTimestamps = false,
      where              = None,
      consistencyLevel   = "LOCAL_QUORUM"
    ),
    target = TargetSettings.Scylla(
      host                          = "scylla-target",
      port                          = 9042,
      localDC                       = Some("dc1"),
      credentials                   = Some(Credentials("target-user", "target-secret")),
      sslOptions                    = None,
      keyspace                      = "target_ks",
      table                         = "target_table",
      connections                   = Some(16),
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "LOCAL_QUORUM"
    ),
    renames          = None,
    savepoints       = Savepoints(300, "/app/savepoints"),
    skipTokenRanges  = None,
    skipSegments     = None,
    skipParquetFiles = None,
    validation       = None
  )

  test("rendered YAML round-trips authenticated credentials for resumable configs") {
    val renderedYaml = resumableConfig.render
    val tempFile = Files.createTempFile("resumable-config-roundtrip", ".yaml")
    try {
      Files.write(tempFile, renderedYaml.getBytes(StandardCharsets.UTF_8))
      val loaded = MigratorConfig.loadFrom(tempFile.toString)
      val loadedSource = loaded.source.asInstanceOf[SourceSettings.Cassandra]
      val loadedTarget = loaded.target.asInstanceOf[TargetSettings.Scylla]
      assertEquals(loadedSource.credentials, Some(Credentials("source-user", "source-secret")))
      assertEquals(loadedTarget.credentials, Some(Credentials("target-user", "target-secret")))
    } finally
      Files.delete(tempFile)
  }

  test("MySQL configs can omit savepoints and receive the default value") {
    val yamlContent =
      """source:
        |  type: mysql
        |  host: db.example.com
        |  port: 3306
        |  database: mydb
        |  table: users
        |  credentials:
        |    username: admin
        |    password: secret
        |target:
        |  type: scylla
        |  host: scylla-server
        |  port: 9042
        |  keyspace: test_keyspace
        |  table: test_table
        |  consistencyLevel: LOCAL_QUORUM
        |  stripTrailingZerosForDecimals: false
        |validation:
        |  compareTimestamps: false
        |  ttlToleranceMillis: 0
        |  writetimeToleranceMillis: 0
        |  failuresToFetch: 100
        |  floatingPointTolerance: 0.001
        |  timestampMsTolerance: 500
        |""".stripMargin
    val tempFile = Files.createTempFile("mysql-config-no-savepoints", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))
      val loaded = MigratorConfig.loadFrom(tempFile.toString)
      assertEquals(loaded.savepoints, Savepoints.Default)
    } finally
      Files.delete(tempFile)
  }

  test("rendered MySQL configs omit savepoints because they are not supported") {
    val renderedYaml = config1.render
    val redactedYaml = config1.renderRedacted

    assert(!renderedYaml.contains("savepoints"))
    assert(!renderedYaml.contains("/app/savepoints"))
    assert(!redactedYaml.contains("savepoints"))
    assert(!redactedYaml.contains("/app/savepoints"))
  }

  test("non-MySQL configs still require explicit savepoints") {
    val yamlContent =
      """source:
        |  type: cassandra
        |  host: cassandra-source
        |  port: 9042
        |  keyspace: source_ks
        |  table: source_table
        |  fetchSize: 512
        |  preserveTimestamps: false
        |  consistencyLevel: LOCAL_QUORUM
        |target:
        |  type: scylla
        |  host: scylla-target
        |  port: 9042
        |  keyspace: target_ks
        |  table: target_table
        |  consistencyLevel: LOCAL_QUORUM
        |  stripTrailingZerosForDecimals: false
        |""".stripMargin
    val tempFile = Files.createTempFile("cassandra-config-no-savepoints", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))
      val error = intercept[Throwable](MigratorConfig.loadFrom(tempFile.toString))
      assert(error.getMessage.contains("savepoints"))
    } finally
      Files.delete(tempFile)
  }

  test("redactSecrets redacts supported sensitive key variants recursively") {
    val redacted = MigratorConfig.redactSecrets(
      Json.obj(
        "type"  -> Json.fromString("mysql"),
        "where" -> Json.fromString("email = 'user@example.com'"),
        "credentials" -> Json.obj(
          "accessKey"  -> Json.fromString("AKIA123456"),
          "secretKey"  -> Json.fromString("secret-value"),
          "access_key" -> Json.fromString("AKIA654321")
        ),
        "connectionProperties" -> Json.obj(
          "apiKey"         -> Json.fromString("api-secret"),
          "access-key"     -> Json.fromString("secondary-secret"),
          "privateKey"     -> Json.fromString("pem-secret"),
          "connectTimeout" -> Json.fromString("5000")
        )
      )
    )

    val credentials = redacted.hcursor.downField("credentials")
    val connectionProperties = redacted.hcursor.downField("connectionProperties")

    assertEquals(redacted.hcursor.get[String]("where").toOption, Some("<redacted>"))
    assertEquals(credentials.get[String]("accessKey").toOption, Some("<redacted>"))
    assertEquals(credentials.get[String]("secretKey").toOption, Some("<redacted>"))
    assertEquals(credentials.get[String]("access_key").toOption, Some("<redacted>"))
    assertEquals(connectionProperties.get[String]("apiKey").toOption, Some("<redacted>"))
    assertEquals(connectionProperties.get[String]("access-key").toOption, Some("<redacted>"))
    assertEquals(connectionProperties.get[String]("privateKey").toOption, Some("<redacted>"))
    assertEquals(connectionProperties.get[String]("connectTimeout").toOption, Some("5000"))
  }

  test("renderRedacted hides credentials and sensitive connectionProperties") {
    val renderedYaml = config1.renderRedacted

    assert(renderedYaml.contains("<redacted>"), "rendered YAML should contain <redacted>")
    assert(!renderedYaml.contains("secret"), "rendered YAML should not contain the plain password")
    assert(
      !renderedYaml.contains("s3cr3t"),
      "rendered YAML should not contain sensitive connectionProperty values"
    )
    assert(
      !renderedYaml.contains("status = 'active'"),
      "rendered YAML should not contain the MySQL WHERE predicate"
    )
    // Non-sensitive connectionProperties should survive
    assert(renderedYaml.contains("5000"), "rendered YAML should contain connectTimeout value")
    assert(renderedYaml.contains("30000"), "rendered YAML should contain socketTimeout value")
  }

  test("loading renderRedacted YAML fails because credentials are redacted") {
    val renderedYaml = config1.renderRedacted
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
    val renderedYaml = config1.renderRedacted
    val json = yaml.parser.parse(renderedYaml).toOption.get
    val sourceCursor = json.hcursor.downField("source")

    assertEquals(sourceCursor.get[String]("host").toOption, Some("db.example.com"))
    assertEquals(sourceCursor.get[Int]("port").toOption, Some(3306))
    assertEquals(sourceCursor.get[String]("database").toOption, Some("mydb"))
    assertEquals(sourceCursor.get[String]("table").toOption, Some("users"))
    assertEquals(sourceCursor.get[Int]("fetchSize").toOption, Some(500))
    assertEquals(sourceCursor.get[String]("where").toOption, Some("<redacted>"))
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
