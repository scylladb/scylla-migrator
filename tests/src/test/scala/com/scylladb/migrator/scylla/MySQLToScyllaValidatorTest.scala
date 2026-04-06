package com.scylladb.migrator.scylla

import com.scylladb.migrator.config.Rename
import com.scylladb.migrator.readers.MySQL
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

import java.lang.reflect.{ InvocationHandler, Method, Proxy }
import java.sql.{ DatabaseMetaData, ResultSet }

class MySQLToScyllaValidatorTest extends munit.FunSuite {

  private def dynamicProxy[A](iface: Class[A])(
    handler: (Method, Array[AnyRef]) => AnyRef
  ): A =
    Proxy
      .newProxyInstance(
        iface.getClassLoader,
        Array(iface),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
            handler(method, Option(args).getOrElse(Array.empty[AnyRef]))
        }
      )
      .asInstanceOf[A]

  private def primaryKeyResultSet(rows: Seq[(Int, String)]): ResultSet = {
    var index = -1
    dynamicProxy(classOf[ResultSet]) { (method, args) =>
      method.getName match {
        case "next" =>
          index += 1
          Boolean.box(index < rows.size)
        case "getShort" =>
          assertEquals(args(0), "KEY_SEQ")
          Short.box(rows(index)._1.toShort)
        case "getString" =>
          assertEquals(args(0), "COLUMN_NAME")
          rows(index)._2
        case "close"   => null
        case "wasNull" => Boolean.box(false)
        case other     => fail(s"Unexpected ResultSet method: $other")
      }
    }
  }

  private def areDifferent(
    left: Option[Any],
    right: Option[Any],
    fpTol: Double = 0.0,
    tsTol: Long = 0L
  ): Boolean =
    MySQLToScyllaValidator.areDifferent(left, right, tsTol, fpTol)

  // --- Null / None handling ---

  test("both None are equal") {
    assert(!areDifferent(None, None))
  }

  test("Some vs None are different") {
    assert(areDifferent(Some("a"), None))
  }

  test("None vs Some are different") {
    assert(areDifferent(None, Some("a")))
  }

  // --- Basic equality ---

  test("equal strings") {
    assert(!areDifferent(Some("hello"), Some("hello")))
  }

  test("different strings") {
    assert(areDifferent(Some("hello"), Some("world")))
  }

  test("equal integers") {
    assert(!areDifferent(Some(42), Some(42)))
  }

  test("different integers") {
    assert(areDifferent(Some(42), Some(43)))
  }

  // --- Timestamp comparison ---

  test("java.sql.Timestamp within tolerance") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1050L)
    assert(!areDifferent(Some(t1), Some(t2), tsTol = 100L))
  }

  test("java.sql.Timestamp outside tolerance") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1200L)
    assert(areDifferent(Some(t1), Some(t2), tsTol = 100L))
  }

  test("java.sql.Timestamp with zero tolerance uses standard equality") {
    val t1 = new java.sql.Timestamp(1000L)
    val t2 = new java.sql.Timestamp(1001L)
    assert(areDifferent(Some(t1), Some(t2), tsTol = 0L))
  }

  test("java.sql.Timestamp with zero tolerance preserves sub-millisecond precision") {
    val t1 = java.sql.Timestamp.from(java.time.Instant.parse("2024-01-01T00:00:00.123456Z"))
    val t2 = java.sql.Timestamp.from(java.time.Instant.parse("2024-01-01T00:00:00.123999Z"))

    assert(areDifferent(Some(t1), Some(t2), tsTol = 0L))
    assert(!areDifferent(Some(t1), Some(t2), tsTol = 1L))
  }

  test("java.time.Instant within tolerance") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1050L)
    assert(!areDifferent(Some(i1), Some(i2), tsTol = 100L))
  }

  test("java.time.Instant outside tolerance") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1200L)
    assert(areDifferent(Some(i1), Some(i2), tsTol = 100L))
  }

  test("java.time.Instant with zero tolerance uses standard equality") {
    val i1 = java.time.Instant.ofEpochMilli(1000L)
    val i2 = java.time.Instant.ofEpochMilli(1001L)
    assert(areDifferent(Some(i1), Some(i2), tsTol = 0L))
  }

  test("java.time.Instant with zero tolerance preserves sub-millisecond precision") {
    val i1 = java.time.Instant.parse("2024-01-01T00:00:00.123456Z")
    val i2 = java.time.Instant.parse("2024-01-01T00:00:00.123999Z")

    assert(areDifferent(Some(i1), Some(i2), tsTol = 0L))
    assert(!areDifferent(Some(i1), Some(i2), tsTol = 1L))
  }

  // --- Float comparison ---

  test("floats within tolerance") {
    assert(!areDifferent(Some(1.0f), Some(1.001f), fpTol = 0.01))
  }

  test("floats outside tolerance") {
    assert(areDifferent(Some(1.0f), Some(1.1f), fpTol = 0.01))
  }

  // --- Double comparison ---

  test("doubles within tolerance") {
    assert(!areDifferent(Some(1.0d), Some(1.001d), fpTol = 0.01))
  }

  test("doubles outside tolerance") {
    assert(areDifferent(Some(1.0d), Some(1.1d), fpTol = 0.01))
  }

  // --- NaN / Infinity edge cases (bug #3) ---

  test("Double NaN values are considered equal") {
    assert(!areDifferent(Some(Double.NaN), Some(Double.NaN), fpTol = 0.01))
  }

  test("Float NaN values are considered equal") {
    assert(!areDifferent(Some(Float.NaN), Some(Float.NaN), fpTol = 0.01))
  }

  test("Double NaN vs regular value are different") {
    assert(areDifferent(Some(Double.NaN), Some(1.0d), fpTol = 0.01))
  }

  test("Float NaN vs regular value are different") {
    assert(areDifferent(Some(Float.NaN), Some(1.0f), fpTol = 0.01))
  }

  test("Double positive infinity values are equal") {
    assert(
      !areDifferent(Some(Double.PositiveInfinity), Some(Double.PositiveInfinity), fpTol = 0.01)
    )
  }

  test("Double positive infinity vs negative infinity are different") {
    assert(
      areDifferent(Some(Double.PositiveInfinity), Some(Double.NegativeInfinity), fpTol = 0.01)
    )
  }

  // --- BigDecimal comparison ---

  test("BigDecimals within tolerance") {
    val a = new java.math.BigDecimal("100.001")
    val b = new java.math.BigDecimal("100.002")
    assert(!areDifferent(Some(a), Some(b), fpTol = 0.01))
  }

  test("BigDecimals outside tolerance") {
    val a = new java.math.BigDecimal("100.0")
    val b = new java.math.BigDecimal("100.1")
    assert(areDifferent(Some(a), Some(b), fpTol = 0.01))
  }

  test("mixed integral wrapper types are considered equal") {
    assert(!areDifferent(Some(42), Some(42L)))
  }

  test("integral and decimal wrapper types are considered equal") {
    val decimal = new java.math.BigDecimal("42.0")
    assert(!areDifferent(Some(42L), Some(decimal)))
  }

  test("mixed numeric wrappers still report real inequality") {
    val decimal = new java.math.BigDecimal("42.1")
    assert(areDifferent(Some(42), Some(decimal), fpTol = 0.0))
  }

  test("floating-point specials are normalized across wrapper types") {
    assert(
      !areDifferent(Some(Float.PositiveInfinity), Some(Double.PositiveInfinity), fpTol = 0.01)
    )
    assert(!areDifferent(Some(Float.NaN), Some(Double.NaN), fpTol = 0.01))
  }

  // --- Byte array comparison ---

  test("equal byte arrays") {
    val a = Array[Byte](1, 2, 3)
    val b = Array[Byte](1, 2, 3)
    assert(!areDifferent(Some(a), Some(b)))
  }

  test("different byte arrays") {
    val a = Array[Byte](1, 2, 3)
    val b = Array[Byte](1, 2, 4)
    assert(areDifferent(Some(a), Some(b)))
  }

  // --- Generic array comparison ---

  test("equal generic arrays") {
    val a = Array("a", "b")
    val b = Array("a", "b")
    assert(!areDifferent(Some(a), Some(b)))
  }

  test("different generic arrays") {
    val a = Array("a", "b")
    val b = Array("a", "c")
    assert(areDifferent(Some(a), Some(b)))
  }

  // --- validator preflight checks ---

  test("validateTargetPrimaryKey accepts same columns with different case in the same order") {
    MySQLToScyllaValidator.validateTargetPrimaryKey(
      configuredPrimaryKey   = Seq("TenantId", "UserId"),
      actualTargetPrimaryKey = Seq("tenantid", "userid")
    )
  }

  test("validateTargetPrimaryKey rejects mismatched target PK") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateTargetPrimaryKey(
        configuredPrimaryKey   = Seq("tenant_id"),
        actualTargetPrimaryKey = Seq("tenant_id", "user_id")
      )
    }
    assert(error.getMessage.contains("does not match target table's actual PK"))
  }

  test("validateTargetPrimaryKey rejects target PK with the same columns in a different order") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateTargetPrimaryKey(
        configuredPrimaryKey   = Seq("TenantId", "UserId"),
        actualTargetPrimaryKey = Seq("userid", "tenantid")
      )
    }
    assert(error.getMessage.contains("does not match target table's actual PK"))
  }

  test("validateSourcePrimaryKey accepts same columns with different case in the same order") {
    MySQLToScyllaValidator.validateSourcePrimaryKey(
      configuredPrimaryKey   = Seq("TenantId", "UserId"),
      actualSourcePrimaryKey = Seq("tenantid", "userid")
    )
  }

  test("validateSourcePrimaryKey rejects mismatched source PK") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateSourcePrimaryKey(
        configuredPrimaryKey   = Seq("tenant_id"),
        actualSourcePrimaryKey = Seq("tenant_id", "user_id")
      )
    }
    assert(error.getMessage.contains("does not match the MySQL table's actual PK"))
  }

  test("validateSourcePrimaryKey rejects source PK with the same columns in a different order") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateSourcePrimaryKey(
        configuredPrimaryKey   = Seq("TenantId", "UserId"),
        actualSourcePrimaryKey = Seq("userid", "tenantid")
      )
    }
    assert(error.getMessage.contains("does not match the MySQL table's actual PK"))
  }

  test("sourcePrimaryKeyFromMetadata escapes metadata wildcards in table names") {
    var requestedCatalog: String = null
    var requestedTableName: String = null
    val metaData = dynamicProxy(classOf[DatabaseMetaData]) { (method, args) =>
      method.getName match {
        case "getSearchStringEscape" =>
          "\\"
        case "getPrimaryKeys" =>
          requestedCatalog = args(0).asInstanceOf[String]
          assertEquals(args(1), null)
          requestedTableName = args(2).asInstanceOf[String]
          primaryKeyResultSet(Seq(2 -> "created_at", 1 -> "id"))
        case other => fail(s"Unexpected DatabaseMetaData method: $other")
      }
    }

    val primaryKey =
      MySQLToScyllaValidator
        .sourcePrimaryKeyFromMetadata(metaData, "source_db", "user_events%archive")

    assertEquals(requestedCatalog, "source_db")
    assertEquals(requestedTableName, """user\_events\%archive""")
    assertEquals(primaryKey, Seq("id", "created_at"))
  }

  test("liveWriteWarning explains the lack of point-in-time safety") {
    val warning = MySQLToScyllaValidator.liveWriteWarning(
      sourceSettings = com.scylladb.migrator.config.SourceSettings.MySQL(
        host                 = "mysql",
        port                 = 3306,
        database             = "app",
        table                = "users",
        credentials          = com.scylladb.migrator.config.Credentials("u", "p"),
        primaryKey           = Some(List("id")),
        partitionColumn      = None,
        numPartitions        = None,
        lowerBound           = None,
        upperBound           = None,
        fetchSize            = 1000,
        where                = None,
        connectionProperties = None
      ),
      targetSettings = com.scylladb.migrator.config.TargetSettings.Scylla(
        host                          = "scylla",
        port                          = 9042,
        localDC                       = None,
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "users",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      )
    )

    assert(warning.contains("not point-in-time safe"))
    assert(warning.contains("app.users"))
    assert(warning.contains("ks.users"))
    assert(warning.contains("quiesced"))
  }

  test("validateSourceColumnsPresentInTarget allows target-only extra columns") {
    MySQLToScyllaValidator.validateSourceColumnsPresentInTarget(
      sourceColumns = Seq("id", "value"),
      targetColumns = Seq("id", "value", "updated_at")
    )
    assertEquals(
      MySQLToScyllaValidator.targetOnlyColumns(
        sourceColumns = Seq("id", "value"),
        targetColumns = Seq("id", "value", "updated_at")
      ),
      Seq("updated_at")
    )
  }

  test("validateSourceColumnsPresentInTarget rejects source columns missing in target") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateSourceColumnsPresentInTarget(
        sourceColumns = Seq("id", "value", "missing_col"),
        targetColumns = Seq("id", "value")
      )
    }
    assert(error.getMessage.contains("missing_col"))
  }

  test("validateHashColumnsPresentOnBothSides accepts columns present in source and target") {
    MySQLToScyllaValidator.validateHashColumnsPresentOnBothSides(
      requestedHashColumns = Seq("payload", "checksum"),
      sourceColumns        = Seq("id", "payload", "checksum"),
      targetColumns        = Seq("id", "checksum", "payload", "updated_at")
    )
  }

  test("validateHashColumnsPresentOnBothSides rejects columns missing in target") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateHashColumnsPresentOnBothSides(
        requestedHashColumns = Seq("payload", "checksum"),
        sourceColumns        = Seq("id", "payload", "checksum"),
        targetColumns        = Seq("id", "payload")
      )
    }
    assert(error.getMessage.contains("missing in target: checksum"))
  }

  test("validateHashColumnsPresentOnBothSides rejects columns missing in source") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.validateHashColumnsPresentOnBothSides(
        requestedHashColumns = Seq("payload", "checksum"),
        sourceColumns        = Seq("id", "payload"),
        targetColumns        = Seq("id", "payload", "checksum")
      )
    }
    assert(error.getMessage.contains("missing in source: checksum"))
  }

  test("buildCaseInsensitiveRenameMap resolves mixed-case rename entries") {
    val renames = MySQLToScyllaValidator.buildCaseInsensitiveRenameMap(
      List(Rename("UserId", "user_id"), Rename("DisplayName", "display_name"))
    )

    assertEquals(renames("userid"), "user_id")
    assertEquals(renames("displayname"), "display_name")
  }

  test("buildCaseInsensitiveRenameMap rejects conflicting case-insensitive renames") {
    val error = intercept[RuntimeException] {
      MySQLToScyllaValidator.buildCaseInsensitiveRenameMap(
        List(Rename("UserId", "user_id"), Rename("userid", "account_id"))
      )
    }

    assert(error.getMessage.contains("conflicting case-insensitive mappings"))
  }

  test("escapeSparkColumnName quotes dots and doubles embedded backticks") {
    assertEquals(MySQLToScyllaValidator.escapeSparkColumnName("user.name"), "`user.name`")
    assertEquals(MySQLToScyllaValidator.escapeSparkColumnName("a`b"), "`a``b`")
  }

  test("compareFieldsBySchemaForRow ignores hash mismatches when decimal values are equal") {
    val differingFields = MySQLToScyllaValidator.compareFieldsBySchemaForRow(
      joinedRow = Row(
        new java.math.BigDecimal("1.0"),
        new java.math.BigDecimal("1.00"),
        "src-hash",
        "tgt-hash"
      ),
      directFieldIndices      = Nil,
      hashBackedFieldIndices  = Seq(("amount", 0, 1)),
      contentHashFieldIndices = Some((2, 3)),
      timestampMsTolerance    = 0L,
      floatingPointTolerance  = 0.0
    )

    assertEquals(differingFields, Nil)
  }

  test("compareFieldsBySchemaForRow reports hash-backed values outside tolerance") {
    val differingFields = MySQLToScyllaValidator.compareFieldsBySchemaForRow(
      joinedRow = Row(
        new java.math.BigDecimal("1.0"),
        new java.math.BigDecimal("1.2"),
        "src-hash",
        "tgt-hash"
      ),
      directFieldIndices      = Nil,
      hashBackedFieldIndices  = Seq(("amount", 0, 1)),
      contentHashFieldIndices = Some((2, 3)),
      timestampMsTolerance    = 0L,
      floatingPointTolerance  = 0.05
    )

    assertEquals(differingFields, List("amount"))
  }

  test("differingFieldsBetweenRows ignores decimal scale-only differences") {
    val differingFields = MySQLToScyllaValidator.differingFieldsBetweenRows(
      sourceRow              = Row(1, new java.math.BigDecimal("1.0")),
      sourceFields           = Array("id", "amount"),
      targetRow              = Row(1, new java.math.BigDecimal("1.00")),
      targetFields           = Array("id", "amount"),
      columns                = Seq("amount"),
      timestampMsTolerance   = 0L,
      floatingPointTolerance = 0.0
    )

    assertEquals(differingFields, Nil)
  }

  // --- prefixColumns tests (Spark-local) ---

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("MySQLToScyllaValidatorTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("prefixColumns renames all columns atomically") {
    import spark.implicits._
    val df = Seq((1, "a"), (2, "b")).toDF("id", "name")
    val prefixed = MySQLToScyllaValidator.prefixColumns(df, "src_")
    assertEquals(prefixed.columns.toList, List("src_id", "src_name"))
    assertEquals(prefixed.count(), 2L)
  }

  test("prefixColumns handles columns that match prefix pattern") {
    import spark.implicits._
    // Column "src_status" already looks like a prefixed name -- should not collide
    val df = Seq((1, "ok", "active")).toDF("status", "src_status", "flag")
    val prefixed = MySQLToScyllaValidator.prefixColumns(df, "src_")
    assertEquals(prefixed.columns.sorted.toList, List("src_flag", "src_src_status", "src_status"))
    assertEquals(prefixed.count(), 1L)
  }

  test("sparkColumn resolves literal column names with dots and backticks") {
    import spark.implicits._
    val df = Seq((1, "alpha", "beta")).toDF("id", "user.name", "a`b")
    val selected = df.select(
      MySQLToScyllaValidator.sparkColumn("user.name").as("user.name"),
      MySQLToScyllaValidator.sparkColumn("a`b").as("a`b")
    )

    assertEquals(selected.columns.toList, List("user.name", "a`b"))
    assertEquals(selected.collect().toList, List(Row("alpha", "beta")))
  }

  test("prefixColumns preserves literal column names with dots and backticks") {
    import spark.implicits._
    val df = Seq((1, "alpha", "beta")).toDF("id", "user.name", "a`b")
    val prefixed = MySQLToScyllaValidator.prefixColumns(df, "src_")

    assertEquals(prefixed.columns.toList, List("src_id", "src_user.name", "src_a`b"))
    assertEquals(prefixed.collect().toList, List(Row(1, "alpha", "beta")))
  }

  // --- addContentHash tests (Spark-local) ---

  test("addContentHash adds hash column and drops hashed columns") {
    import spark.implicits._
    val df = Seq((1, "hello", 42)).toDF("id", "text", "num")
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text", "num"), List("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
    assert(!hashed.columns.contains("text"))
    assert(!hashed.columns.contains("num"))
    assert(hashed.columns.contains("id"))
  }

  test("addContentHash preserves PK columns even if they are in hashCols") {
    import spark.implicits._
    val df = Seq((1, "hello")).toDF("id", "text")
    // id is in pkCols, so it should not be dropped even though we hash text
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    assert(hashed.columns.contains("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
  }

  test("addContentHash returns DF unchanged when no hash columns exist") {
    import spark.implicits._
    val df = Seq((1, "hello")).toDF("id", "text")
    val result =
      MySQLToScyllaValidator.addContentHash(df, List("nonexistent"), List("id"))
    assertEquals(result.columns.toSet, df.columns.toSet)
    assert(!result.columns.contains(MySQL.ContentHashColumn))
  }

  test("addContentHash handles NULL values without error") {
    import spark.implicits._
    val df = Seq((1, Option.empty[String]), (2, Some("hello"))).toDF("id", "text")
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
    assertEquals(hashed.count(), 2L)
  }

  test("addContentHash distinguishes NULL from the literal string 'NULL'") {
    import spark.implicits._
    val df = Seq(
      (1, Option.empty[String]),
      (2, Some("NULL"))
    ).toDF("id", "text")
    val hashed = MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    val hashes = hashed.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertNotEquals(hashes(0), hashes(1))
  }

  test("addContentHash distinguishes NULL from the literal string '__NULL_SENTINEL__'") {
    import spark.implicits._
    val df = Seq(
      (1, Option.empty[String]),
      (2, Some("__NULL_SENTINEL__"))
    ).toDF("id", "text")
    val hashed = MySQLToScyllaValidator.addContentHash(df, List("text"), List("id"))
    val hashes = hashed.orderBy("id").select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertNotEquals(hashes(0), hashes(1))
  }

  test("addContentHash hashes binary columns from raw bytes") {
    import spark.implicits._
    val df = Seq(
      (1, Array[Byte](1, 2, 3)),
      (2, Array[Byte](1, 2, 4))
    ).toDF("id", "payload")
    val hashed = MySQLToScyllaValidator.addContentHash(df, List("payload"), List("id"))
    val hashes = hashed.orderBy("id").select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertNotEquals(hashes(0), hashes(1))
  }

  test("addContentHash handles column names with dots and backticks") {
    import spark.implicits._
    val df = Seq((1, "alpha", "beta")).toDF("id", "user.name", "a`b")
    val hashed =
      MySQLToScyllaValidator.addContentHash(df, List("user.name", "a`b"), List("id"))

    assertEquals(hashed.columns.toList, List("id", MySQL.ContentHashColumn))
    assertEquals(hashed.count(), 1L)
  }

  test("addContentHash produces same hash regardless of DataFrame column order") {
    import spark.implicits._
    // Simulate source with columns in one order and target with columns in different order.
    // The hash should be identical for the same data.
    val df1 = Seq((1, "hello", 42)).toDF("id", "Name", "Age")
    val df2 = Seq((1, 42, "hello")).toDF("id", "age", "name")
    val hashed1 =
      MySQLToScyllaValidator.addContentHash(df1, List("Name", "Age"), List("id"))
    val hashed2 =
      MySQLToScyllaValidator.addContentHash(df2, List("name", "age"), List("id"))
    val hash1 = hashed1.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    val hash2 = hashed2.select(MySQL.ContentHashColumn).collect().map(_.getString(0))
    assertEquals(hash1.toList, hash2.toList)
  }

  test("addContentHash can preserve hash-backed columns for fallback comparison") {
    import spark.implicits._
    val df = Seq((1, "hello", 42)).toDF("id", "text", "num")
    val hashed =
      MySQLToScyllaValidator.addContentHash(
        df,
        List("text", "num"),
        List("id"),
        dropHashedColumns = false
      )

    assert(hashed.columns.contains("text"))
    assert(hashed.columns.contains("num"))
    assert(hashed.columns.contains(MySQL.ContentHashColumn))
  }

  test("hash-based comparison join schema excludes hashed payload columns") {
    import spark.implicits._
    val source = Seq((1, "hello", 42)).toDF("id", "text", "num")
    val target = Seq((1, "hello", 42)).toDF("id", "text", "num")

    val hashedSource =
      MySQLToScyllaValidator.addContentHash(source, List("text", "num"), List("id"))
    val hashedTarget =
      MySQLToScyllaValidator.addContentHash(target, List("text", "num"), List("id"))

    val joined = MySQLToScyllaValidator
      .prefixColumns(hashedSource, "src_")
      .join(
        MySQLToScyllaValidator.prefixColumns(hashedTarget, "tgt_"),
        col("src_id") === col("tgt_id"),
        "inner"
      )

    assert(!joined.columns.contains("src_text"))
    assert(!joined.columns.contains("src_num"))
    assert(!joined.columns.contains("tgt_text"))
    assert(!joined.columns.contains("tgt_num"))
    assert(joined.columns.contains(s"src_${MySQL.ContentHashColumn}"))
    assert(joined.columns.contains(s"tgt_${MySQL.ContentHashColumn}"))
  }

  test("resolveHashBackedDifferences refines sampled hash mismatches by primary key") {
    import spark.implicits._
    implicit val sparkSession: SparkSession = spark
    val source = Seq(
      (1, "blob-a", "same"),
      (2, "blob-b", "same")
    ).toDF("id", "payload", "comment")
    val target = Seq(
      (1, "blob-a", "same"),
      (2, "blob-b", "changed")
    ).toDF("id", "payload", "comment")

    val differingFields = MySQLToScyllaValidator.resolveHashBackedDifferences(
      rawSourceDF            = source,
      rawTargetDF            = target,
      primaryKey             = Seq("id"),
      primaryKeyValues       = Seq(Vector[Any](2)),
      hashBackedColumns      = Seq("payload", "comment"),
      timestampMsTolerance   = 0L,
      floatingPointTolerance = 0.0
    )(spark)

    assertEquals(
      differingFields,
      Map(Vector[Any](2) -> List("comment (source=same, target=changed)"))
    )
  }

  test("selectColumnsForHashRefinement keeps only PK and hash-backed columns") {
    import spark.implicits._
    val df = Seq((1, "blob-a", "same", "ignored")).toDF("id", "payload", "comment", "extra")

    val selected = MySQLToScyllaValidator.selectColumnsForHashRefinement(
      df,
      primaryKey        = Seq("id"),
      hashBackedColumns = Seq("payload", "comment")
    )

    assertEquals(selected.columns.toList, List("id", "payload", "comment"))
  }

  test("collectExtraTargetFailureSample only reports target-only keys and honors the limit") {
    import spark.implicits._
    val sourceKeys = Seq((1, "tenant-a"), (2, "tenant-b")).toDF("id", "tenant")
    val targetKeys =
      Seq((1, "tenant-a"), (3, "tenant-c"), (4, "tenant-d")).toDF("id", "tenant")

    val failures = MySQLToScyllaValidator.collectExtraTargetFailureSample(
      sourceKeys,
      targetKeys,
      primaryKeyColumns = Seq("id", "tenant"),
      failuresToFetch   = 1
    )

    assertEquals(failures.length, 1)
    assertEquals(failures.head.otherRepr, None)
    assertEquals(failures.head.items, List(RowComparisonFailure.Item.ExtraTargetRow))
    assert(failures.head.rowRepr.contains("id="))
    assert(failures.head.rowRepr.contains("tenant="))
  }

  test("resolveHashBackedDifferences handles binary primary keys by content") {
    import spark.implicits._
    implicit val sparkSession: SparkSession = spark
    val source = Seq(
      (Array[Byte](1, 2), "blob-a", "same")
    ).toDF("id", "payload", "comment")
    val target = Seq(
      (Array[Byte](1, 2), "blob-a", "changed")
    ).toDF("id", "payload", "comment")
    val requestedPk = Vector[Any](Array[Byte](1, 2))

    val differingFields = MySQLToScyllaValidator.resolveHashBackedDifferences(
      rawSourceDF            = source,
      rawTargetDF            = target,
      primaryKey             = Seq("id"),
      primaryKeyValues       = Seq(requestedPk),
      hashBackedColumns      = Seq("payload", "comment"),
      timestampMsTolerance   = 0L,
      floatingPointTolerance = 0.0
    )(spark)

    assertEquals(
      differingFields,
      Map(
        MySQLToScyllaValidator.normalizePrimaryKeyValues(requestedPk) ->
          List("comment (source=same, target=changed)")
      )
    )
  }

  test("collectFailureSample keeps scanning until it finds a real hash-backed failure") {
    import spark.implicits._
    implicit val sparkSession: SparkSession = spark

    val source = Seq(
      (1, new java.math.BigDecimal("1.0")),
      (2, new java.math.BigDecimal("2.0"))
    ).toDF("id", "amount")
    val target = Seq(
      (1, new java.math.BigDecimal("1.00")),
      (2, new java.math.BigDecimal("3.0"))
    ).toDF("id", "amount")

    val candidates = spark.sparkContext.parallelize(
      Seq[MySQLToScyllaValidator.ValidationCandidate](
        MySQLToScyllaValidator.MatchedRowValidationCandidate(
          sourcePkValues        = Vector[Any](1),
          sourceRepr            = "id=1",
          targetRepr            = "id=1",
          directDifferingFields = Nil,
          hashMismatch          = true
        ),
        MySQLToScyllaValidator.MatchedRowValidationCandidate(
          sourcePkValues        = Vector[Any](2),
          sourceRepr            = "id=2",
          targetRepr            = "id=2",
          directDifferingFields = Nil,
          hashMismatch          = true
        )
      ),
      1
    )

    val failures = MySQLToScyllaValidator.collectFailureSample(
      candidateFailuresRdd   = candidates,
      rawSourceDF            = source,
      rawTargetDF            = target,
      primaryKey             = Seq("id"),
      hashBackedColumns      = Seq("amount"),
      timestampMsTolerance   = 0L,
      floatingPointTolerance = 0.0,
      failuresToFetch        = 1
    )(spark)

    assertEquals(failures.map(_.rowRepr), List("id=2"))
    val differingFields =
      failures.head.items.collectFirst {
        case RowComparisonFailure.Item.DifferingFieldValues(fields) => fields
      }
    assertEquals(differingFields.map(_.size), Some(1))
    assert(differingFields.exists(_.head.startsWith("amount (source=2")))
    assert(differingFields.exists(_.head.contains("target=3")))
  }

  test("collectFailureSample reuses cached hash refinement projections across batches") {
    implicit val sparkSession: SparkSession = spark

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable     = false),
        StructField("payload", StringType, nullable = true)
      )
    )

    val sourceReads = spark.sparkContext.longAccumulator("hash-refinement-source-reads")
    val targetReads = spark.sparkContext.longAccumulator("hash-refinement-target-reads")

    val source = spark.createDataFrame(
      spark.sparkContext
        .parallelize(
          Seq(Row(1, "same"), Row(2, "left")),
          1
        )
        .mapPartitions { rows =>
          sourceReads.add(1L)
          rows
        },
      schema
    )
    val target = spark.createDataFrame(
      spark.sparkContext
        .parallelize(
          Seq(Row(1, "same"), Row(2, "right")),
          1
        )
        .mapPartitions { rows =>
          targetReads.add(1L)
          rows
        },
      schema
    )

    val candidates = spark.sparkContext.parallelize(
      Seq[MySQLToScyllaValidator.ValidationCandidate](
        MySQLToScyllaValidator.MatchedRowValidationCandidate(
          sourcePkValues        = Vector[Any](1),
          sourceRepr            = "id=1",
          targetRepr            = "id=1",
          directDifferingFields = Nil,
          hashMismatch          = true
        ),
        MySQLToScyllaValidator.MatchedRowValidationCandidate(
          sourcePkValues        = Vector[Any](2),
          sourceRepr            = "id=2",
          targetRepr            = "id=2",
          directDifferingFields = Nil,
          hashMismatch          = true
        )
      ),
      1
    )

    val failures = MySQLToScyllaValidator.collectFailureSample(
      candidateFailuresRdd   = candidates,
      rawSourceDF            = source,
      rawTargetDF            = target,
      primaryKey             = Seq("id"),
      hashBackedColumns      = Seq("payload"),
      timestampMsTolerance   = 0L,
      floatingPointTolerance = 0.0,
      failuresToFetch        = 1
    )

    assertEquals(failures.map(_.rowRepr), List("id=2"))
    assert(sourceReads.value == 1L)
    assert(targetReads.value == 1L)
  }
}
