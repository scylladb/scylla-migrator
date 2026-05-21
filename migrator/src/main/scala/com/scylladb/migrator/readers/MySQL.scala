package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ SensitiveKeys, SourceSettings, SparkSecretRedaction }
import com.scylladb.migrator.readers.jdbc.{
  JdbcConsistencyWarning,
  JdbcFetchSize,
  JdbcMetadata,
  JdbcNumericProperty,
  JdbcPartitionBounds,
  JdbcSafeProperties,
  JdbcUrl,
  JdbcWhereFilter
}
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }

import java.sql.{ Connection, DriverManager }
import java.util.Properties

import scala.util.Using

object MySQL {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQL")

  val DefaultMaxAllowedPacketBytes: Long = 256 * 1024 * 1024 // 256MB
  val DefaultConnectTimeoutMs: Int = 30000 // 30 seconds
  val DefaultSocketTimeoutMs: Int = 600000 // 10 minutes
  val ContentHashColumn: String = "_content_hash"
  val DefaultConnectionTimeZoneId: String = "UTC"

  /** Maximum allowed fetchSize. Re-exported from [[JdbcFetchSize]] so existing callers (e.g.
    * `SourceSettings.MySQL.validate`) keep working without an import change.
    */
  val MaxFetchSize: Int = JdbcFetchSize.MaxFetchSize

  /** Threshold above which a warning is emitted about potential memory issues with large fetchSize
    * values. This is the top of the recommended range.
    */
  val RecommendedMaxFetchSize: Int = JdbcFetchSize.RecommendedMaxFetchSize

  private[readers] val DefaultSensitiveRedactionRegex = SensitiveKeys.DefaultRedactionRegex

  // Backward-compatible type aliases for code that referenced `MySQL.PartitionColumnType.*` and
  // `MySQL.PartitionColumnMetadata(...)`. The canonical definitions now live in
  // `readers.jdbc.JdbcPartitionBounds` so other JDBC backends can share them.
  private[readers] type PartitionColumnType = JdbcPartitionBounds.PartitionColumnType
  private[readers] val PartitionColumnType: JdbcPartitionBounds.PartitionColumnType.type =
    JdbcPartitionBounds.PartitionColumnType

  private[readers] type PartitionColumnMetadata = JdbcPartitionBounds.PartitionColumnMetadata
  private[readers] val PartitionColumnMetadata: JdbcPartitionBounds.PartitionColumnMetadata.type =
    JdbcPartitionBounds.PartitionColumnMetadata

  private[readers] def isSensitiveOptionKey(key: String): Boolean =
    SensitiveKeys.isSensitiveKey(key)

  private[readers] def redactionRegexCoversKeys(
    regex: String,
    keys: Seq[String]
  ): Boolean =
    SparkSecretRedaction.redactionRegexCoversKeys(regex, keys)

  private[readers] def ensureSensitiveReaderOptionsAreRedacted(
    spark: SparkSession,
    optionKeys: Seq[String],
    context: String
  ): Unit =
    SparkSecretRedaction.ensureKeysRedacted(spark, optionKeys, context)

  // ----- Numeric property specs -----

  private val MaxAllowedPacketSpec: JdbcNumericProperty.Spec =
    JdbcNumericProperty.Spec(
      name         = "maxAllowedPacket",
      minimum      = 1L,
      maximum      = Long.MaxValue,
      defaultValue = DefaultMaxAllowedPacketBytes,
      expectation  = "a positive integer number of bytes"
    )

  private val ConnectTimeoutSpec: JdbcNumericProperty.Spec =
    JdbcNumericProperty.Spec(
      name         = "connectTimeout",
      minimum      = 0L,
      maximum      = Int.MaxValue.toLong,
      defaultValue = DefaultConnectTimeoutMs.toLong,
      expectation  = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
    )

  private val SocketTimeoutSpec: JdbcNumericProperty.Spec =
    JdbcNumericProperty.Spec(
      name         = "socketTimeout",
      minimum      = 0L,
      maximum      = Int.MaxValue.toLong,
      defaultValue = DefaultSocketTimeoutMs.toLong,
      expectation  = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
    )

  private val NumericPropertySpecs: Seq[JdbcNumericProperty.Spec] =
    Seq(MaxAllowedPacketSpec, ConnectTimeoutSpec, SocketTimeoutSpec)

  private def normalizedPartitionColumnName(column: String): String =
    if (column.startsWith("`") && column.endsWith("`"))
      column.substring(1, column.length - 1).replace("``", "`")
    else
      column

  /** Escape metadata-pattern wildcard characters before calling `getColumns`. MySQL's metadata
    * pattern matching treats `%` and `_` as wildcards, so literal table names containing those
    * characters must be escaped or resolved metadata lookup may unexpectedly match sibling tables.
    */
  private[migrator] def escapeMetadataPattern(
    pattern: String,
    escape: String
  ): String =
    JdbcMetadata.escapeMetadataPattern(pattern, escape)

  private def classifyPartitionColumnType(
    jdbcType: Int,
    jdbcTypeName: String,
    configuredColumn: String
  ): PartitionColumnType =
    JdbcPartitionBounds.classify(jdbcType, jdbcTypeName, configuredColumn)

  private def partitionColumnMetadata(
    source: SourceSettings.MySQL,
    configuredColumn: String,
    safeProps: Map[String, String] = Map.empty
  ): PartitionColumnMetadata = {
    val requestedColumn = normalizedPartitionColumnName(configuredColumn)
    withJdbcConnection(source, safeProps) { connection =>
      val catalog = Option(connection.getCatalog).getOrElse(source.database)
      JdbcMetadata.resolvePartitionColumn(
        connection       = connection,
        catalog          = catalog,
        table            = source.table,
        configuredColumn = configuredColumn,
        normalizedColumn = requestedColumn,
        onMissing = columnNames =>
          sys.error(
            s"Partition column '$configuredColumn' was not found in MySQL table " +
              s"${source.database}.${source.table}. Available columns: " +
              s"${columnNames.mkString(", ")}. " +
              "Check the configured column name and quoting."
          ),
        classifier = (columnName, jdbcType, jdbcTypeName) =>
          PartitionColumnMetadata(
            columnName   = columnName,
            jdbcTypeName = jdbcTypeName,
            columnType   = classifyPartitionColumnType(jdbcType, jdbcTypeName, configuredColumn)
          )
      )
    }
  }

  private[readers] def partitionedReadOptions(
    configuredPartitionColumn: String,
    partitionColumnInfo: PartitionColumnMetadata,
    numPartitions: Int,
    lowerBound: SourceSettings.MySQL.PartitionBound,
    upperBound: SourceSettings.MySQL.PartitionBound
  ): Seq[(String, String)] = {
    val _ = configuredPartitionColumn // retained to keep the public signature stable
    Seq(
      "partitionColumn" -> partitionColumnInfo.columnName,
      "numPartitions"   -> numPartitions.toString,
      "lowerBound"      -> lowerBound.value,
      "upperBound"      -> upperBound.value
    )
  }

  private[readers] def validatePartitionBoundsForColumnType(
    partitionColumn: String,
    jdbcTypeName: String,
    columnType: PartitionColumnType,
    lowerBound: SourceSettings.MySQL.PartitionBound,
    upperBound: SourceSettings.MySQL.PartitionBound,
    timeZoneId: String
  ): Unit =
    JdbcPartitionBounds.validateBounds(
      partitionColumn = partitionColumn,
      jdbcTypeName    = jdbcTypeName,
      columnType      = columnType,
      lowerBound      = lowerBound.value,
      upperBound      = upperBound.value,
      timeZoneId      = timeZoneId
    )

  /** Escape a SQL identifier for use in backtick-quoted MySQL syntax. Doubles any embedded backtick
    * characters so that e.g. a table named `foo`` ` becomes `` `foo``` `` `.
    */
  private[readers] def escapeIdentifier(name: String): String = {
    require(name.nonEmpty, "Identifier must not be empty")
    s"`${name.replace("`", "``")}`"
  }

  /** Strip SQL block comments and single-line comments from a string. This prevents keyword
    * obfuscation like `UNI&#47;**&#47;ON` (which MySQL treats as `UNION`) from bypassing the
    * dangerous keyword regex.
    */
  private[readers] def stripSqlComments(sql: String): String =
    sql
      .replaceAll("""/\*.*?\*/""", "") // block comments /* ... */
      .replaceAll("""--(?=[\s\p{Cntrl}])[^\r\n]*""", " ") // MySQL single-line comments -- ...

  private def isMySqlLineCommentStart(sql: String, dashIndex: Int): Boolean =
    dashIndex + 2 < sql.length &&
      sql.charAt(dashIndex) == '-' &&
      sql.charAt(dashIndex + 1) == '-' && {
        val next = sql.charAt(dashIndex + 2)
        next.isWhitespace || next.isControl
      }

  /** Replace the contents of quoted SQL strings and quoted identifiers with spaces so that
    * subsequent keyword checks only inspect SQL structure and not literal data.
    */
  private[readers] def stripQuotedSqlText(sql: String): String = {
    val out = new StringBuilder(sql.length)
    var i = 0
    var quoteChar: Option[Char] = None

    while (i < sql.length) {
      val c = sql.charAt(i)
      quoteChar match {
        case None =>
          if (c == '\'' || c == '"' || c == '`') {
            quoteChar = Some(c)
            out.append(' ')
            i += 1
          } else {
            out.append(c)
            i += 1
          }

        case Some('\'') | Some('"') =>
          val activeQuote = quoteChar.get
          if (c == '\\' && i + 1 < sql.length) {
            out.append("  ")
            i += 2
          } else if (c == activeQuote && i + 1 < sql.length && sql.charAt(i + 1) == activeQuote) {
            out.append("  ")
            i += 2
          } else if (c == activeQuote) {
            quoteChar = None
            out.append(' ')
            i += 1
          } else {
            out.append(' ')
            i += 1
          }

        case Some('`') =>
          if (c == '`' && i + 1 < sql.length && sql.charAt(i + 1) == '`') {
            out.append("  ")
            i += 2
          } else if (c == '`') {
            quoteChar = None
            out.append(' ')
            i += 1
          } else {
            out.append(' ')
            i += 1
          }

        case Some(other) =>
          throw new IllegalStateException(s"Unexpected quote character '$other'")
      }
    }

    out.toString
  }

  /** Strip comments and quoted SQL text in one pass. Keeping comment and quote state together
    * prevents quotes inside comments from hiding later executable comments or dangerous keywords.
    * Backslash escapes are rejected because their behavior depends on MySQL sql_mode.
    */
  private[readers] def stripSqlCommentsAndQuotedText(sql: String): String = {
    val out = new StringBuilder(sql.length)
    var i = 0

    def rejectExecutableComment(): Nothing =
      sys.error(
        "WHERE clause contains MySQL executable comments (`/*!...*/`), which are not allowed " +
          "in WHERE filters."
      )

    def rejectUnterminated(kind: String): Nothing =
      sys.error(s"WHERE clause contains unterminated $kind, which is not allowed in WHERE filters.")

    def rejectBackslashEscape(): Nothing =
      sys.error(
        "WHERE clause contains backslash escapes, which are not allowed because MySQL " +
          "NO_BACKSLASH_ESCAPES mode changes how quoted strings are parsed."
      )

    while (i < sql.length) {
      val c = sql.charAt(i)
      if (c == '\'' || c == '"' || c == '`') {
        val quoteChar = c
        out.append(' ')
        i += 1

        var closed = false
        while (i < sql.length && !closed) {
          val quoted = sql.charAt(i)
          if (quoteChar != '`' && quoted == '\\') {
            rejectBackslashEscape()
          } else if (quoted == quoteChar && i + 1 < sql.length && sql.charAt(i + 1) == quoteChar) {
            out.append("  ")
            i += 2
          } else if (quoted == quoteChar) {
            out.append(' ')
            i += 1
            closed = true
          } else {
            out.append(' ')
            i += 1
          }
        }

        if (!closed)
          rejectUnterminated("quoted SQL text")
      } else if (c == '/' && i + 1 < sql.length && sql.charAt(i + 1) == '*') {
        if (i + 2 < sql.length && sql.charAt(i + 2) == '!')
          rejectExecutableComment()

        i += 2
        var closed = false
        while (i + 1 < sql.length && !closed)
          if (sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
            i += 2
            closed = true
          } else {
            i += 1
          }

        if (!closed)
          rejectUnterminated("SQL block comment")
      } else if (isMySqlLineCommentStart(sql, i)) {
        i += 2
        while (i < sql.length && sql.charAt(i) != '\r' && sql.charAt(i) != '\n')
          i += 1
      } else {
        out.append(c)
        i += 1
      }
    }

    out.toString
  }

  private[migrator] def validateConnectionPropertyValues(
    connectionProperties: Option[Map[String, String]]
  ): List[String] =
    JdbcNumericProperty.validateAll(connectionProperties, NumericPropertySpecs)

  private[migrator] def validateWhereClause(filter: String): Unit = {
    JdbcWhereFilter.requireNonBlank(filter)
    JdbcWhereFilter.requireNoControlCharacters(filter)
    JdbcWhereFilter.validateKeywords(
      filter           = filter,
      dangerousPattern = MySqlDangerousKeywordPattern,
      stripDialect     = stripSqlCommentsAndQuotedText,
      dialectName      = "MySQL"
    )
  }

  /** MySQL-specific DDL/DML keyword regex; used by [[validateWhereClause]] via the pluggable
    * [[JdbcWhereFilter.validateKeywords]] scan. Future backends define their own pattern.
    */
  private val MySqlDangerousKeywordPattern: scala.util.matching.Regex =
    """\b(drop|delete|truncate|alter|create|exec|execute|union|into|outfile|dumpfile|load_file|benchmark|sleep|grant|revoke)\b""".r

  private[readers] def redactedWhereFilterLogMessage(filter: String): String =
    JdbcWhereFilter.redactedLogMessage("MySQL", filter)

  private[readers] def partitionedReadConsistencyWarning(
    source: SourceSettings.MySQL
  ): String =
    JdbcConsistencyWarning.partitionedReadWarning(
      backendName    = "MySQL",
      qualifiedTable = s"${source.database}.${source.table}"
    )

  def readDataframe(spark: SparkSession, source: SourceSettings.MySQL): SourceDataFrame = {
    val df = readDataframeRaw(spark, source)
    MySQLSchemaLogger.logSchemaInfo(df)
    log.info(
      s"Number of partitions: ${df.queryExecution.executedPlan.execute().getNumPartitions}"
    )
    sourceDataFrame(df)
  }

  private[readers] def sourceDataFrame(df: DataFrame): SourceDataFrame =
    SourceDataFrame(df, timestampColumns = None, savepointsSupported = false)

  private[readers] def buildJdbcUrl(
    source: SourceSettings.MySQL,
    connectionTimeZoneId: String = DefaultConnectionTimeZoneId
  ): String = {
    JdbcUrl.requireSimpleDatabaseName(source.database)
    JdbcUrl.requireValidHost(source.host)
    val userProps = source.connectionProperties.getOrElse(Map.empty)
    val maxPacket = JdbcNumericProperty.parseOrThrow(userProps, MaxAllowedPacketSpec).toString
    val connectTimeout =
      JdbcNumericProperty.parseOrThrow(userProps, ConnectTimeoutSpec).toString
    val socketTimeout =
      JdbcNumericProperty.parseOrThrow(userProps, SocketTimeoutSpec).toString
    val hostPart = JdbcUrl.bracketIPv6Host(source.host)
    val queryParams = List(
      "zeroDateTimeBehavior"             -> source.zeroDateTimeBehavior.jdbcValue,
      "tinyInt1IsBit"                    -> "false",
      "maxAllowedPacket"                 -> maxPacket,
      "useCursorFetch"                   -> "true",
      "connectTimeout"                   -> connectTimeout,
      "socketTimeout"                    -> socketTimeout,
      "connectionTimeZone"               -> connectionTimeZoneId,
      "forceConnectionTimeZoneToSession" -> "true"
    )
    s"jdbc:mysql://$hostPart:${source.port}/${source.database}?${JdbcUrl.encodeQueryParams(queryParams)}"
  }

  /** JDBC properties that could be exploited to read local files, enable deserialization attacks,
    * or otherwise compromise security. These are blocked even when supplied via
    * `connectionProperties`.
    *
    * Note: TLS key-store properties (`trustcertificatekeystoreurl`, `clientcertificatekeystoreurl`,
    * etc.) are intentionally NOT blocked here. They are standard MySQL Connector/J 9.x TLS
    * configuration knobs required for verified TLS connections and do not introduce the
    * local-file-read or deserialization attack vectors present in the properties listed below.
    *
    * This list was validated against MySQL Connector/J 8.x and 9.x documentation. It should be
    * reviewed when upgrading to new major versions of the MySQL JDBC driver.
    */
  private[migrator] val DangerousJdbcKeys: Set[String] = Set(
    "allowloadlocalinfile",
    "allowurlinlocalinfile",
    "allowloadlocalinfileinpath",
    "autodeserialize",
    "allowpublickeyretrieval",
    "serverrsapublickeyfile",
    "queryinterceptors",
    "exceptioninterceptors",
    "connectionlifecycleinterceptors",
    "authenticationplugins",
    "propertiestransform",
    "socketfactory",
    "autogeneratetestcasescript",
    "statementinterceptors",
    "connectionpropertiesloadbalancer",
    "allowmultiqueries",
    "profilereventhandler",
    "clientinfoprovider",
    "logger",
    "sessionvariables"
  )

  /** JDBC properties that are already embedded in the JDBC URL by [[buildJdbcUrl]]. If specified
    * via `connectionProperties` they would be silently ignored because MySQL Connector/J gives URL
    * parameters precedence over `Properties`. Filter them with a warning.
    */
  private val UrlEmbeddedJdbcKeys: Set[String] = Set(
    "maxallowedpacket",
    "usecursorfetch",
    "zerodatetimebehavior",
    "tinyint1isbit",
    "connecttimeout",
    "sockettimeout",
    "connectiontimezone",
    "forceconnectiontimezonetosession"
  )

  /** Per-key MySQL guidance attached when an URL-embedded property is supplied. The key match is
    * case-insensitive (see [[JdbcSafeProperties.classifyKey]]) so the dispatch survives the same
    * Turkish-İ bypass that the dangerous-key blocklist defends against.
    */
  private val UrlEmbeddedJdbcGuidance: Map[String, String] = Map(
    "zeroDateTimeBehavior" -> "use source.zeroDateTimeBehavior instead of connectionProperties"
  )

  private[migrator] def safeConnectionProperties(
    source: SourceSettings.MySQL
  ): Map[String, String] =
    source.connectionProperties.getOrElse(Map.empty).flatMap { case (k, v) =>
      JdbcSafeProperties.classifyKey(
        k,
        DangerousJdbcKeys,
        UrlEmbeddedJdbcKeys,
        UrlEmbeddedJdbcGuidance
      ) match {
        case JdbcSafeProperties.KeyClassification.Reserved =>
          log.warn(
            s"Ignoring reserved connectionProperty '$k' – this key is managed by the migrator"
          )
          None
        case JdbcSafeProperties.KeyClassification.Dangerous =>
          log.warn(
            s"Ignoring dangerous connectionProperty '$k' – this property is blocked for security"
          )
          None
        case JdbcSafeProperties.KeyClassification.UrlEmbedded(guidance) =>
          val tail = guidance.getOrElse("this property is already set in the JDBC URL")
          log.warn(s"Ignoring connectionProperty '$k' – $tail")
          None
        case JdbcSafeProperties.KeyClassification.Allowed =>
          if (JdbcSafeProperties.looksLikeTlsKeystoreUrlKey(k)) {
            JdbcSafeProperties.insecureUrlScheme(v).foreach { scheme =>
              log.warn(
                s"SECURITY: connectionProperty '$k' uses a potentially insecure URL scheme '$scheme'. " +
                  "Verify this is from a trusted source."
              )
            }
          }
          Some(k -> v)
      }
    }

  /** Build the JDBC `Properties` payload for a MySQL connection. `safeProps` defaults to
    * `safeConnectionProperties(source)` for ad-hoc callers; the partitioned-read path computes
    * `safeProps` once in `readDataframeRaw` and threads it through here to avoid re-emitting
    * "Ignoring..." warnings for every blocked key.
    */
  private[migrator] def jdbcConnectionProperties(
    source: SourceSettings.MySQL,
    safeProps: Map[String, String] = Map.empty,
    computeSafePropsIfEmpty: Boolean = true
  ): Properties = {
    val resolvedSafeProps =
      if (safeProps.nonEmpty || !computeSafePropsIfEmpty) safeProps
      else safeConnectionProperties(source)
    val props = new Properties()
    props.setProperty("user", source.credentials.username)
    props.setProperty("password", source.credentials.password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    resolvedSafeProps.foreach { case (k, v) =>
      props.setProperty(k, v)
    }
    props
  }

  private[migrator] def withJdbcConnection[A](
    source: SourceSettings.MySQL,
    safeProps: Map[String, String] = Map.empty
  )(f: Connection => A): A = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    Using.resource(
      DriverManager.getConnection(
        buildJdbcUrl(source),
        jdbcConnectionProperties(source, safeProps)
      )
    )(
      f
    )
  }

  private[migrator] def jdbcReadProperties(
    source: SourceSettings.MySQL,
    safeProps: Map[String, String] = Map.empty,
    computeSafePropsIfEmpty: Boolean = true
  ): Properties = {
    val props = jdbcConnectionProperties(source, safeProps, computeSafePropsIfEmpty)
    props.setProperty("fetchsize", source.fetchSize.toString)
    props
  }

  private def partitionedJdbcReader(
    spark: SparkSession,
    source: SourceSettings.MySQL,
    jdbcUrl: String,
    tableExpression: String,
    safeProps: Map[String, String]
  ): DataFrameReader = {
    ensureSensitiveReaderOptionsAreRedacted(
      spark,
      Seq("user", "password", "driver") ++ safeProps.keys.toSeq,
      "partitioned MySQL JDBC reader"
    )
    val base = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableExpression)
      .option("user", source.credentials.username)
      .option("password", source.credentials.password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("fetchsize", source.fetchSize)

    safeProps.foldLeft(base) { case (reader, (k, v)) =>
      reader.option(k, v)
    }
  }

  private def readDataframeRaw(
    spark: SparkSession,
    source: SourceSettings.MySQL
  ): DataFrame = {
    // Guard against programmatic misuse where the config decoder is bypassed.
    val errors = SourceSettings.MySQL.validate(source)
    require(errors.isEmpty, errors.mkString("; "))

    // Compute once: each call to safeConnectionProperties emits log.warn for every blocked key.
    // Hoisting eliminates 2-3 duplicate warnings per blocked key per partitioned-read setup.
    val safeProps = safeConnectionProperties(source)

    log.info(s"Connecting to MySQL at ${source.host}:${source.port}/${source.database}")
    log.info(s"Reading table: ${source.table}")
    log.info(s"JDBC useCursorFetch=true, fetchSize=${source.fetchSize}")
    if (source.zeroDateTimeBehavior != SourceSettings.MySQL.ZeroDateTimeBehavior.Exception)
      log.warn(
        s"MySQL zeroDateTimeBehavior=${source.zeroDateTimeBehavior.jdbcValue} is an explicit opt-in " +
          "that changes how invalid zero dates are read. Continue only if that behavior is intentional."
      )
    if (source.fetchSize > RecommendedMaxFetchSize)
      log.warn(JdbcFetchSize.aboveRecommendedWarning(source.fetchSize))

    val tableExpression = source.where match {
      case Some(filter) if filter.trim.nonEmpty =>
        validateWhereClause(filter)
        // Note: the `where` value is treated as trusted input because the config author
        // already has database credentials. No SQL sanitization is applied.
        log.debug(
          "The 'where' field is passed directly to MySQL as SQL without sanitization. " +
            "Ensure the configuration file is not writable by untrusted principals."
        )
        log.info(redactedWhereFilterLogMessage(filter))
        s"(SELECT * FROM ${escapeIdentifier(source.table)} WHERE $filter) AS filtered_table"
      case Some(_) =>
        sys.error(
          "WHERE clause is empty or blank. Remove the 'where' key or provide a valid filter."
        )
      case None => escapeIdentifier(source.table)
    }

    val jdbcUrl =
      buildJdbcUrl(source, connectionTimeZoneId = spark.sessionState.conf.sessionLocalTimeZone)
    val readProperties = jdbcReadProperties(source, safeProps)
    ensureSensitiveReaderOptionsAreRedacted(
      spark,
      readProperties.stringPropertyNames().toArray(new Array[String](0)).toSeq,
      "MySQL JDBC reader"
    )

    (source.partitionColumn, source.numPartitions) match {
      case (Some(col), Some(n)) =>
        (source.lowerBound, source.upperBound) match {
          case (Some(lower), Some(upper)) =>
            val partitionColumnInfo = partitionColumnMetadata(source, col, safeProps)
            validatePartitionBoundsForColumnType(
              partitionColumn = partitionColumnInfo.columnName,
              jdbcTypeName    = partitionColumnInfo.jdbcTypeName,
              columnType      = partitionColumnInfo.columnType,
              lowerBound      = lower,
              upperBound      = upper,
              timeZoneId      = spark.sessionState.conf.sessionLocalTimeZone
            )
            log.info(
              s"Using partitioned read: column=$col, jdbcType=${partitionColumnInfo.jdbcTypeName}, " +
                s"partitions=$n, lowerBound=${lower.value}, upperBound=${upper.value}"
            )
            log.warn(partitionedReadConsistencyWarning(source))
            partitionedReadOptions(col, partitionColumnInfo, n, lower, upper)
              .foldLeft(partitionedJdbcReader(spark, source, jdbcUrl, tableExpression, safeProps)) {
                case (reader, (key, value)) => reader.option(key, value)
              }
              .load()
          case _ =>
            sys.error(
              s"Both lowerBound and upperBound must be set when using partitioned reads " +
                s"(partitionColumn='$col', numPartitions=$n). " +
                s"Please set lowerBound and upperBound to the MIN/MAX range of '$col' in the table."
            )
        }
      case (Some(col), None) =>
        sys.error(
          s"partitionColumn '$col' specified but numPartitions is missing. " +
            "Both partitionColumn and numPartitions must be set together for partitioned reads."
        )
      case (None, Some(n)) =>
        sys.error(
          s"numPartitions ($n) specified but partitionColumn is missing. " +
            "Both partitionColumn and numPartitions must be set together for partitioned reads."
        )
      case _ =>
        log.warn(
          "No partitioning configured. This will read the entire table in a single partition. " +
            "For large tables, set partitionColumn, numPartitions, lowerBound, and upperBound for parallel reads."
        )
        spark.read.jdbc(jdbcUrl, tableExpression, readProperties)
    }
  }
}
