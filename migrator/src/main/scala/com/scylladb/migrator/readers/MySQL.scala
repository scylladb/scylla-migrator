package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ HostValidation, SensitiveKeys, SourceSettings }
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  getZoneId,
  stringToDate,
  stringToTimestamp
}
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }
import org.apache.spark.unsafe.types.UTF8String

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.{ Connection, DriverManager, Types }
import java.util.Locale
import java.util.Properties

import scala.util.{ Try, Using }

object MySQL {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQL")

  val DefaultMaxAllowedPacketBytes: Long = 256 * 1024 * 1024 // 256MB
  val DefaultConnectTimeoutMs: Int = 30000 // 30 seconds
  val DefaultSocketTimeoutMs: Int = 600000 // 10 minutes
  val ContentHashColumn: String = "_content_hash"
  val DefaultConnectionTimeZoneId: String = "UTC"

  /** Maximum allowed fetchSize. Values above this risk OOM errors in the JDBC driver or Spark
    * executors. The recommended range is 1000-10000 for most workloads.
    */
  val MaxFetchSize: Int = 100000

  /** Threshold above which a warning is emitted about potential memory issues with large fetchSize
    * values. This is the top of the recommended range.
    */
  val RecommendedMaxFetchSize: Int = 10000
  private[readers] val DefaultSensitiveRedactionRegex = SensitiveKeys.DefaultRedactionRegex
  private val DigitsOnly = """\d+""".r

  private[readers] sealed trait PartitionColumnType {
    def description: String
  }

  private[readers] object PartitionColumnType {
    case object Numeric extends PartitionColumnType {
      override val description: String = "numeric"
    }
    case object Date extends PartitionColumnType {
      override val description: String = "DATE"
    }
    case object Timestamp extends PartitionColumnType {
      override val description: String = "TIMESTAMP"
    }
  }

  private[readers] case class PartitionColumnMetadata(
    columnName: String,
    jdbcTypeName: String,
    columnType: PartitionColumnType
  )

  private[readers] def isSensitiveOptionKey(key: String): Boolean =
    SensitiveKeys.isSensitiveKey(key)

  private[readers] def redactionRegexCoversKeys(
    regex: String,
    keys: Seq[String]
  ): Boolean = {
    val pattern = regex.r.pattern
    keys.forall(key => pattern.matcher(key).find())
  }

  private[readers] def ensureSensitiveReaderOptionsAreRedacted(
    spark: SparkSession,
    optionKeys: Seq[String],
    context: String
  ): Unit = {
    val sensitiveKeys = optionKeys.distinct.filter(isSensitiveOptionKey)
    if (sensitiveKeys.nonEmpty) {
      val redactionRegex = spark.conf
        .getOption("spark.redaction.regex")
        .filter(_.trim.nonEmpty)
        .getOrElse(DefaultSensitiveRedactionRegex)

      require(
        redactionRegexCoversKeys(redactionRegex, sensitiveKeys),
        s"Refusing to create $context because spark.redaction.regex does not redact all sensitive option keys: ${sensitiveKeys.mkString(", ")}"
      )
    }
  }

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
  ): String = {
    val searchEscape = Option(escape).filter(_.nonEmpty).getOrElse("\\")
    pattern
      .replace(searchEscape, s"${searchEscape}${searchEscape}")
      .replace("%", s"$searchEscape%")
      .replace("_", s"${searchEscape}_")
  }

  private def classifyPartitionColumnType(
    jdbcType: Int,
    jdbcTypeName: String,
    configuredColumn: String
  ): PartitionColumnType =
    jdbcType match {
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT | Types.FLOAT |
          Types.REAL | Types.DOUBLE | Types.NUMERIC | Types.DECIMAL =>
        PartitionColumnType.Numeric
      case Types.DATE =>
        PartitionColumnType.Date
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        PartitionColumnType.Timestamp
      case _ =>
        sys.error(
          s"Partition column '$configuredColumn' has JDBC type '$jdbcTypeName', which Spark JDBC " +
            "does not support for partitioned reads. Use a numeric, DATE, or TIMESTAMP column."
        )
    }

  private def partitionColumnMetadata(
    source: SourceSettings.MySQL,
    configuredColumn: String
  ): PartitionColumnMetadata = {
    val requestedColumn = normalizedPartitionColumnName(configuredColumn)
    withJdbcConnection(source) { connection =>
      val catalog = Option(connection.getCatalog).getOrElse(source.database)
      val tablePattern =
        escapeMetadataPattern(
          source.table,
          Option(connection.getMetaData.getSearchStringEscape).getOrElse("\\")
        )
      Using.resource(connection.getMetaData.getColumns(catalog, null, tablePattern, "%")) {
        resultSet =>
          val columns = Iterator
            .continually(resultSet.next())
            .takeWhile(identity)
            .map { _ =>
              (
                resultSet.getString("COLUMN_NAME"),
                resultSet.getInt("DATA_TYPE"),
                resultSet.getString("TYPE_NAME")
              )
            }
            .toList

          columns
            .find(_._1.equalsIgnoreCase(requestedColumn))
            .map { case (columnName, jdbcType, jdbcTypeName) =>
              PartitionColumnMetadata(
                columnName   = columnName,
                jdbcTypeName = jdbcTypeName,
                columnType   = classifyPartitionColumnType(jdbcType, jdbcTypeName, configuredColumn)
              )
            }
            .getOrElse(
              sys.error(
                s"Partition column '$configuredColumn' was not found in MySQL table " +
                  s"${source.database}.${source.table}. Available columns: " +
                  s"${columns.map(_._1).mkString(", ")}. " +
                  "Check the configured column name and quoting."
              )
            )
      }
    }
  }

  private[readers] def partitionedReadOptions(
    configuredPartitionColumn: String,
    partitionColumnInfo: PartitionColumnMetadata,
    numPartitions: Int,
    lowerBound: SourceSettings.MySQL.PartitionBound,
    upperBound: SourceSettings.MySQL.PartitionBound
  ): Seq[(String, String)] =
    Seq(
      "partitionColumn" -> partitionColumnInfo.columnName,
      "numPartitions"   -> numPartitions.toString,
      "lowerBound"      -> lowerBound.value,
      "upperBound"      -> upperBound.value
    )

  private def parseNumericPartitionBound(
    partitionColumn: String,
    jdbcTypeName: String,
    boundName: String,
    bound: SourceSettings.MySQL.PartitionBound
  ): Long =
    Try(bound.value.toLong).getOrElse(
      sys.error(
        s"Partition column '$partitionColumn' has JDBC type '$jdbcTypeName', so $boundName " +
          s"must be an integer literal. Got '${bound.value}'."
      )
    )

  private def parseTemporalPartitionBound(
    partitionColumn: String,
    jdbcTypeName: String,
    expectedLiteral: String,
    boundName: String,
    bound: SourceSettings.MySQL.PartitionBound,
    parser: String => Option[Long]
  ): Long =
    parser(bound.value).getOrElse(
      sys.error(
        s"Partition column '$partitionColumn' has JDBC type '$jdbcTypeName', so $boundName " +
          s"must be a $expectedLiteral literal. Got '${bound.value}'. " +
          "Epoch-millisecond bounds are not supported for temporal JDBC partition columns."
      )
    )

  private[readers] def validatePartitionBoundsForColumnType(
    partitionColumn: String,
    jdbcTypeName: String,
    columnType: PartitionColumnType,
    lowerBound: SourceSettings.MySQL.PartitionBound,
    upperBound: SourceSettings.MySQL.PartitionBound,
    timeZoneId: String
  ): Unit = {
    val lowerValue = columnType match {
      case PartitionColumnType.Numeric =>
        parseNumericPartitionBound(partitionColumn, jdbcTypeName, "lowerBound", lowerBound)
      case PartitionColumnType.Date =>
        parseTemporalPartitionBound(
          partitionColumn = partitionColumn,
          jdbcTypeName    = jdbcTypeName,
          expectedLiteral = "DATE",
          boundName       = "lowerBound",
          bound           = lowerBound,
          parser          = value => stringToDate(UTF8String.fromString(value)).map(_.toLong)
        )
      case PartitionColumnType.Timestamp =>
        parseTemporalPartitionBound(
          partitionColumn = partitionColumn,
          jdbcTypeName    = jdbcTypeName,
          expectedLiteral = "TIMESTAMP",
          boundName       = "lowerBound",
          bound           = lowerBound,
          parser = value => stringToTimestamp(UTF8String.fromString(value), getZoneId(timeZoneId))
        )
    }

    val upperValue = columnType match {
      case PartitionColumnType.Numeric =>
        parseNumericPartitionBound(partitionColumn, jdbcTypeName, "upperBound", upperBound)
      case PartitionColumnType.Date =>
        parseTemporalPartitionBound(
          partitionColumn = partitionColumn,
          jdbcTypeName    = jdbcTypeName,
          expectedLiteral = "DATE",
          boundName       = "upperBound",
          bound           = upperBound,
          parser          = value => stringToDate(UTF8String.fromString(value)).map(_.toLong)
        )
      case PartitionColumnType.Timestamp =>
        parseTemporalPartitionBound(
          partitionColumn = partitionColumn,
          jdbcTypeName    = jdbcTypeName,
          expectedLiteral = "TIMESTAMP",
          boundName       = "upperBound",
          bound           = upperBound,
          parser = value => stringToTimestamp(UTF8String.fromString(value), getZoneId(timeZoneId))
        )
    }

    if (lowerValue >= upperValue)
      sys.error(
        s"lowerBound ('${lowerBound.value}') must be less than upperBound ('${upperBound.value}') " +
          s"for partition column '$partitionColumn' ($jdbcTypeName)."
      )
  }

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

  private def urlEncode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8)

  private def insecureUrlScheme(value: String): Option[String] = {
    val normalized = value.trim.toLowerCase(Locale.ROOT)
    if (normalized.startsWith("file://")) Some("file")
    else if (normalized.startsWith("http://")) Some("http")
    else None
  }

  private def caseInsensitivePropertyMatches(
    props: Map[String, String],
    propertyName: String
  ): List[(String, String)] =
    props.toList.filter { case (k, _) => k.equalsIgnoreCase(propertyName) }

  private def parseJdbcNumericProperty(
    props: Map[String, String],
    propertyName: String,
    minimum: Long,
    maximum: Long,
    defaultValue: Long,
    expectation: String
  ): Either[String, Long] =
    caseInsensitivePropertyMatches(props, propertyName) match {
      case Nil => Right(defaultValue)
      case List((_, rawValue)) =>
        rawValue match {
          case DigitsOnly() =>
            val parsed = scala.util.Try(rawValue.toLong).getOrElse(Long.MinValue)
            if (parsed < minimum || parsed > maximum)
              Left(
                s"$propertyName must be $expectation, got: '$rawValue'"
              )
            else
              Right(parsed)
          case _ =>
            Left(
              s"$propertyName must be $expectation, got: '$rawValue'"
            )
        }
      case matches =>
        Left(
          s"connectionProperties contains duplicate entries for '$propertyName' with different casing: " +
            matches.map(_._1).mkString(", ")
        )
    }

  private[migrator] def validateConnectionPropertyValues(
    connectionProperties: Option[Map[String, String]]
  ): List[String] = {
    val userProps = connectionProperties.getOrElse(Map.empty)
    List(
      parseJdbcNumericProperty(
        userProps,
        propertyName = "maxAllowedPacket",
        minimum      = 1L,
        maximum      = Long.MaxValue,
        defaultValue = DefaultMaxAllowedPacketBytes,
        expectation  = "a positive integer number of bytes"
      ),
      parseJdbcNumericProperty(
        userProps,
        propertyName = "connectTimeout",
        minimum      = 0L,
        maximum      = Int.MaxValue.toLong,
        defaultValue = DefaultConnectTimeoutMs.toLong,
        expectation = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
      ),
      parseJdbcNumericProperty(
        userProps,
        propertyName = "socketTimeout",
        minimum      = 0L,
        maximum      = Int.MaxValue.toLong,
        defaultValue = DefaultSocketTimeoutMs.toLong,
        expectation = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
      )
    ).collect { case Left(error) => error }
  }

  private[migrator] def validateWhereClause(filter: String): Unit = {
    if (filter.trim.isEmpty)
      sys.error(
        "WHERE clause is empty or blank. Remove the 'where' key or provide a valid filter."
      )
    if (filter.exists(c => c.isControl))
      sys.error(
        s"WHERE clause contains control characters (newlines, null bytes, etc.) which are not allowed. " +
          s"Filter length: ${filter.length} characters"
      )

    val dangerousPattern =
      """\b(drop|delete|truncate|alter|create|exec|execute|union|into|outfile|dumpfile|load_file|benchmark|sleep|grant|revoke)\b""".r
    val filterForScan = stripSqlCommentsAndQuotedText(filter).toLowerCase(Locale.ROOT)
    dangerousPattern.findFirstIn(filterForScan).foreach { keyword =>
      sys.error(
        "WHERE clause contains potentially dangerous SQL keyword(s): " +
          s"matched '$keyword'. " +
          "DDL/DML keywords (DROP, DELETE, TRUNCATE, ALTER, CREATE, EXEC, EXECUTE, UNION, " +
          "INTO, OUTFILE, DUMPFILE, LOAD_FILE, BENCHMARK, SLEEP, GRANT, REVOKE) " +
          "are not allowed in WHERE filters."
      )
    }
  }

  private[readers] def redactedWhereFilterLogMessage(filter: String): String =
    s"Applying configured WHERE filter to MySQL read (content redacted, ${filter.length} characters)"

  private[readers] def partitionedReadConsistencyWarning(
    source: SourceSettings.MySQL
  ): String =
    s"Partitioned MySQL reads for ${source.database}.${source.table} use multiple independent " +
      "JDBC statements/connections and do not provide a single global snapshot across partitions. " +
      "Concurrent source writes can yield mixed-time results. For correctness-sensitive runs, " +
      "quiesce or otherwise freeze the source table before starting the migration or validation."

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
    require(
      source.database.matches("[a-zA-Z0-9_$\\-]+"),
      s"Invalid database name '${source.database}'. " +
        "Must contain only alphanumeric characters, underscores, dollar signs, or hyphens. " +
        "URL-significant characters (/, ?, #, &) are not allowed."
    )
    require(
      source.host.nonEmpty,
      "host must not be empty"
    )
    require(
      HostValidation.isValidHostname(source.host) || HostValidation.isValidIPv6Host(source.host),
      s"Invalid host '${source.host}': must be a hostname, IPv4, or IPv6 address. " +
        "URL metacharacters (/, ?, #, &, @) are not allowed."
    )
    val userProps = source.connectionProperties.getOrElse(Map.empty)
    val maxPacket = parseJdbcNumericProperty(
      userProps,
      propertyName = "maxAllowedPacket",
      minimum      = 1L,
      maximum      = Long.MaxValue,
      defaultValue = DefaultMaxAllowedPacketBytes,
      expectation  = "a positive integer number of bytes"
    ).fold(error => throw new IllegalArgumentException(error), _.toString)
    val connectTimeout = parseJdbcNumericProperty(
      userProps,
      propertyName = "connectTimeout",
      minimum      = 0L,
      maximum      = Int.MaxValue.toLong,
      defaultValue = DefaultConnectTimeoutMs.toLong,
      expectation  = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
    ).fold(error => throw new IllegalArgumentException(error), _.toString)
    val socketTimeout = parseJdbcNumericProperty(
      userProps,
      propertyName = "socketTimeout",
      minimum      = 0L,
      maximum      = Int.MaxValue.toLong,
      defaultValue = DefaultSocketTimeoutMs.toLong,
      expectation  = s"a non-negative integer number of milliseconds between 0 and ${Int.MaxValue}"
    ).fold(error => throw new IllegalArgumentException(error), _.toString)
    val hostPart =
      // Config validation already ensures IPv4 is not wrapped in brackets.
      // Wrap IPv6 addresses (containing colons) in brackets if not already wrapped.
      if (source.host.contains(':') && !source.host.startsWith("["))
        s"[${source.host}]"
      else
        source.host
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
    val query = queryParams
      .map { case (key, value) => s"${urlEncode(key)}=${urlEncode(value)}" }
      .mkString("&")

    s"jdbc:mysql://$hostPart:${source.port}/${source.database}?$query"
  }

  /** Spark treats JDBC property map entries as data-source options before creating driver
    * connection properties. Keep this denylist aligned with Spark JDBCOptions so user-supplied
    * connectionProperties cannot override the validated read path or execute session SQL.
    */
  private val ReservedJdbcKeys =
    Set(
      "url",
      "dbtable",
      "query",
      "driver",
      "partitioncolumn",
      "lowerbound",
      "upperbound",
      "numpartitions",
      "querytimeout",
      "fetchsize",
      "truncate",
      "cascadetruncate",
      "createtableoptions",
      "createtablecolumntypes",
      "customschema",
      "batchsize",
      "isolationlevel",
      "sessioninitstatement",
      "pushdownpredicate",
      "pushdownaggregate",
      "pushdownlimit",
      "pushdownoffset",
      "pushdowntablesample",
      "keytab",
      "principal",
      "tablecomment",
      "refreshkrb5config",
      "connectionprovider",
      "preparequery",
      "prefertimestampntz",
      "hint",
      "user",
      "password"
    )

  /** JDBC properties that could be exploited to read local files, enable deserialization attacks,
    * or otherwise compromise security. These are blocked even when supplied via
    * connectionProperties.
    *
    * Note: TLS key-store properties (`trustcertificatekeystoreurl`, `clientcertificatekeystoreurl`,
    * etc.) are intentionally NOT blocked here. They are standard MySQL Connector/J 9.x TLS
    * configuration knobs required for verified TLS connections and do not introduce the
    * local-file-read or deserialization attack vectors present in the properties listed below.
    *
    * This list was validated against MySQL Connector/J 8.x and 9.x documentation. It should be
    * reviewed when upgrading to new major versions of the MySQL JDBC driver.
    */
  private[migrator] val DangerousJdbcKeys = Set(
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
    * via connectionProperties they would be silently ignored because MySQL Connector/J gives URL
    * parameters precedence over Properties. Filter them with a warning.
    */
  private val UrlEmbeddedJdbcKeys =
    Set(
      "maxallowedpacket",
      "usecursorfetch",
      "zerodatetimebehavior",
      "tinyint1isbit",
      "connecttimeout",
      "sockettimeout",
      "connectiontimezone",
      "forceconnectiontimezonetosession"
    )

  private[migrator] def safeConnectionProperties(
    source: SourceSettings.MySQL
  ): Map[String, String] =
    source.connectionProperties.getOrElse(Map.empty).flatMap { case (k, v) =>
      val lower = k.toLowerCase(Locale.ROOT)
      if (ReservedJdbcKeys.contains(lower)) {
        log.warn(
          s"Ignoring reserved connectionProperty '$k' – this key is managed by the migrator"
        )
        None
      } else if (DangerousJdbcKeys.contains(lower)) {
        log.warn(
          s"Ignoring dangerous connectionProperty '$k' – this property is blocked for security"
        )
        None
      } else if (lower == "zerodatetimebehavior") {
        log.warn(
          s"Ignoring connectionProperty '$k' – use source.zeroDateTimeBehavior instead of connectionProperties"
        )
        None
      } else if (UrlEmbeddedJdbcKeys.contains(lower)) {
        log.warn(
          s"Ignoring connectionProperty '$k' – this property is already set in the JDBC URL"
        )
        None
      } else {
        // Warn about potentially insecure TLS keystore URLs
        if ((lower.contains("keystore") || lower.contains("cert")) && lower.contains("url")) {
          insecureUrlScheme(v).foreach { scheme =>
            log.warn(
              s"SECURITY: connectionProperty '$k' uses a potentially insecure URL scheme '$scheme'. " +
                "Verify this is from a trusted source."
            )
          }
        }
        Some(k -> v)
      }
    }

  private[migrator] def jdbcConnectionProperties(source: SourceSettings.MySQL): Properties = {
    val props = new Properties()
    props.setProperty("user", source.credentials.username)
    props.setProperty("password", source.credentials.password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    safeConnectionProperties(source).foreach { case (k, v) =>
      props.setProperty(k, v)
    }
    props
  }

  private[migrator] def withJdbcConnection[A](source: SourceSettings.MySQL)(
    f: Connection => A
  ): A = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    Using.resource(
      DriverManager.getConnection(buildJdbcUrl(source), jdbcConnectionProperties(source))
    )(
      f
    )
  }

  private[migrator] def jdbcReadProperties(source: SourceSettings.MySQL): Properties = {
    val props = jdbcConnectionProperties(source)
    props.setProperty("fetchsize", source.fetchSize.toString)
    props
  }

  private def partitionedJdbcReader(
    spark: SparkSession,
    source: SourceSettings.MySQL,
    jdbcUrl: String,
    tableExpression: String
  ): DataFrameReader = {
    ensureSensitiveReaderOptionsAreRedacted(
      spark,
      Seq("user", "password", "driver") ++ safeConnectionProperties(source).keys.toSeq,
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

    safeConnectionProperties(source).foldLeft(base) { case (reader, (k, v)) =>
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

    log.info(s"Connecting to MySQL at ${source.host}:${source.port}/${source.database}")
    log.info(s"Reading table: ${source.table}")
    log.info(s"JDBC useCursorFetch=true, fetchSize=${source.fetchSize}")
    if (source.zeroDateTimeBehavior != SourceSettings.MySQL.ZeroDateTimeBehavior.Exception)
      log.warn(
        s"MySQL zeroDateTimeBehavior=${source.zeroDateTimeBehavior.jdbcValue} is an explicit opt-in " +
          "that changes how invalid zero dates are read. Continue only if that behavior is intentional."
      )
    if (source.fetchSize > RecommendedMaxFetchSize)
      log.warn(
        s"fetchSize (${source.fetchSize}) exceeds the recommended maximum of $RecommendedMaxFetchSize. " +
          "For tables with large rows (TEXT/BLOB columns) this may cause excessive memory usage " +
          "per JDBC connection. Consider lowering fetchSize to the 1000-10000 range."
      )

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
    val readProperties = jdbcReadProperties(source)
    ensureSensitiveReaderOptionsAreRedacted(
      spark,
      readProperties.stringPropertyNames().toArray(new Array[String](0)).toSeq,
      "MySQL JDBC reader"
    )

    (source.partitionColumn, source.numPartitions) match {
      case (Some(col), Some(n)) =>
        (source.lowerBound, source.upperBound) match {
          case (Some(lower), Some(upper)) =>
            val partitionColumnInfo = partitionColumnMetadata(source, col)
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
              .foldLeft(partitionedJdbcReader(spark, source, jdbcUrl, tableExpression)) {
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
