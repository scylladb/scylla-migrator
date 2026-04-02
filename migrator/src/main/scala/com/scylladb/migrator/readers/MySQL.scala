package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ HostValidation, SourceSettings }
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }

object MySQL {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQL")

  val DefaultMaxAllowedPacketBytes: Long = 256 * 1024 * 1024 // 256MB
  val DefaultConnectTimeoutMs: Int = 30000 // 30 seconds
  val DefaultSocketTimeoutMs: Int = 600000 // 10 minutes
  val ContentHashColumn: String = "_content_hash"

  /** Maximum allowed fetchSize. Values above this risk OOM errors in the JDBC driver or Spark
    * executors. The recommended range is 1000-10000 for most workloads.
    */
  val MaxFetchSize: Int = 100000

  /** Threshold above which a warning is emitted about potential memory issues with large fetchSize
    * values. This is the top of the recommended range.
    */
  val RecommendedMaxFetchSize: Int = 10000

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
      .replaceAll("""/\*.*?\*/""", " ") // block comments /* ... */
      .replaceAll("""--[^\r\n]*""", " ") // single-line comments -- ...

  def readDataframe(spark: SparkSession, source: SourceSettings.MySQL): SourceDataFrame = {
    val df = readDataframeRaw(spark, source)
    MySQLSchemaLogger.logSchemaInfo(df)
    log.info(
      s"Number of partitions: ${df.queryExecution.executedPlan.execute().getNumPartitions}"
    )
    SourceDataFrame(df, timestampColumns = None, savepointsSupported = false)
  }

  private[readers] def buildJdbcUrl(source: SourceSettings.MySQL): String = {
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
    val maxPacket = userProps
      .find(_._1.equalsIgnoreCase("maxAllowedPacket"))
      .map(_._2)
      .getOrElse(DefaultMaxAllowedPacketBytes.toString)
    require(
      {
        val parsed = scala.util.Try(maxPacket.toLong).getOrElse(-1L)
        parsed > 0
      },
      s"maxAllowedPacket must be a positive number, got: '$maxPacket'"
    )
    val connectTimeout = userProps
      .find(_._1.equalsIgnoreCase("connectTimeout"))
      .map(_._2)
      .getOrElse(DefaultConnectTimeoutMs.toString)
    val socketTimeout = userProps
      .find(_._1.equalsIgnoreCase("socketTimeout"))
      .map(_._2)
      .getOrElse(DefaultSocketTimeoutMs.toString)
    val hostPart =
      // Config validation already ensures IPv4 is not wrapped in brackets.
      // Wrap IPv6 addresses (containing colons) in brackets if not already wrapped.
      if (source.host.contains(':') && !source.host.startsWith("["))
        s"[${source.host}]"
      else
        source.host
    s"jdbc:mysql://$hostPart:${source.port}/${source.database}" +
      s"?zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1IsBit=false" +
      s"&maxAllowedPacket=$maxPacket&useCursorFetch=true" +
      s"&connectTimeout=$connectTimeout&socketTimeout=$socketTimeout"
  }

  private val ReservedJdbcKeys =
    Set(
      "url",
      "user",
      "password",
      "driver",
      "dbtable",
      "fetchsize",
      "numpartitions",
      "partitioncolumn",
      "lowerbound",
      "upperbound"
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
    "allowloadlocalinfilerolespath",
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
      "sockettimeout"
    )

  /** Build a base JDBC reader with common options (URL, credentials, driver) and user-supplied
    * connectionProperties (with reserved keys filtered out). Reused by `readDataframeRaw` so that
    * custom connection properties (e.g. SSL, timeouts) are applied consistently.
    */
  private def baseJdbcReader(
    spark: SparkSession,
    source: SourceSettings.MySQL
  ): DataFrameReader = {
    // Note: credentials are passed via .option() which Spark converts to JDBC Properties
    // internally. The password may appear in the Spark UI SQL plan display and History Server.
    // If Spark's JDBC logging is set to DEBUG, the password could also appear in logs.
    // To mitigate: set spark.redaction.regex to cover "password" (Spark's default regex
    // already covers it in Spark 3.x+), and do not enable DEBUG logging in production.
    if (!spark.conf.getOption("spark.redaction.regex").exists(_.nonEmpty)) {
      log.warn(
        "SECURITY: spark.redaction.regex is not explicitly set. " +
          "JDBC passwords may be visible in the Spark UI. " +
          "Consider setting spark.redaction.regex to include 'password' patterns."
      )
    }
    val base = spark.read
      .format("jdbc")
      .option("url", buildJdbcUrl(source))
      .option("user", source.credentials.username)
      .option("password", source.credentials.password)
      .option("driver", "com.mysql.cj.jdbc.Driver")

    val safeProps = source.connectionProperties.getOrElse(Map.empty).flatMap { case (k, v) =>
      val lower = k.toLowerCase
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
      } else if (UrlEmbeddedJdbcKeys.contains(lower)) {
        log.warn(
          s"Ignoring connectionProperty '$k' – this property is already set in the JDBC URL"
        )
        None
      } else {
        // Warn about potentially insecure TLS keystore URLs
        if ((lower.contains("keystore") || lower.contains("cert")) && lower.contains("url")) {
          val vLower = v.toLowerCase
          if (vLower.startsWith("file://") || vLower.startsWith("http://")) {
            log.warn(
              s"SECURITY: connectionProperty '$k' uses a potentially insecure URL scheme: '$v'. " +
                "Verify this is from a trusted source."
            )
          }
        }
        Some(k -> v)
      }
    }

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

    log.info(s"Connecting to MySQL at ${source.host}:${source.port}/${source.database}")
    log.info(s"Reading table: ${source.table}")
    log.info(s"JDBC useCursorFetch=true, fetchSize=${source.fetchSize}")
    if (source.fetchSize > RecommendedMaxFetchSize)
      log.warn(
        s"fetchSize (${source.fetchSize}) exceeds the recommended maximum of $RecommendedMaxFetchSize. " +
          "For tables with large rows (TEXT/BLOB columns) this may cause excessive memory usage " +
          "per JDBC connection. Consider lowering fetchSize to the 1000-10000 range."
      )

    val tableExpression = source.where match {
      case Some(filter) if filter.trim.nonEmpty =>
        // Block control characters that could cause issues
        if (filter.exists(c => c.isControl))
          sys.error(
            s"WHERE clause contains control characters (newlines, null bytes, etc.) which are not allowed. " +
              s"Filter length: ${filter.length} characters"
          )
        // Reject WHERE clauses containing dangerous SQL keywords. Word boundaries (\b) are
        // used to avoid false positives on column names like "is_deleted", "truncated", etc.
        // This is a hard reject (matching the control-character and dangerous-JDBC-property
        // checks) because a legitimate WHERE filter should not contain DDL/DML keywords.
        // Strip SQL comments first to prevent bypass via keyword obfuscation like UNI/**/ON.
        val dangerousPattern =
          """\b(drop|delete|truncate|alter|create|exec|execute|union|into|outfile|dumpfile|load_file|benchmark|sleep|grant|revoke)\b""".r
        val filterLower = stripSqlComments(filter.toLowerCase)
        if (dangerousPattern.findFirstIn(filterLower).isDefined)
          sys.error(
            "WHERE clause contains potentially dangerous SQL keyword(s): " +
              s"matched '${dangerousPattern.findFirstIn(filterLower).get}'. " +
              "DDL/DML keywords (DROP, DELETE, TRUNCATE, ALTER, CREATE, EXEC, EXECUTE, UNION, " +
              "INTO, OUTFILE, DUMPFILE, LOAD_FILE, BENCHMARK, SLEEP, GRANT, REVOKE) " +
              "are not allowed in WHERE filters."
          )
        // Note: the `where` value is treated as trusted input because the config author
        // already has database credentials. No SQL sanitization is applied.
        log.debug(
          "The 'where' field is passed directly to MySQL as SQL without sanitization. " +
            "Ensure the configuration file is not writable by untrusted principals."
        )
        log.info(s"Applying WHERE filter: $filter")
        s"(SELECT * FROM ${escapeIdentifier(source.table)} WHERE $filter) AS filtered_table"
      case Some(_) =>
        sys.error(
          "WHERE clause is empty or blank. Remove the 'where' key or provide a valid filter."
        )
      case None => escapeIdentifier(source.table)
    }

    val baseReader = baseJdbcReader(spark, source)
      .option("fetchsize", source.fetchSize)
      .option("dbtable", tableExpression)

    val reader = (source.partitionColumn, source.numPartitions) match {
      case (Some(col), Some(n)) =>
        (source.lowerBound, source.upperBound) match {
          case (Some(lower), Some(upper)) =>
            log.info(
              s"Using partitioned read: column=$col, partitions=$n, " +
                s"lowerBound=$lower, upperBound=$upper"
            )
            baseReader
              .option("partitionColumn", col)
              .option("numPartitions", n)
              .option("lowerBound", lower)
              .option("upperBound", upper)
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
        baseReader
    }

    val rawDf = reader.load()
    rawDf
  }
}
