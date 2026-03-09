package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }

object MySQL {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.MySQL")

  val DefaultMaxAllowedPacketBytes: Long = 256 * 1024 * 1024 // 256MB
  val ContentHashColumn: String = "_content_hash"

  def readDataframe(spark: SparkSession, source: SourceSettings.MySQL): SourceDataFrame = {
    val df = readDataframeRaw(spark, source, hashColumns = None)
    val mapped = MySQLSchemaMapper.mapDataFrame(df)
    log.info("MySQL mapped schema (after type transformations):")
    mapped.printSchema()
    log.info(s"Number of partitions: ${mapped.rdd.getNumPartitions}")
    SourceDataFrame(mapped, timestampColumns = None, savepointsSupported = false)
  }

  /**
    * Read from MySQL with hash-based optimization for validation. The specified columns
    * are replaced by a single MD5 hash computed server-side in MySQL, dramatically reducing
    * network data transfer. MySQL still reads the columns from disk for hashing, but only
    * the 32-byte hash is sent over the wire.
    *
    * The resulting DataFrame contains all non-hashed columns plus a `_content_hash` column.
    */
  def readDataframeWithHash(spark: SparkSession,
                            source: SourceSettings.MySQL,
                            hashColumns: List[String]): DataFrame = {
    val df = readDataframeRaw(spark, source, hashColumns = Some(hashColumns))
    log.info("MySQL hash-based schema:")
    df.printSchema()
    log.info(s"Number of partitions: ${df.rdd.getNumPartitions}")
    df
  }

  private def buildJdbcUrl(source: SourceSettings.MySQL): String = {
    val userProps = source.connectionProperties.getOrElse(Map.empty)
    val maxPacket = userProps.getOrElse("maxAllowedPacket", DefaultMaxAllowedPacketBytes.toString)
    s"jdbc:mysql://${source.host}:${source.port}/${source.database}" +
      s"?zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false" +
      s"&maxAllowedPacket=$maxPacket&useCursorFetch=true"
  }

  /**
    * Discover column names by reading zero rows from the MySQL table.
    */
  private def discoverColumns(spark: SparkSession, source: SourceSettings.MySQL): Array[String] = {
    val url = buildJdbcUrl(source)
    val schema = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", source.credentials.username)
      .option("password", source.credentials.password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", s"(SELECT * FROM `${source.table}` LIMIT 0) AS schema_probe")
      .load()
      .schema
    schema.fieldNames
  }

  private def readDataframeRaw(spark: SparkSession,
                               source: SourceSettings.MySQL,
                               hashColumns: Option[List[String]]): DataFrame = {
    val jdbcUrl = buildJdbcUrl(source)

    log.info(s"Connecting to MySQL at ${source.host}:${source.port}/${source.database}")
    log.info(s"Reading table: ${source.table}")
    log.info(s"JDBC useCursorFetch=true, fetchSize=${source.fetchSize}")

    var reader = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", source.credentials.username)
      .option("password", source.credentials.password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("fetchsize", source.fetchSize)

    source.connectionProperties.getOrElse(Map.empty).foreach {
      case (k, v) => reader = reader.option(k, v)
    }

    val tableExpression = hashColumns match {
      case Some(cols) if cols.nonEmpty =>
        val allColumnNames = discoverColumns(spark, source)
        val hashColSet = cols.map(_.toLowerCase).toSet
        val nonHashedCols = allColumnNames.filterNot(c => hashColSet.contains(c.toLowerCase))
        val selectList = nonHashedCols.map(c => s"`$c`").mkString(", ")
        val hashExpr = buildMySQLHashExpression(cols)
        val whereClause = source.where.map(f => s" WHERE $f").getOrElse("")
        val subquery =
          s"(SELECT $selectList, $hashExpr FROM `${source.table}`$whereClause) AS hash_source"
        log.info(s"Using hash-based read. Hashed columns: ${cols.mkString(", ")}")
        log.info(s"Non-hashed columns: ${nonHashedCols.mkString(", ")}")
        log.info(s"Subquery: $subquery")
        subquery

      case _ =>
        source.where match {
          case Some(filter) =>
            log.info(s"Applying WHERE filter: $filter")
            s"(SELECT * FROM `${source.table}` WHERE $filter) AS filtered_table"
          case None => s"`${source.table}`"
        }
    }
    reader = reader.option("dbtable", tableExpression)

    (source.partitionColumn, source.numPartitions) match {
      case (Some(col), Some(n)) =>
        log.info(
          s"Using partitioned read: column=$col, partitions=$n, " +
            s"lowerBound=${source.lowerBound.getOrElse(0L)}, " +
            s"upperBound=${source.upperBound.getOrElse(Long.MaxValue)}")
        reader = reader
          .option("partitionColumn", col)
          .option("numPartitions", n)
          .option("lowerBound", source.lowerBound.getOrElse(0L))
          .option("upperBound", source.upperBound.getOrElse(Long.MaxValue))
      case (Some(col), None) =>
        sys.error(
          s"partitionColumn '$col' specified but numPartitions is missing. " +
            "Both partitionColumn and numPartitions must be set together for partitioned reads.")
      case (None, Some(n)) =>
        sys.error(
          s"numPartitions ($n) specified but partitionColumn is missing. " +
            "Both partitionColumn and numPartitions must be set together for partitioned reads.")
      case _ =>
        log.warn(
          "No partitioning configured. This will read the entire table in a single partition. " +
            "For large tables (2TB+), set partitionColumn and numPartitions for parallel reads.")
    }

    val rawDf = reader.load()
    log.info("MySQL raw source schema:")
    rawDf.printSchema()
    rawDf
  }

  private def buildMySQLHashExpression(columns: List[String]): String = {
    val coalesced = columns.map(c => s"COALESCE(`$c`, '')").mkString(", ")
    s"MD5(CONCAT_WS('|', $coalesced)) AS `$ContentHashColumn`"
  }
}
