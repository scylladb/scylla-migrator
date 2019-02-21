package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.dht.LongToken
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledThreadPoolExecutor

import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.nio.file.Files

import com.datastax.spark.connector.writer.TokenRangeAccumulator
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.writer.{ SqlRowWriter, TTLOption, TimestampOption, WriteConf }
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ LongType, StructField, StructType }
import sun.misc.{ Signal, SignalHandler }

sealed trait CopyType
object CopyType {
  case object WithTimestampPreservation extends CopyType
  case object NoTimestampPreservation extends CopyType
}
case class Selection(columnRefs: List[ColumnRef], schema: StructType, copyType: CopyType)

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def determineCopyType(tableDef: TableDef,
                        preserveTimesRequest: Boolean): Either[Throwable, CopyType] =
    if (tableDef.columnTypes.exists(_.isCollection) && preserveTimesRequest)
      Left(
        new Exception("TTL/Writetime preservation is unsupported for tables with collection types"))
    else if (preserveTimesRequest && tableDef.regularColumns.nonEmpty)
      Right(CopyType.WithTimestampPreservation)
    else if (preserveTimesRequest && tableDef.regularColumns.isEmpty) {
      log.warn("No regular columns in the table - disabling timestamp preservation")
      Right(CopyType.NoTimestampPreservation)
    } else Right(CopyType.NoTimestampPreservation)

  def createSelection(tableDef: TableDef,
                      origSchema: StructType,
                      preserveTimes: Boolean): Either[Throwable, Selection] =
    determineCopyType(tableDef, preserveTimes).right map {
      case CopyType.WithTimestampPreservation =>
        val columnRefs =
          tableDef.partitionKey.map(_.ref) ++
            tableDef.clusteringColumns.map(_.ref) ++
            tableDef.regularColumns.flatMap { column =>
              val colName = column.columnName

              List(
                column.ref,
                colName.ttl as s"${colName}_ttl",
                colName.writeTime as s"${colName}_writetime"
              )
            }

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))

        val schema = StructType(for {
          origField <- origSchema.fields
          isRegular = tableDef.regularColumns.exists(_.ref.columnName == origField.name)
          field <- if (isRegular)
                    List(
                      origField,
                      StructField(s"${origField.name}_ttl", LongType, true),
                      StructField(s"${origField.name}_writetime", LongType, true))
                  else List(origField)
        } yield field)

        log.info("Schema generated with TTLs and Writetimes:")
        schema.printTreeString()

        Selection(columnRefs.toList, schema, CopyType.WithTimestampPreservation)

      case CopyType.NoTimestampPreservation =>
        val columnRefs = tableDef.allColumns.map(_.ref).toList

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))
        log.info("Schema generated:")
        origSchema.printTreeString()

        Selection(columnRefs, origSchema, CopyType.NoTimestampPreservation)
    }

  def readDataframe(source: SourceSettings,
                    preserveTimes: Boolean,
                    tokenRangesToSkip: Set[(Long, Long)])(
    implicit spark: SparkSession): (StructType, TableDef, DataFrame, CopyType) = {
    val clusterName = "source"
    spark.setCassandraConf(
      clusterName,
      CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
        CassandraConnectorConf.ConnectionPortParam.option(source.port))
    spark.setCassandraConf(
      clusterName,
      CassandraConnectorConf.MaxConnectionsPerExecutorParam
        .option(source.connections))

    implicit val connector = new CassandraConnector(
      CassandraConnectorConf(
        spark.sparkContext.getConf.setAll(
          CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
            CassandraConnectorConf.ConnectionPortParam.option(source.port) ++
            source.credentials
              .map {
                case Credentials(user, pass) =>
                  DefaultAuthConfFactory.UserNameParam.option(user) ++
                    DefaultAuthConfFactory.PasswordParam.option(pass)
              }
              .getOrElse(Map())
        )
      ).copy(
        maxConnectionsPerExecutor = source.connections,
        queryRetryCount           = -1
      )
    )

    implicit val readConf = ReadConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(
        splitCount      = source.splitCount,
        fetchSizeInRows = source.fetchSize
      )

    val tableDef =
      Schema.tableFromCassandra(connector, source.keyspace, source.table)
    log.info("TableDef retrieved for source:")
    log.info(tableDef)

    val origSchema = StructType(tableDef.columns.map(DataTypeConverter.toStructField))
    log.info("Original schema loaded:")
    origSchema.printTreeString()

    val selection = createSelection(tableDef, origSchema, preserveTimes).fold(throw _, identity)

    val rdd = spark.sparkContext
      .cassandraTable[CassandraSQLRow](
        source.keyspace,
        source.table,
        (s, e) => !tokenRangesToSkip.contains((s, e)))
      .select(selection.columnRefs: _*)
      .asInstanceOf[RDD[Row]]

    // spark.createDataFrame does something weird with the encoder (tries to convert the row again),
    // so it's important to use createDataset with an explciit encoder instead here
    (
      origSchema,
      tableDef,
      spark.createDataset(rdd)(RowEncoder(selection.schema)),
      selection.copyType)
  }

  def explodeRow(row: Row,
                 schema: StructType,
                 primaryKeyOrdinals: Map[String, Int],
                 regularKeyOrdinals: Map[String, (Int, Int, Int)]) =
    if (regularKeyOrdinals.isEmpty) List(row)
    else
      regularKeyOrdinals
        .flatMap {
          case (fieldName, (ordinal, ttlOrdinal, writetimeOrdinal))
              if !row.isNullAt(writetimeOrdinal) =>
            Some(
              (
                fieldName,
                if (row.isNullAt(ordinal)) CassandraOption.Null
                else CassandraOption.Value(row.get(ordinal)),
                if (row.isNullAt(ttlOrdinal)) None
                else Some(row.getLong(ttlOrdinal)),
                row.getLong(writetimeOrdinal)))

          case _ =>
            None
        }
        .groupBy {
          case (fieldName, value, ttl, writetime) => (ttl, writetime)
        }
        .mapValues(
          _.map {
            case (fieldName, value, _, _) => fieldName -> value
          }.toMap
        )
        .map {
          case ((ttl, writetime), fields) =>
            val newValues = schema.fields.map { field =>
              primaryKeyOrdinals
                .get(field.name)
                .flatMap { ord =>
                  if (row.isNullAt(ord)) None
                  else Some(row.get(ord))
                }
                .getOrElse(fields.getOrElse(field.name, CassandraOption.Unset))
            } ++ Seq(ttl.getOrElse(0L), writetime)

            Row(newValues: _*)
        }

  def indexFields(currentFieldNames: List[String],
                  origFieldNames: List[String],
                  tableDef: TableDef) = {
    val fieldIndices = currentFieldNames.zipWithIndex.toMap
    val primaryKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.primaryKey.exists(_.ref.columnName == origFieldName)
        index <- fieldIndices.get(origFieldName)
      } yield origFieldName -> index).toMap

    val regularKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.regularColumns.exists(_.ref.columnName == origFieldName)
        fieldIndex     <- fieldIndices.get(origFieldName)
        ttlIndex       <- fieldIndices.get(s"${origFieldName}_ttl")
        writetimeIndex <- fieldIndices.get(s"${origFieldName}_writetime")
      } yield origFieldName -> (fieldIndex, ttlIndex, writetimeIndex)).toMap

    (primaryKeyIndices, regularKeyIndices)
  }

  def writeDataframe(
    target: TargetSettings,
    renames: List[Rename],
    df: DataFrame,
    origSchema: StructType,
    tableDef: TableDef,
    copyType: CopyType,
    tokenRangeAccumulator: TokenRangeAccumulator)(implicit spark: SparkSession): Unit = {
    val clusterName = "dest"
    spark.setCassandraConf(
      clusterName,
      CassandraConnectorConf.ConnectionHostParam.option(target.host) ++
        CassandraConnectorConf.ConnectionPortParam.option(target.port))
    spark.setCassandraConf(
      clusterName,
      CassandraConnectorConf.MaxConnectionsPerExecutorParam
        .option(target.connections))

    val connector = new CassandraConnector(
      CassandraConnectorConf(
        spark.sparkContext.getConf.setAll(
          CassandraConnectorConf.ConnectionHostParam.option(target.host) ++
            CassandraConnectorConf.ConnectionPortParam.option(target.port) ++
            target.credentials
              .map {
                case Credentials(user, pass) =>
                  DefaultAuthConfFactory.UserNameParam.option(user) ++
                    DefaultAuthConfFactory.PasswordParam.option(pass)
              }
              .getOrElse(Map())
        )
      ).copy(
        maxConnectionsPerExecutor = target.connections,
        queryRetryCount           = -1
      )
    )

    val writeConf = WriteConf.fromSparkConf(spark.sparkContext.getConf)

    val transformedDF = copyType match {
      case CopyType.WithTimestampPreservation =>
        val (primaryKeyOrdinals, regularKeyOrdinals) = indexFields(
          df.schema.fields.map(_.name).toList,
          origSchema.fields.map(_.name).toList,
          tableDef)

        val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
        val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(regularKeyOrdinals)
        val broadcastSchema = spark.sparkContext.broadcast(origSchema)
        val finalSchema = StructType(
          origSchema.fields ++
            Seq(StructField("ttl", LongType, true), StructField("writetime", LongType, true))
        )

        log.info("Schema that'll be used for writing to Scylla:")
        log.info(finalSchema.treeString)

        df.flatMap {
          explodeRow(
            _,
            broadcastSchema.value,
            broadcastPrimaryKeyOrdinals.value,
            broadcastRegularKeyOrdinals.value)
        }(RowEncoder(finalSchema))
      case CopyType.NoTimestampPreservation => df
    }

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    val renamedSchema = renames
      .foldLeft(transformedDF) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

    log.info("Schema after renames:")
    log.info(renamedSchema.treeString)

    transformedDF.rdd.saveToCassandra(
      target.keyspace,
      target.table,
      SomeColumns(
        renamedSchema.fields
          .map(x => x.name: ColumnRef)
          .filterNot(ref => ref.columnName == "ttl" || ref.columnName == "writetime"): _*),
      copyType match {
        case CopyType.WithTimestampPreservation =>
          writeConf.copy(
            ttl       = TTLOption.perRow("ttl"),
            timestamp = TimestampOption.perRow("writetime")
          )
        case CopyType.NoTimestampPreservation => writeConf
      },
      tokenRangeAccumulator = Some(tokenRangeAccumulator)
    )(connector, SqlRowWriter.Factory)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.cassandra.dev.customFromDriver", "com.scylladb.migrator.CustomUUIDConverter")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val (origSchema, tableDef, sourceDF, copyType) =
      readDataframe(
        migratorConfig.source,
        migratorConfig.preserveTimestamps,
        migratorConfig.skipTokenRanges)

    log.info("Created source dataframe; resulting schema:")
    sourceDF.printSchema()

    log.info("Starting write...")

    val tokenRangeAccumulator = TokenRangeAccumulator.empty
    spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    addUSR2Handler(migratorConfig, tokenRangeAccumulator)
    startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator)

    try {
      writeDataframe(
        migratorConfig.target,
        migratorConfig.renames,
        sourceDF,
        origSchema,
        tableDef,
        copyType,
        tokenRangeAccumulator)
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      dumpAccumulatorState(migratorConfig, tokenRangeAccumulator, "final")
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def dumpAccumulatorState(config: MigratorConfig,
                           accumulator: TokenRangeAccumulator,
                           reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize
    val rangesToSkip = accumulator.value.get.map { range =>
      (
        range.range.start.asInstanceOf[LongToken].value,
        range.range.end.asInstanceOf[LongToken].value)
    }

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges ++ rangesToSkip
    )

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))

    log.info(
      s"Created a savepoint config at ${filename} due to ${reason}. Ranges added: ${rangesToSkip}")
  }

  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor,
                             config: MigratorConfig,
                             acc: TokenRangeAccumulator): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpAccumulatorState(config, acc, "schedule")
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")

    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
  }

  def addUSR2Handler(config: MigratorConfig, acc: TokenRangeAccumulator) = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpAccumulatorState(config, acc, signal.toString)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
  }
}
