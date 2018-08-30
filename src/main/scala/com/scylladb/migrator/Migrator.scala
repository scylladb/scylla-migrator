package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{
  CassandraConnector,
  CassandraConnectorConf,
  Schema,
  TableDef
}
import java.nio.charset.StandardCharsets
import com.datastax.spark.connector.types.NullableTypeConverter
import com.datastax.spark.connector.types.TypeConverter
import java.util.UUID
import com.datastax.spark.connector.types.PrimitiveColumnType
import com.datastax.spark.connector.types.CustomDriverConverter
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.writer.{
  SqlRowWriter,
  TTLOption,
  TimestampOption,
  WriteConf
}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{
  DataTypes,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class Renames(renames: List[Rename])
object Renames {
  def fromString(str: String): Renames =
    Renames(
      str
        .split(';')
        .flatMap {
          _.split(':') match {
            case Array(from, to) => Some(Rename(from, to))
            case _               => None
          }
        }
        .toList)
}

case class Rename(from: String, to: String)

object AnotherCustomUUIDConverter extends NullableTypeConverter[UUID] {
  def targetTypeTag = implicitly[TypeTag[UUID]]
  def convertPF = {
    case x: UUID   => x
    case x: String => UUID.fromString(x)
    case x: UTF8String =>
      UUID.fromString(new String(x.getBytes, StandardCharsets.UTF_8))
  }
}

case object CustomTimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "timeuuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(AnotherCustomUUIDConverter)
}

case object CustomUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "uuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(AnotherCustomUUIDConverter)
}

object CustomUUIDConverter extends CustomDriverConverter {
  import org.apache.spark.sql.{types => catalystTypes}
  import com.datastax.driver.core.DataType
  import com.datastax.spark.connector.types.ColumnType

  override val fromDriverRowExtension
    : PartialFunction[DataType, ColumnType[_]] = {
    case dataType if dataType.getName == DataType.timeuuid().getName =>
      CustomTimeUUIDType
    case dataType if dataType.getName == DataType.uuid().getName =>
      CustomUUIDType
  }

  override val catalystDataType
    : PartialFunction[ColumnType[_], catalystTypes.DataType] = {
    case CustomTimeUUIDType => catalystTypes.StringType
    case CustomUUIDType     => catalystTypes.StringType
  }
}

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  case class Target(cluster: String,
                    host: String,
                    port: Int,
                    keyspace: String,
                    table: String,
                    splitCount: Option[Int] = None,
                    connectionCount: Int)
  case class Config(source: Target, dest: Target)

  def createSelection(tableDef: TableDef,
                      origSchema: StructType): (List[ColumnRef], StructType) = {
    val columnRefs = for {
      colName <- origSchema.fieldNames.toList
      isRegular = tableDef.regularColumns.exists(_.ref.columnName == colName)
      colRef <- if (isRegular)
        List[ColumnRef](colName,
                        colName.ttl as s"${colName}_ttl",
                        colName.writeTime as s"${colName}_writetime")
      else List[ColumnRef](colName)
    } yield colRef
    println("ColumnRefs generated for selection:")
    println(columnRefs.mkString("\n"))

    val schema = StructType(for {
      origField <- origSchema.fields
      isRegular = tableDef.regularColumns.exists(
        _.ref.columnName == origField.name)
      field <- if (isRegular)
        List(origField,
             StructField(s"${origField.name}_ttl", LongType, true),
             StructField(s"${origField.name}_writetime", LongType, true))
      else List(origField)
    } yield field)

    println("Schema generated with TTLs and Writetimes:")
    schema.printTreeString()

    (columnRefs, schema)
  }

  def readDataframe(source: Target)(
      implicit spark: SparkSession): (StructType, TableDef, DataFrame) = {
    spark.setCassandraConf(
      source.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
        CassandraConnectorConf.ConnectionPortParam.option(source.port))
    spark.setCassandraConf(source.cluster,
                           CassandraConnectorConf.MaxConnectionsPerExecutorParam
                             .option(source.connectionCount))

    implicit val connector = CassandraConnector(
      spark.sparkContext.getConf.setAll(
        CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
          CassandraConnectorConf.ConnectionPortParam.option(source.port) ++
          CassandraConnectorConf.MaxConnectionsPerExecutorParam.option(
            source.connectionCount)
      )
    )

    implicit val readConf = ReadConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(splitCount = source.splitCount)

    val tableDef =
      Schema.tableFromCassandra(connector, source.keyspace, source.table)
    println("TableDef retrieved for source:")
    println(tableDef)

    val origSchema = StructType(
      tableDef.columns.map(DataTypeConverter.toStructField))
    println("Original schema loaded:")
    origSchema.printTreeString()

    val (selection, schemaWithTtlsWritetimes) =
      createSelection(tableDef, origSchema)

    val rdd = spark.sparkContext
      .cassandraTable[CassandraSQLRow](source.keyspace, source.table)
      .select(selection: _*)
      .asInstanceOf[RDD[Row]]

    // spark.createDataFrame does something weird with the encoder (tries to convert the row again),
    // so it's important to use createDataset with an explciit encoder instead here
    (origSchema,
     tableDef,
     spark.createDataset(rdd)(RowEncoder(schemaWithTtlsWritetimes)))
  }

  def writeDataframe(dest: Target,
                     df: DataFrame,
                     renames: Renames,
                     origSchema: StructType,
                     tableDef: TableDef)(implicit spark: SparkSession): Unit = {
    spark.setCassandraConf(
      dest.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(dest.host) ++
        CassandraConnectorConf.ConnectionPortParam.option(dest.port))
    spark.setCassandraConf(dest.cluster,
                           CassandraConnectorConf.MaxConnectionsPerExecutorParam
                             .option(dest.connectionCount))
    implicit val connector = CassandraConnector(
      spark.sparkContext.getConf.setAll(
        CassandraConnectorConf.ConnectionHostParam.option(dest.host) ++
          CassandraConnectorConf.ConnectionPortParam.option(dest.port) ++
          CassandraConnectorConf.MaxConnectionsPerExecutorParam.option(
            dest.connectionCount)
      )
    )

    import spark.implicits._

    val zipped = df.schema.fields.zipWithIndex
    val primaryKeyOrdinals =
      spark.sparkContext.broadcast {
        (for {
          origField <- origSchema.fields
          if tableDef.primaryKey.exists(_.ref.columnName == origField.name)
          (_, ordinal) <- zipped.find(_._1.name == origField.name)
        } yield origField.name -> ordinal).toMap
      }

    val regularKeyOrdinals =
      spark.sparkContext.broadcast {
        (for {
          origField <- origSchema.fields
          if tableDef.regularColumns.exists(_.ref.columnName == origField.name)
          (_, fieldOrdinal) <- zipped.find(_._1.name == origField.name)
          (_, ttlOrdinal) <- zipped.find(_._1.name == s"${origField.name}_ttl")
          (_, writetimeOrdinal) <- zipped.find(
            _._1.name == s"${origField.name}_writetime")
        } yield
          origField.name -> (fieldOrdinal, ttlOrdinal, writetimeOrdinal)).toMap
      }

    val broadcastTableDef = spark.sparkContext.broadcast(tableDef)
    val broadcastSchema = spark.sparkContext.broadcast(origSchema)
    val finalSchema = StructType(
      origSchema.fields ++
        Seq(StructField("ttl", LongType, true),
            StructField("writetime", LongType, true))
    )

    println("Schema that'll be used for writing to Scylla:")
    finalSchema.printTreeString()

    val timeTransformations = df
      .flatMap { row =>
        regularKeyOrdinals.value
          .flatMap {
            case (fieldName, (ordinal, ttlOrdinal, writetimeOrdinal))
                if !row.isNullAt(writetimeOrdinal) =>
              Some(
                (fieldName,
                 if (row.isNullAt(ordinal)) CassandraOption.Null
                 else CassandraOption.Value(row.get(ordinal)),
                 if (row.isNullAt(ttlOrdinal)) None
                 else Some(row.getLong(ttlOrdinal)),
                 row.getLong(writetimeOrdinal)))

            case _ =>
              None
          }
          .groupBy(tp => (tp._3, tp._4))
          .mapValues(_.map(tp => tp._1 -> tp._2).toMap)
          .map {
            case ((ttl, writetime), fields) =>
              val newValues = broadcastSchema.value.fields.map { field =>
                primaryKeyOrdinals.value
                  .get(field.name)
                  .flatMap { ord =>
                    if (row.isNullAt(ord)) None
                    else Some(row.get(ord))
                  }
                  .getOrElse(fields.getOrElse(field.name,
                                              CassandraOption.Unset))
              } ++ Seq(ttl.getOrElse(0L), writetime)

              Row(newValues: _*)
          }
      }(RowEncoder(finalSchema))

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    val renamedSchema = renames.renames
      .foldLeft(timeTransformations) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

    println("Schema after renames:")
    renamedSchema.printTreeString()

    implicit val rwf = SqlRowWriter.Factory
    timeTransformations.rdd.saveToCassandra(
      dest.keyspace,
      dest.table,
      SomeColumns(
        renamedSchema.fields
          .map(x => x.name: ColumnRef)
          .filterNot(ref =>
            ref.columnName == "ttl" || ref.columnName == "writetime"): _*),
      WriteConf
        .fromSparkConf(spark.sparkContext.getConf)
        .copy(
          ttl = TTLOption.perRow("ttl"),
          timestamp = TimestampOption.perRow("writetime")
        )
    )
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.cassandra.input.fetch.size_in_rows", 50000)
      .config("spark.cassandra.output.batch.size.bytes", 100L * 1024L * 1024L)
      .config("spark.cassandra.dev.customFromDriver",
              "com.scylladb.migrator.CustomUUIDConverter")
      .getOrCreate

    import spark.implicits._

    val source = Target(
      spark.conf.get("spark.scylla.source.cluster"),
      spark.conf.get("spark.scylla.source.host"),
      spark.conf.get("spark.scylla.source.port").toInt,
      spark.conf.get("spark.scylla.source.keyspace"),
      spark.conf.get("spark.scylla.source.table"),
      spark.conf.getOption("spark.scylla.source.splitCount").map(_.toInt),
      spark.conf
        .getOption("spark.scylla.source.connections")
        .map(_.toInt)
        .getOrElse(1)
    )

    val renames = spark.conf
      .getOption("spark.scylla.dest.renames")
      .map(Renames.fromString)
      .getOrElse(Renames(Nil))

    val dest = Target(
      spark.conf.get("spark.scylla.dest.cluster"),
      spark.conf.get("spark.scylla.dest.host"),
      spark.conf.get("spark.scylla.dest.port").toInt,
      spark.conf.get("spark.scylla.dest.keyspace"),
      spark.conf.get("spark.scylla.dest.table"),
      connectionCount = spark.conf
        .getOption("spark.scylla.dest.connections")
        .map(_.toInt)
        .getOrElse(1)
    )

    val (origSchema, tableDef, sourceDF) = readDataframe(source)

    log.info("Created source dataframe; resulting schema:")
    sourceDF.printSchema()

    log.info("Starting write...")
    writeDataframe(dest, sourceDF, renames, origSchema, tableDef)
  }
}
