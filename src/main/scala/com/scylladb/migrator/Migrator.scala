package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

case class Renames(renames: List[Rename])
object Renames {
  def fromString(str: String): Renames =
    Renames(str.split(';').flatMap {
      _.split(':') match {
        case Array(from, to) => Some(Rename(from, to))
        case _ => None
      }
    }.toList)
}

case class Rename(from: String, to: String)

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  case class Target(cluster: String, host: String, port: Int, keyspace: String,
    table: String, splitCount: Option[Int] = None, connectionCount: Int)
  case class Config(source: Target, dest: Target)

  def readDataframe(source: Target)(implicit spark: SparkSession): DataFrame =
    spark.read
      .cassandraFormat(source.table, source.keyspace, source.cluster, pushdownEnable = true)
      .options(source.splitCount.map(cnt => ReadConf.SplitCountParam.name -> cnt.toString).toMap ++
        Map(CassandraConnectorConf.MaxConnectionsPerExecutorParam.name -> source.connectionCount.toString))
      .load()

  def writeDataframe(dest: Target, df: DataFrame, renames: Renames)(implicit spark: SparkSession): Unit = {
    val renamed = renames.renames.foldLeft(df) {
      case (acc, Rename(from, to)) => df.withColumnRenamed(from, to)
    }

    renamed.write
      .cassandraFormat(dest.table, dest.keyspace, dest.cluster, pushdownEnable = true)
      .option(CassandraConnectorConf.MaxConnectionsPerExecutorParam.name, dest.connectionCount.toString)
      .mode(SaveMode.Append)
      .save()
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("scylla-migrator")
      .config("spark.cassandra.input.fetch.size_in_rows", 50000)
      .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .config("spark.cassandra.output.batch.size.bytes", 100L * 1024L * 1024L)
      .getOrCreate

    import spark.implicits._

    val source = Target(
      spark.conf.get("spark.scylla.source.cluster"),
      spark.conf.get("spark.scylla.source.host"),
      spark.conf.get("spark.scylla.source.port").toInt,
      spark.conf.get("spark.scylla.source.keyspace"),
      spark.conf.get("spark.scylla.source.table"),
      spark.conf.getOption("spark.scylla.source.splitCount").map(_.toInt),
      spark.conf.getOption("spark.scylla.source.connections").map(_.toInt).getOrElse(1)
    )

    val renames = spark.conf.getOption("spark.scylla.dest.renames")
      .map(Renames.fromString)
      .getOrElse(Renames(Nil))

    spark.setCassandraConf(source.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
      CassandraConnectorConf.ConnectionPortParam.option(source.port))
    spark.setCassandraConf(source.cluster,
      CassandraConnectorConf.MaxConnectionsPerExecutorParam.option(source.connectionCount))

    val dest = Target(
      spark.conf.get("spark.scylla.dest.cluster"),
      spark.conf.get("spark.scylla.dest.host"),
      spark.conf.get("spark.scylla.dest.port").toInt,
      spark.conf.get("spark.scylla.dest.keyspace"),
      spark.conf.get("spark.scylla.dest.table"),
      connectionCount = spark.conf.getOption("spark.scylla.dest.connections").map(_.toInt).getOrElse(1)
    )

    spark.setCassandraConf(dest.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(dest.host) ++
      CassandraConnectorConf.ConnectionPortParam.option(dest.port))
    spark.setCassandraConf(dest.cluster,
      CassandraConnectorConf.MaxConnectionsPerExecutorParam.option(dest.connectionCount))

    val sourceDF = readDataframe(source)

    log.info("Read source dataframe; resulting schema:")
    sourceDF.printSchema()

    log.info("Starting write...")
    writeDataframe(dest, sourceDF, renames)
  }
}
