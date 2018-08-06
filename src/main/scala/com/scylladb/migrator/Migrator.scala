package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  case class Target(cluster: String, host: String, port: Int, keyspace: String, table: String, splitCount: Option[Int] = None)
  case class Config(source: Target, dest: Target)

  def readDataframe(source: Target)(implicit spark: SparkSession): DataFrame =
    spark.read
      .cassandraFormat(source.table, source.keyspace, source.cluster, pushdownEnable = true)
      .options(source.splitCount.map(cnt => ("spark.cassandra.splitCount", cnt.toString)).toMap)
      .load()

  def writeDataframe(dest: Target, df: DataFrame)(implicit spark: SparkSession): Unit =
    df.write
      .cassandraFormat(dest.table, dest.keyspace, dest.cluster, pushdownEnable = true)
      .mode(SaveMode.Append)
      .save()

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("scylla-migrator")
      .config("spark.cassandra.input.fetch.size_in_rows", 50000)
      .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .config("spark.cassandra.output.batch.size.bytes", 100L * 1024L * 1024L)
      .getOrCreate

    import spark.implicits._
    // import spark.log

    val source = Target(
      spark.conf.get("spark.scylla.source.cluster"),
      spark.conf.get("spark.scylla.source.host"),
      spark.conf.get("spark.scylla.source.port").toInt,
      spark.conf.get("spark.scylla.source.keyspace"),
      spark.conf.get("spark.scylla.source.table"),
      spark.conf.getOption("scylla.source.splitCount").map(_.toInt)
    )

    spark.setCassandraConf(source.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(source.host) ++
      CassandraConnectorConf.ConnectionPortParam.option(source.port))

    val dest = Target(
      spark.conf.get("spark.scylla.dest.cluster"),
      spark.conf.get("spark.scylla.dest.host"),
      spark.conf.get("spark.scylla.dest.port").toInt,
      spark.conf.get("spark.scylla.dest.keyspace"),
      spark.conf.get("spark.scylla.dest.table")
    )

    spark.setCassandraConf(dest.cluster,
      CassandraConnectorConf.ConnectionHostParam.option(dest.host) ++
      CassandraConnectorConf.ConnectionPortParam.option(dest.port))

    val sourceDF = readDataframe(source)

    log.info("Read source dataframe; resulting schema:")
    sourceDF.printSchema()

    log.info("Starting write...")
    writeDataframe(dest, sourceDF)
  }
}
