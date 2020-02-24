package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.writer.{ SqlRowWriter, WriteConf }
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SparkSession }

object ParquetLoader {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def loadDataFrame(spark: SparkSession, source: ParquetSourceSettings): DataFrame =
    spark.read.parquet(source.path)

  def applyRenames(df: DataFrame, renames: List[Rename]): DataFrame =
    renames.foldLeft(df) {
      case (df, Rename(from, to)) => df.withColumnRenamed(from, to)
    }

  def writeDataFrame(spark: SparkSession, target: TargetSettings, df: DataFrame): Unit = {
    val connector = Connectors.targetConnector(spark.sparkContext.getConf, target)
    val writeConf = WriteConf.fromSparkConf(spark.sparkContext.getConf)

    df.rdd.saveToCassandra(
      target.keyspace,
      target.table,
      columns = SomeColumns(
        df.schema.fields
          .map(x => x.name: ColumnRef): _*),
      writeConf = writeConf)(connector, SqlRowWriter.Factory)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-parquet-loader")
      .config("spark.cassandra.dev.customFromDriver", "com.scylladb.migrator.CustomUUIDConverter")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.INFO)

    val config = ParquetLoaderConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    config.source.credentials.foreach { credentials =>
      log.info("Loaded AWS credentials from config file")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.secretKey)
    }

    val parquetDf = loadDataFrame(spark, config.source)

    log.info("Created source dataframe; resulting schema:")
    parquetDf.printSchema()

    val renamedDf = applyRenames(parquetDf, config.renames)
    log.info("Schema after renames:")
    renamedDf.printSchema()

    log.info("Starting write...")
    writeDataFrame(spark, config.target, renamedDf)
  }
}
