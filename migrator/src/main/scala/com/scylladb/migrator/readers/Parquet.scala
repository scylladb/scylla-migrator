package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame = {
    source.finalCredentials.foreach { credentials =>
      log.info("Loaded AWS credentials from config file")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.secretKey)
      credentials.maybeSessionToken.foreach { sessionToken =>
        // See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Using_Session_Credentials_with_TemporaryAWSCredentialsProvider
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", sessionToken)
      }
    }

    SourceDataFrame(spark.read.parquet(source.path), None, false)
  }

}
