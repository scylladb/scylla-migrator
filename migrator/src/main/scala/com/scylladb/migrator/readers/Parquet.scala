package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.scylla.SourceDataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Parquet {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Parquet")

  def readDataFrame(spark: SparkSession, source: SourceSettings.Parquet): SourceDataFrame = {
    source.credentials.foreach { credentials =>
      log.info("Loaded AWS credentials from config file")
      source.region.foreach { region =>
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", region)
      }
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.secretKey)
      // See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/assumed_roles.html
      credentials.assumeRole.foreach { role =>
        spark.sparkContext.hadoopConfiguration.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.assumed.role.arn", role.arn)
        role.sessionName.foreach { sessionName =>
          spark.sparkContext.hadoopConfiguration
            .set("fs.s3a.assumed.role.session.name", sessionName)
        }
      }
    }

    SourceDataFrame(spark.read.parquet(source.path), None, false)
  }

}
