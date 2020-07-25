package com.scylladb.migrator.readers

import com.audienceproject.spark.dynamodb.implicits._
import com.scylladb.migrator.config.SourceSettings
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object DynamoDB {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDB")

  def readDataFrame(spark: SparkSession, source: SourceSettings.DynamoDB): SourceDataFrame = {
    source.hostURL.foreach(System.setProperty("aws.dynamodb.endpoint", _))

    val df = spark.read
      .options(
        Map("region" -> source.region) ++
          source.scanSegments
            .map(segs => Map("readPartitions" -> segs.toString))
            .getOrElse(Map()) ++
          source.throughputReadPercent
            .map(pct => Map("targetCapacity" -> pct.toString))
            .getOrElse(Map()) ++
          source.maxMapTasks
            .map(tasks => Map("defaultParallelism" -> tasks.toString))
            .getOrElse(Map())
      )
      .dynamodb(source.table)

    SourceDataFrame(df, None, false)
  }

}
