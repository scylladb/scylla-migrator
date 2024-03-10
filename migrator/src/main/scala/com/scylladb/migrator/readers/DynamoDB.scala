package com.scylladb.migrator.readers

import cats.implicits._
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.audienceproject.spark.dynamodb.implicits._
import com.scylladb.migrator.config.{ AWSCredentials, SourceSettings }
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object DynamoDB {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDB")

  def readDataFrame(spark: SparkSession,
                    source: SourceSettings.DynamoDB,
                    description: TableDescription): SourceDataFrame = {
    val provisionedThroughput = Option(description.getProvisionedThroughput)
    val readThroughput = provisionedThroughput.flatMap(p => Option(p.getReadCapacityUnits))
    val writeThroughput = provisionedThroughput.flatMap(p => Option(p.getWriteCapacityUnits))

    val throughput =
      if (readThroughput.isEmpty || writeThroughput.isEmpty) Some("25")
      else None

    val df = spark.read
      .options(
        (List(
          source.region.map("region" -> _),
          source.endpoint.map(ep => "endpoint"                     -> ep.renderEndpoint),
          source.scanSegments.map(segs => "readPartitions"         -> segs.toString),
          source.throughputReadPercent.map(pct => "targetCapacity" -> pct.toString),
          source.maxMapTasks.map(tasks => "defaultParallelism"     -> tasks.toString),
          throughput.map("throughput" -> _)
        ).flatten ++ source.credentials.toList.flatMap {
          case AWSCredentials(accessKey, secretKey) =>
            List("accesskey" -> accessKey, "secretkey" -> secretKey)
        }).toMap
      )
      .dynamodb(source.table)

    SourceDataFrame(df, None, false)
  }

}
