package com.scylladb.migrator.writers

import cats.implicits._
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.audienceproject.spark.dynamodb.implicits._
import com.scylladb.migrator.config.{ AWSCredentials, Rename, TargetSettings }
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, SparkSession }

object DynamoDB {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDB")

  def writeDataframe(target: TargetSettings.DynamoDB,
                     renames: List[Rename],
                     df: DataFrame,
                     targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {
    val provisionedThroughput = Option(targetTableDesc.getProvisionedThroughput)
    val readThroughput = provisionedThroughput.flatMap(p => Option(p.getReadCapacityUnits))
    val writeThroughput = provisionedThroughput.flatMap(p => Option(p.getWriteCapacityUnits))

    val throughput =
      if (readThroughput.isEmpty || writeThroughput.isEmpty) Some("25")
      else None

    val renamedDf = renames
      .foldLeft(df) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }

    log.info(s"Schema after renames:\n${renamedDf.schema.treeString}")

    renamedDf.write
      .options(
        (List(
          target.region.map("region" -> _),
          target.endpoint.map(ep => "endpoint"                      -> ep.renderEndpoint),
          target.scanSegments.map(segs => "readPartitions"          -> segs.toString),
          target.throughputWritePercent.map(pct => "targetCapacity" -> pct.toString),
          target.maxMapTasks.map(tasks => "defaultParallelism"      -> tasks.toString),
          throughput.map("throughput" -> _)
        ).flatten ++ target.credentials.toList.flatMap {
          case AWSCredentials(accessKey, secretKey) =>
            List("accesskey" -> accessKey, "secretkey" -> secretKey)
        }).toMap
      )
      .dynamodb(target.table)
  }
}
