package com.scylladb.migrator.alternator

import com.scylladb.migrator.readers.DynamoDB
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{
  BillingMode,
  BillingModeSummary,
  ProvisionedThroughputDescription,
  TableDescription
}

class DynamoDBInputFormatTest extends munit.FunSuite {

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val GB: Long = 1024 * 1024 * 1024

  test("no configured scanSegments in on-demand billing mode") {
    checkPartitions(10)(tableSizeBytes = 1 * GB, tableProvisionedThroughput = None)
  }

  test("no configured scanSegments in on-demand billing mode and table size is 100 GB") {
    // segments are limited by the default read throughput
    checkPartitions(200)(tableSizeBytes = 100 * GB, tableProvisionedThroughput = None)
  }

  test(
    "no configured scanSegments in on-demand billing mode, table size is 100 GB, and read throughput is 1,000,000"
  ) {
    checkPartitions(1024)(
      tableSizeBytes             = 100 * GB,
      tableProvisionedThroughput = None,
      configuredReadThroughput   = Some(1000000)
    )
  }

  test("no configured scanSegments in provisioned billing mode") {
    checkPartitions(10)(tableSizeBytes = 1 * GB, tableProvisionedThroughput = Some((10000, 10000)))
  }

  test("scanSegments = 42") {
    checkPartitions(42)(configuredScanSegments = Some(42))
  }

  test("scanSegments = 42 and maxMapTasks = 10") {
    checkPartitions(10)(configuredScanSegments = Some(42), configuredMaxMapTasks = Some(10))
  }

  def checkPartitions(expectedPartitions: Int)(
    tableSizeBytes: Long = 0L,
    tableProvisionedThroughput: Option[(Int, Int)] = None,
    configuredScanSegments: Option[Int] = None,
    configuredMaxMapTasks: Option[Int] = None,
    configuredReadThroughput: Option[Int] = None,
    configuredThroughputReadPercent: Option[Float] = None
  ): Unit = {
    val tableDescriptionBuilder =
      TableDescription
        .builder()
        .tableName("DummyTable")
        .tableSizeBytes(tableSizeBytes)
    tableProvisionedThroughput match {
      case Some((rcu, wcu)) =>
        tableDescriptionBuilder.provisionedThroughput(
          ProvisionedThroughputDescription
            .builder()
            .readCapacityUnits(rcu)
            .writeCapacityUnits(wcu)
            .build()
        )
      case None =>
        tableDescriptionBuilder
          .provisionedThroughput(ProvisionedThroughputDescription.builder().build())
          .billingModeSummary(
            BillingModeSummary.builder().billingMode(BillingMode.PAY_PER_REQUEST).build()
          )
    }

    val jobConf = DynamoDB.makeJobConf(
      spark                  = spark,
      endpoint               = None,
      credentials            = None,
      region                 = None,
      table                  = "DummyTable",
      scanSegments           = configuredScanSegments,
      maxMapTasks            = configuredMaxMapTasks,
      readThroughput         = configuredReadThroughput,
      throughputReadPercent  = configuredThroughputReadPercent,
      description            = tableDescriptionBuilder.build(),
      maybeTtlDescription    = None,
      skipSegments           = None,
      removeConsumedCapacity = false
    )
    val splits = new DynamoDBInputFormat().getSplits(jobConf, 1)

    val partitions = splits.length
    assertEquals(partitions, expectedPartitions)
  }

  override def afterAll(): Unit =
    spark.stop()

}
