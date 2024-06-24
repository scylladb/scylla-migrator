package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.{BillingMode, BillingModeSummary, ProvisionedThroughputDescription, TableDescription}
import com.scylladb.migrator.readers.DynamoDB
import org.apache.spark.sql.SparkSession

class DynamoDBInputFormatTest extends munit.FunSuite {

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val GB: Long = 1024 * 1024 * 1024

  test("no configured scanSegments in on-demand billing mode") {
    checkPartitions(11)(tableSizeBytes = 1 * GB, tableProvisionedThroughput = None)
  }

  test("no configured scanSegments in on-demand billing mode and table size is 100 GB") {
    checkPartitions(1024)(tableSizeBytes = 100 * GB, tableProvisionedThroughput = None)
  }

  test("no configured scanSegments in provisioned billing mode") {
    checkPartitions(11)(tableSizeBytes = 1 * GB, tableProvisionedThroughput = Some((25, 25)))
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
    val tableDescription =
      new TableDescription()
        .withTableName("DummyTable")
        .withTableSizeBytes(tableSizeBytes)
    tableProvisionedThroughput match {
      case Some((rcu, wcu)) =>
        tableDescription.withProvisionedThroughput(
          new ProvisionedThroughputDescription()
            .withReadCapacityUnits(rcu)
            .withWriteCapacityUnits(wcu)
        )
      case None =>
        tableDescription.withProvisionedThroughput(new ProvisionedThroughputDescription())
          .withBillingModeSummary(new BillingModeSummary().withBillingMode(BillingMode.PAY_PER_REQUEST))
    }

    val jobConf = DynamoDB.makeJobConf(
      spark = spark,
      endpoint = None,
      credentials = None,
      region = None,
      table = "DummyTable",
      scanSegments = configuredScanSegments,
      maxMapTasks = configuredMaxMapTasks,
      readThroughput = configuredReadThroughput,
      throughputReadPercent = configuredThroughputReadPercent,
      description = tableDescription
    )
    val splits = new DynamoDBInputFormat().getSplits(jobConf, 1)

    val partitions = splits.length
    assertEquals(partitions, expectedPartitions)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
