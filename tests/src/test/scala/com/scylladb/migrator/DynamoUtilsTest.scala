package com.scylladb.migrator

import com.scylladb.migrator.config.DynamoDBEndpoint
import org.apache.hadoop.dynamodb.DynamoDBConstants
import org.apache.hadoop.mapred.JobConf
import software.amazon.awssdk.core.interceptor.{ Context, ExecutionAttributes }
import software.amazon.awssdk.services.dynamodb.model.{
  BatchWriteItemRequest,
  BillingMode,
  BillingModeSummary,
  DeleteItemRequest,
  DescribeTableRequest,
  ProvisionedThroughputDescription,
  PutItemRequest,
  QueryRequest,
  ReturnConsumedCapacity,
  ScanRequest,
  TableDescription,
  UpdateItemRequest
}

class DynamoUtilsTest extends munit.FunSuite {

  // --- Section 3: RemoveConsumedCapacityInterceptor ---

  val interceptor = new DynamoUtils.RemoveConsumedCapacityInterceptor()
  val attrs = ExecutionAttributes.builder().build()

  private def modifyRequest(req: software.amazon.awssdk.core.SdkRequest) = {
    val ctx = new Context.ModifyRequest {
      override def request() = req
    }
    interceptor.modifyRequest(ctx, attrs)
  }

  test("Strips returnConsumedCapacity from BatchWriteItemRequest") {
    val request = BatchWriteItemRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[BatchWriteItemRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Strips returnConsumedCapacity from PutItemRequest") {
    val request = PutItemRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[PutItemRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Strips returnConsumedCapacity from DeleteItemRequest") {
    val request = DeleteItemRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[DeleteItemRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Strips returnConsumedCapacity from UpdateItemRequest") {
    val request = UpdateItemRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[UpdateItemRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Strips returnConsumedCapacity from ScanRequest") {
    val request = ScanRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[ScanRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Strips returnConsumedCapacity from QueryRequest") {
    val request = QueryRequest
      .builder()
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()
    val modified = modifyRequest(request).asInstanceOf[QueryRequest]
    assertEquals(modified.returnConsumedCapacity(), null)
  }

  test("Passes through unrelated request types unchanged") {
    val request = DescribeTableRequest.builder().tableName("test").build()
    val modified = modifyRequest(request)
    assertEquals(modified, request)
  }

  // --- Section 4: tableReadThroughput / tableWriteThroughput ---

  test("PROVISIONED billing mode returns the table's RCU") {
    val desc = TableDescription
      .builder()
      .billingModeSummary(BillingModeSummary.builder().billingMode(BillingMode.PROVISIONED).build())
      .provisionedThroughput(
        ProvisionedThroughputDescription
          .builder()
          .readCapacityUnits(100L)
          .writeCapacityUnits(50L)
          .build()
      )
      .build()
    assertEquals(DynamoUtils.tableReadThroughput(desc), 100L)
  }

  test("PROVISIONED billing mode returns the table's WCU") {
    val desc = TableDescription
      .builder()
      .billingModeSummary(BillingModeSummary.builder().billingMode(BillingMode.PROVISIONED).build())
      .provisionedThroughput(
        ProvisionedThroughputDescription
          .builder()
          .readCapacityUnits(100L)
          .writeCapacityUnits(50L)
          .build()
      )
      .build()
    assertEquals(DynamoUtils.tableWriteThroughput(desc), 50L)
  }

  test("PAY_PER_REQUEST billing mode returns DEFAULT_CAPACITY_FOR_ON_DEMAND") {
    val desc = TableDescription
      .builder()
      .billingModeSummary(
        BillingModeSummary.builder().billingMode(BillingMode.PAY_PER_REQUEST).build()
      )
      .provisionedThroughput(
        ProvisionedThroughputDescription
          .builder()
          .readCapacityUnits(0L)
          .writeCapacityUnits(0L)
          .build()
      )
      .build()
    assertEquals(
      DynamoUtils.tableReadThroughput(desc),
      DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toLong
    )
    assertEquals(
      DynamoUtils.tableWriteThroughput(desc),
      DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toLong
    )
  }

  test("Null billingModeSummary (legacy) returns the provisioned throughput") {
    val desc = TableDescription
      .builder()
      .provisionedThroughput(
        ProvisionedThroughputDescription
          .builder()
          .readCapacityUnits(200L)
          .writeCapacityUnits(75L)
          .build()
      )
      .build()
    // billingModeSummary is null by default
    assertEquals(DynamoUtils.tableReadThroughput(desc), 200L)
    assertEquals(DynamoUtils.tableWriteThroughput(desc), 75L)
  }

  // --- Section 5: setDynamoDBJobConf ---

  test("Region is set on the JobConf when provided") {
    val jobConf = new JobConf()
    DynamoUtils.setDynamoDBJobConf(jobConf, Some("eu-west-1"), None, None, None, None)
    assertEquals(jobConf.get(DynamoDBConstants.REGION), "eu-west-1")
  }

  test("Endpoint is set on the JobConf when provided") {
    val jobConf = new JobConf()
    DynamoUtils.setDynamoDBJobConf(
      jobConf,
      None,
      Some(DynamoDBEndpoint("http://localhost", 8000)),
      None,
      None,
      None
    )
    assertEquals(jobConf.get(DynamoDBConstants.ENDPOINT), "http://localhost:8000")
  }

  test("Scan segments and max map tasks are set when provided") {
    val jobConf = new JobConf()
    DynamoUtils.setDynamoDBJobConf(jobConf, None, None, Some(10), Some(5), None)
    assertEquals(jobConf.get(DynamoDBConstants.SCAN_SEGMENTS), "10")
    assertEquals(jobConf.get(DynamoDBConstants.MAX_MAP_TASKS), "5")
  }

  test("AWS credentials are set on the JobConf") {
    val jobConf = new JobConf()
    val creds = AWSCredentials("ak", "sk", Some("token"))
    DynamoUtils.setDynamoDBJobConf(jobConf, None, None, None, None, Some(creds))
    assertEquals(jobConf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF), "ak")
    assertEquals(jobConf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF), "sk")
    assertEquals(jobConf.get(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF), "token")
  }

  test("YARN resource manager is always disabled") {
    val jobConf = new JobConf()
    DynamoUtils.setDynamoDBJobConf(jobConf, None, None, None, None, None)
    assertEquals(jobConf.get(DynamoDBConstants.YARN_RESOURCE_MANAGER_ENABLED), "false")
  }

  test("removeConsumedCapacity flag is propagated") {
    val jobConf = new JobConf()
    DynamoUtils.setDynamoDBJobConf(
      jobConf,
      None,
      None,
      None,
      None,
      None,
      removeConsumedCapacity = true
    )
    assertEquals(jobConf.get("scylla.migrator.remove_consumed_capacity"), "true")
  }

}
