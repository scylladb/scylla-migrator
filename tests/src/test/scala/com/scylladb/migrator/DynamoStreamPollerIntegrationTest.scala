package com.scylladb.migrator

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  ResourceNotFoundException,
  ScalarAttributeType
}

/** Integration tests for DynamoStreamPoller. Requires DynamoDB Local on port 8001. */
class DynamoStreamPollerIntegrationTest extends MigratorSuiteWithDynamoDBLocal {

  private val tableName = "StreamPollerTest"

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    try {
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      sourceDDb()
        .waiter()
        .waitUntilTableNotExists(DescribeTableRequest.builder().tableName(tableName).build())
    } catch {
      case _: ResourceNotFoundException => ()
    }
  }

  test("getStreamArn: throws when table has no stream enabled") {
    // Create table without streams
    sourceDDb().createTable(
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build()
        )
        .build()
    )
    sourceDDb()
      .waiter()
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build())

    val ex = intercept[RuntimeException] {
      DynamoStreamPoller.getStreamArn(sourceDDb(), tableName)
    }
    assert(ex.getMessage.contains("does not have a stream enabled"))
  }

  override def afterEach(context: AfterEach): Unit = {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
    catch {
      case _: Exception => ()
    }
    super.afterEach(context)
  }
}
