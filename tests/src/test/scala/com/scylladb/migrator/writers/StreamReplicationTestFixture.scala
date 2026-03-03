package com.scylladb.migrator.writers

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

/** Shared test fixture for stream replication integration tests that need a target table on
  * Alternator (port 8000) and a checkpoint table on DynamoDB Local (port 8001).
  *
  * Provides lifecycle hooks to create/delete the target and checkpoint tables between tests.
  */
trait StreamReplicationTestFixture extends MigratorSuiteWithDynamoDBLocal {

  /** Name of the target table (on Alternator). */
  protected def targetTable: String

  /** Name of the checkpoint table (on DynamoDB Local). */
  protected def checkpointTable: String

  /** Whether to create the target table in `beforeEach`. Override to `false` in tests that
    * deliberately need the target table to be absent (e.g., write failure tests).
    */
  protected def createTargetTableOnSetup: Boolean = true

  /** Create (or recreate) the target table with a single hash key `id` (S). */
  protected def ensureTargetTable(): Unit = {
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(targetTable).build()
      )
    catch { case _: ResourceNotFoundException => () }

    targetAlternator().createTable(
      CreateTableRequest
        .builder()
        .tableName(targetTable)
        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
        )
        .build()
    )
    targetAlternator()
      .waiter()
      .waitUntilTableExists(DescribeTableRequest.builder().tableName(targetTable).build())
  }

  /** Delete the target and checkpoint tables, ignoring errors if they don't exist. */
  protected def cleanupTables(): Unit = {
    try
      targetAlternator().deleteTable(
        DeleteTableRequest.builder().tableName(targetTable).build()
      )
    catch { case _: ResourceNotFoundException => () }
    try
      sourceDDb().deleteTable(
        DeleteTableRequest.builder().tableName(checkpointTable).build()
      )
    catch { case _: ResourceNotFoundException => () }
  }

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    cleanupTables()
    if (createTargetTableOnSetup) ensureTargetTable()
  }

  override def afterEach(context: AfterEach): Unit = {
    cleanupTables()
    super.afterEach(context)
  }
}
