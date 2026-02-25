package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model.{ DeleteTableRequest, DescribeTableRequest }

/** Tests for createCheckpointTable idempotency (when table already exists). */
class CreateCheckpointTableIdempotencyTest extends MigratorSuiteWithDynamoDBLocal {

  private val checkpointTable = "checkpoint_idempotency_test"

  override def afterEach(context: AfterEach): Unit = {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
    catch { case _: Exception => () }
    super.afterEach(context)
  }

  test("createCheckpointTable creates a new table") {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
    catch { case _: Exception => () }

    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)

    // Verify table exists
    val desc = sourceDDb()
      .describeTable(DescribeTableRequest.builder().tableName(checkpointTable).build())
    assert(desc.table().tableName() == checkpointTable)
  }

  test("createCheckpointTable is idempotent when table already exists") {
    // Create table first
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)

    // Second call should not throw
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)

    // Table should still exist
    val desc = sourceDDb()
      .describeTable(DescribeTableRequest.builder().tableName(checkpointTable).build())
    assert(desc.table().tableName() == checkpointTable)
  }
}
