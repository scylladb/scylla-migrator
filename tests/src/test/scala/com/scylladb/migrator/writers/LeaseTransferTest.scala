package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteTableRequest,
  GetItemRequest
}

import scala.jdk.CollectionConverters._

/** Tests for requestLeaseTransfer and the lease transfer flow. */
class LeaseTransferTest extends MigratorSuiteWithDynamoDBLocal {

  private val checkpointTable = "lease_transfer_test"
  private val worker1 = "worker-1"
  private val worker2 = "worker-2"

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
    catch { case _: Exception => () }
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)
  }

  override def afterEach(context: AfterEach): Unit = {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
    catch { case _: Exception => () }
    super.afterEach(context)
  }

  test("requestLeaseTransfer sets transfer field on an existing shard") {
    // Worker 1 claims the shard
    val claimed =
      DefaultCheckpointManager.tryClaimShard(sourceDDb(), checkpointTable, "shard-1", worker1)
    assert(claimed.isDefined)

    // Worker 2 requests a transfer
    val result =
      DefaultCheckpointManager.requestLeaseTransfer(
        sourceDDb(),
        checkpointTable,
        "shard-1",
        worker2
      )
    assert(result)

    // Verify the transfer field is set
    val item = sourceDDb()
      .getItem(
        GetItemRequest
          .builder()
          .tableName(checkpointTable)
          .key(
            Map(DefaultCheckpointManager.leaseKeyColumn -> AttributeValue.fromS("shard-1")).asJava
          )
          .build()
      )
      .item()
    assertEquals(item.get("leaseTransferTo").s(), worker2)
  }

  test("requestLeaseTransfer fails for non-existent shard") {
    val result = DefaultCheckpointManager.requestLeaseTransfer(
      sourceDDb(),
      checkpointTable,
      "nonexistent",
      worker2
    )
    assert(!result)
  }

  test("renewLeaseAndCheckpoint detects transfer request and releases lease") {
    // Worker 1 claims the shard
    DefaultCheckpointManager.tryClaimShard(sourceDDb(), checkpointTable, "shard-1", worker1)

    // Worker 2 requests a transfer
    DefaultCheckpointManager.requestLeaseTransfer(sourceDDb(), checkpointTable, "shard-1", worker2)

    // Worker 1 renews — should see the transfer and release
    val renewed = DefaultCheckpointManager.renewLeaseAndCheckpoint(
      sourceDDb(),
      checkpointTable,
      "shard-1",
      worker1,
      None
    )
    assert(!renewed, "renewLeaseAndCheckpoint should return false when transfer is requested")

    // Worker 2 should now be able to claim the shard
    val claimed =
      DefaultCheckpointManager.tryClaimShard(sourceDDb(), checkpointTable, "shard-1", worker2)
    assert(claimed.isDefined)
  }
}
