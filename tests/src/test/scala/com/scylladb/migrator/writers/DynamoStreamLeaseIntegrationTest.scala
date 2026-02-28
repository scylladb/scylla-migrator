package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteTableRequest,
  DescribeTableRequest,
  GetItemRequest,
  ResourceNotFoundException,
  UpdateItemRequest
}

import scala.jdk.CollectionConverters._

/** Integration tests for the shard lease and checkpoint system in DynamoStreamReplication. Requires
  * DynamoDB Local on port 8001.
  */
class DynamoStreamLeaseIntegrationTest extends MigratorSuiteWithDynamoDBLocal {

  private val checkpointTable = "lease_integration_test"
  private val leaseKeyColumn = "leaseKey"
  private val checkpointColumn = "checkpoint"
  private val leaseOwnerColumn = "leaseOwner"
  private val leaseExpiryColumn = "leaseExpiryEpochMs"

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    // Clean up checkpoint table before each test
    try {
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
      sourceDDb()
        .waiter()
        .waitUntilTableNotExists(
          DescribeTableRequest.builder().tableName(checkpointTable).build()
        )
    } catch {
      case _: ResourceNotFoundException => ()
    }
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)
  }

  // --- createCheckpointTable tests ---

  test("createCheckpointTable: creates table when missing") {
    val tableName = "ckpt_create_test"
    try {
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      sourceDDb()
        .waiter()
        .waitUntilTableNotExists(DescribeTableRequest.builder().tableName(tableName).build())
    } catch {
      case _: ResourceNotFoundException => ()
    }
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), tableName)
    val desc = sourceDDb()
      .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
      .table()
    assertEquals(desc.tableName(), tableName)
    // Clean up
    sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
  }

  test("createCheckpointTable: no-op when table already exists") {
    // Table was already created in beforeEach; calling again should not throw
    DefaultCheckpointManager.createCheckpointTable(sourceDDb(), checkpointTable)
    val desc = sourceDDb()
      .describeTable(DescribeTableRequest.builder().tableName(checkpointTable).build())
      .table()
    assertEquals(desc.tableName(), checkpointTable)
  }

  // --- tryClaimShard tests ---

  test("tryClaimShard: claim unclaimed shard returns Some(None)") {
    val result = DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-001",
      "worker-A"
    )
    assertEquals(result, Some(None))
  }

  test("tryClaimShard: claim shard with expired lease preserves checkpoint") {
    // First, worker-A claims the shard and sets a checkpoint
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-002",
      "worker-A"
    )
    DefaultCheckpointManager.renewLeaseAndCheckpoint(
      sourceDDb(),
      checkpointTable,
      "shard-002",
      "worker-A",
      Some("seq-123")
    )

    // Manually expire the lease by setting expiry to the past
    sourceDDb().updateItem(
      UpdateItemRequest
        .builder()
        .tableName(checkpointTable)
        .key(Map(leaseKeyColumn -> AttributeValue.fromS("shard-002")).asJava)
        .updateExpression("SET #expiry = :expired")
        .expressionAttributeNames(Map("#expiry" -> leaseExpiryColumn).asJava)
        .expressionAttributeValues(
          Map(":expired" -> AttributeValue.fromN("0")).asJava
        )
        .build()
    )

    // worker-B claims the expired shard
    val result = DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-002",
      "worker-B"
    )
    assertEquals(result, Some(Some("seq-123")))
  }

  test("tryClaimShard: fail on shard with active lease owned by another worker") {
    // worker-A claims with a far-future expiry
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-003",
      "worker-A",
      leaseDurationMs = 600000L // 10 minutes
    )

    // worker-B tries to claim
    val result = DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-003",
      "worker-B"
    )
    assertEquals(result, None)
  }

  test("tryClaimShard: re-claim own shard succeeds") {
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-004",
      "worker-A"
    )
    val result = DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-004",
      "worker-A"
    )
    assert(result.isDefined)
  }

  // --- renewLeaseAndCheckpoint tests ---

  test("renewLeaseAndCheckpoint: renew with checkpoint update") {
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-010",
      "worker-A"
    )
    val renewed = DefaultCheckpointManager.renewLeaseAndCheckpoint(
      sourceDDb(),
      checkpointTable,
      "shard-010",
      "worker-A",
      Some("seq-456")
    )
    assert(renewed)

    // Verify checkpoint was stored
    val item = sourceDDb()
      .getItem(
        GetItemRequest
          .builder()
          .tableName(checkpointTable)
          .key(Map(leaseKeyColumn -> AttributeValue.fromS("shard-010")).asJava)
          .build()
      )
      .item()
    assertEquals(item.get(checkpointColumn).s(), "seq-456")
  }

  test("renewLeaseAndCheckpoint: renew without checkpoint (expiry-only)") {
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-011",
      "worker-A"
    )
    val renewed = DefaultCheckpointManager.renewLeaseAndCheckpoint(
      sourceDDb(),
      checkpointTable,
      "shard-011",
      "worker-A",
      None
    )
    assert(renewed)
  }

  test("renewLeaseAndCheckpoint: fail when lease was stolen by another worker") {
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-012",
      "worker-A"
    )

    // Manually expire the lease
    sourceDDb().updateItem(
      UpdateItemRequest
        .builder()
        .tableName(checkpointTable)
        .key(Map(leaseKeyColumn -> AttributeValue.fromS("shard-012")).asJava)
        .updateExpression("SET #expiry = :expired")
        .expressionAttributeNames(Map("#expiry" -> leaseExpiryColumn).asJava)
        .expressionAttributeValues(
          Map(":expired" -> AttributeValue.fromN("0")).asJava
        )
        .build()
    )

    // worker-B steals the lease
    DefaultCheckpointManager.tryClaimShard(
      sourceDDb(),
      checkpointTable,
      "shard-012",
      "worker-B",
      leaseDurationMs = 600000L
    )

    // worker-A tries to renew — should fail
    val renewed = DefaultCheckpointManager.renewLeaseAndCheckpoint(
      sourceDDb(),
      checkpointTable,
      "shard-012",
      "worker-A",
      Some("seq-789")
    )
    assert(!renewed)
  }

  // --- Multi-runner coordination test ---

  test("multi-runner: two workers claim disjoint subsets of shards") {
    val shardIds = (1 to 6).map(i => s"shard-multi-$i")

    // Worker A claims first
    val claimedByA = shardIds.flatMap { shardId =>
      DefaultCheckpointManager.tryClaimShard(
        sourceDDb(),
        checkpointTable,
        shardId,
        "worker-A",
        leaseDurationMs = 600000L
      ) match {
        case Some(_) => Some(shardId)
        case None    => None
      }
    }

    // Worker B tries to claim all — should get None for A's shards
    val claimedByB = shardIds.flatMap { shardId =>
      DefaultCheckpointManager.tryClaimShard(
        sourceDDb(),
        checkpointTable,
        shardId,
        "worker-B"
      ) match {
        case Some(_) => Some(shardId)
        case None    => None
      }
    }

    // A got all 6, B got none (since A's leases are still active)
    assertEquals(claimedByA.size, 6)
    assertEquals(claimedByB.size, 0)
  }

  test("multi-runner: dead worker's shards are reassigned after lease expiry") {
    val shardIds = Seq("shard-expire-1", "shard-expire-2")

    // Worker A claims with very short lease
    shardIds.foreach { shardId =>
      DefaultCheckpointManager.tryClaimShard(
        sourceDDb(),
        checkpointTable,
        shardId,
        "worker-A",
        leaseDurationMs = 1L // expires immediately
      )
    }

    // Small sleep to ensure lease is expired
    Thread.sleep(10)

    // Worker B claims the expired shards
    val claimedByB = shardIds.flatMap { shardId =>
      DefaultCheckpointManager.tryClaimShard(
        sourceDDb(),
        checkpointTable,
        shardId,
        "worker-B"
      ) match {
        case Some(_) => Some(shardId)
        case None    => None
      }
    }

    assertEquals(claimedByB.toSet, shardIds.toSet)
  }

  override def afterEach(context: AfterEach): Unit = {
    try
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(checkpointTable).build())
    catch {
      case _: Exception => ()
    }
    super.afterEach(context)
  }
}
