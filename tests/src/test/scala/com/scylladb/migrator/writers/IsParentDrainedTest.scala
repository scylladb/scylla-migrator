package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.MigratorSuiteWithDynamoDBLocal
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteTableRequest,
  PutItemRequest
}

import scala.jdk.CollectionConverters._

/** Tests for isParentDrained behavior with various checkpoint states. */
class IsParentDrainedTest extends MigratorSuiteWithDynamoDBLocal {

  private val checkpointTable = "parent_drained_test"

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

  test("isParentDrained returns true when parentShardId is None (root shard)") {
    val result = DefaultCheckpointManager.isParentDrained(sourceDDb(), checkpointTable, None)
    assert(result)
  }

  test("isParentDrained returns false when parent has no checkpoint row") {
    val result =
      DefaultCheckpointManager.isParentDrained(sourceDDb(), checkpointTable, Some("nonexistent"))
    assert(!result)
  }

  test("isParentDrained returns false when parent has a sequence number checkpoint") {
    // Write a regular checkpoint (not SHARD_END) for the parent
    sourceDDb().putItem(
      PutItemRequest
        .builder()
        .tableName(checkpointTable)
        .item(
          Map(
            DefaultCheckpointManager.leaseKeyColumn -> AttributeValue.fromS("parent-shard"),
            "checkpoint"                            -> AttributeValue.fromS("seq-12345")
          ).asJava
        )
        .build()
    )

    val result =
      DefaultCheckpointManager.isParentDrained(sourceDDb(), checkpointTable, Some("parent-shard"))
    assert(!result)
  }

  test("isParentDrained returns true when parent has SHARD_END checkpoint") {
    sourceDDb().putItem(
      PutItemRequest
        .builder()
        .tableName(checkpointTable)
        .item(
          Map(
            DefaultCheckpointManager.leaseKeyColumn -> AttributeValue.fromS("parent-shard"),
            "checkpoint" -> AttributeValue.fromS(DefaultCheckpointManager.shardEndSentinel)
          ).asJava
        )
        .build()
    )

    val result =
      DefaultCheckpointManager.isParentDrained(sourceDDb(), checkpointTable, Some("parent-shard"))
    assert(result)
  }
}
