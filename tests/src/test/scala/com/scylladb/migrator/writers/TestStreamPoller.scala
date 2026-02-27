package com.scylladb.migrator.writers

import com.scylladb.migrator.{ DynamoStreamPoller, StreamPollerOps }
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  Record,
  Shard,
  ShardIteratorType
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util

/** Configurable test double for [[StreamPollerOps]]. Each method delegates to a `var` function that
  * can be replaced per-test. Default implementations delegate to the real [[DynamoStreamPoller]] or
  * throw.
  */
class TestStreamPoller extends StreamPollerOps {

  var getStreamArnFn: (DynamoDbClient, String) => String =
    (_, _) => throw new UnsupportedOperationException("getStreamArn not configured")

  var listShardsFn: (DynamoDbStreamsClient, String) => Seq[Shard] =
    (_, _) => Seq.empty

  var getShardIteratorFn: (DynamoDbStreamsClient, String, String, ShardIteratorType) => String =
    (_, _, _, _) => "test-iterator"

  var getShardIteratorAfterSequenceFn: (DynamoDbStreamsClient, String, String, String) => String =
    (_, _, _, _) => "test-iterator-after-seq"

  var getRecordsFn: (DynamoDbStreamsClient, String, Option[Int]) => (Seq[Record], Option[String]) =
    (_, _, _) => (Seq.empty, None)

  var recordToItemFn
    : (Record, String, AttributeValue, AttributeValue) => Option[util.Map[String, AttributeValue]] =
    DynamoStreamPoller.recordToItem

  override def getStreamArn(client: DynamoDbClient, tableName: String): String =
    getStreamArnFn(client, tableName)

  override def listShards(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String
  ): Seq[Shard] =
    listShardsFn(streamsClient, streamArn)

  override def getShardIterator(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    iteratorType: ShardIteratorType
  ): String =
    getShardIteratorFn(streamsClient, streamArn, shardId, iteratorType)

  override def getShardIteratorAfterSequence(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    sequenceNumber: String
  ): String =
    getShardIteratorAfterSequenceFn(streamsClient, streamArn, shardId, sequenceNumber)

  override def getRecords(
    streamsClient: DynamoDbStreamsClient,
    shardIterator: String,
    limit: Option[Int]
  ): (Seq[Record], Option[String]) =
    getRecordsFn(streamsClient, shardIterator, limit)

  override def recordToItem(
    record: Record,
    operationTypeColumn: String,
    putMarker: AttributeValue,
    deleteMarker: AttributeValue
  ): Option[util.Map[String, AttributeValue]] =
    recordToItemFn(record, operationTypeColumn, putMarker, deleteMarker)
}
