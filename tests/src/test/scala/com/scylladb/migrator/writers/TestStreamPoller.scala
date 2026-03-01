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
import java.util.concurrent.atomic.AtomicReference

/** Configurable test double for [[StreamPollerOps]]. Each method delegates to an
  * [[AtomicReference]]-wrapped function that can be safely swapped from the test thread while
  * polling pool threads invoke the methods.
  */
class TestStreamPoller extends StreamPollerOps {

  val getStreamArnFn: AtomicReference[(DynamoDbClient, String) => String] =
    new AtomicReference((_, _) => throw new UnsupportedOperationException("getStreamArn not configured"))

  val listShardsFn: AtomicReference[(DynamoDbStreamsClient, String) => Seq[Shard]] =
    new AtomicReference((_, _) => Seq.empty)

  val getShardIteratorFn: AtomicReference[(DynamoDbStreamsClient, String, String, ShardIteratorType) => String] =
    new AtomicReference((_, _, _, _) => "test-iterator")

  val getShardIteratorAfterSequenceFn: AtomicReference[(DynamoDbStreamsClient, String, String, String) => String] =
    new AtomicReference((_, _, _, _) => "test-iterator-after-seq")

  val getRecordsFn: AtomicReference[(DynamoDbStreamsClient, String, Option[Int]) => (Seq[Record], Option[String])] =
    new AtomicReference((_, _, _) => throw new UnsupportedOperationException("getRecords not configured"))

  val recordToItemFn
    : AtomicReference[(Record, String, AttributeValue, AttributeValue) => Option[util.Map[String, AttributeValue]]] =
    new AtomicReference(DynamoStreamPoller.recordToItem)

  override def getStreamArn(client: DynamoDbClient, tableName: String): String =
    getStreamArnFn.get()(client, tableName)

  override def listShards(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String
  ): Seq[Shard] =
    listShardsFn.get()(streamsClient, streamArn)

  override def getShardIterator(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    iteratorType: ShardIteratorType
  ): String =
    getShardIteratorFn.get()(streamsClient, streamArn, shardId, iteratorType)

  override def getShardIteratorAfterSequence(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    sequenceNumber: String
  ): String =
    getShardIteratorAfterSequenceFn.get()(streamsClient, streamArn, shardId, sequenceNumber)

  override def getRecords(
    streamsClient: DynamoDbStreamsClient,
    shardIterator: String,
    limit: Option[Int]
  ): (Seq[Record], Option[String]) =
    getRecordsFn.get()(streamsClient, shardIterator, limit)

  override def recordToItem(
    record: Record,
    operationTypeColumn: String,
    putMarker: AttributeValue,
    deleteMarker: AttributeValue
  ): Option[util.Map[String, AttributeValue]] =
    recordToItemFn.get()(record, operationTypeColumn, putMarker, deleteMarker)
}
