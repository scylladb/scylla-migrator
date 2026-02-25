package com.scylladb.migrator

import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DescribeStreamRequest,
  DescribeTableRequest,
  GetRecordsRequest,
  GetShardIteratorRequest,
  Record,
  Shard,
  ShardIteratorType,
  StreamRecord
}
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

import java.util
import scala.jdk.CollectionConverters._

/** Polls DynamoDB Streams using the v2 SDK directly, replacing the KCL-based approach. */
object DynamoStreamPoller {
  private val log = LogManager.getLogger("com.scylladb.migrator.DynamoStreamPoller")

  /** Discover the stream ARN for the given table. */
  def getStreamArn(
    client: DynamoDbClient,
    tableName: String
  ): String = {
    val response =
      client.describeTable(DescribeTableRequest.builder().tableName(tableName).build())
    val arn = response.table().latestStreamArn()
    if (arn == null)
      throw new RuntimeException(
        s"Table $tableName does not have a stream enabled"
      )
    arn
  }

  /** List all shards for a given stream ARN. */
  def listShards(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String
  ): Seq[Shard] = {
    var shards = Seq.empty[Shard]
    var lastEvaluatedShardId: Option[String] = None
    var done = false

    while (!done) {
      val requestBuilder =
        DescribeStreamRequest.builder().streamArn(streamArn)
      lastEvaluatedShardId.foreach(requestBuilder.exclusiveStartShardId)
      val response = streamsClient.describeStream(requestBuilder.build())
      val desc = response.streamDescription()
      shards = shards ++ desc.shards().asScala
      val lastShardId = desc.lastEvaluatedShardId()
      if (lastShardId != null) {
        lastEvaluatedShardId = Some(lastShardId)
      } else {
        done = true
      }
    }
    shards
  }

  /** Get a shard iterator for the given shard. */
  def getShardIterator(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    iteratorType: ShardIteratorType
  ): String =
    streamsClient
      .getShardIterator(
        GetShardIteratorRequest
          .builder()
          .streamArn(streamArn)
          .shardId(shardId)
          .shardIteratorType(iteratorType)
          .build()
      )
      .shardIterator()

  /** Get a shard iterator starting after the given sequence number. Used for resuming from
    * checkpoints.
    */
  def getShardIteratorAfterSequence(
    streamsClient: DynamoDbStreamsClient,
    streamArn: String,
    shardId: String,
    sequenceNumber: String
  ): String =
    streamsClient
      .getShardIterator(
        GetShardIteratorRequest
          .builder()
          .streamArn(streamArn)
          .shardId(shardId)
          .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
          .sequenceNumber(sequenceNumber)
          .build()
      )
      .shardIterator()

  /** Read records from a shard iterator. Returns (records, nextShardIterator). The next iterator is
    * None when the shard is closed.
    */
  def getRecords(
    streamsClient: DynamoDbStreamsClient,
    shardIterator: String
  ): (Seq[Record], Option[String]) = {
    val response =
      streamsClient.getRecords(
        GetRecordsRequest.builder().shardIterator(shardIterator).build()
      )
    val records = response.records().asScala.toSeq
    val nextIterator = Option(response.nextShardIterator())
    (records, nextIterator)
  }

  /** Convert a stream Record into a v2 item map with the operation type marker. Returns None for
    * unrecognized record types.
    */
  def recordToItem(
    record: Record,
    operationTypeColumn: String,
    putMarker: AttributeValue,
    deleteMarker: AttributeValue
  ): Option[util.Map[String, AttributeValue]] = {
    val eventName = record.eventName().toString
    val streamRecord: StreamRecord = record.dynamodb()

    eventName match {
      case "INSERT" | "MODIFY" =>
        val item = new util.HashMap[String, AttributeValue]()
        if (streamRecord.newImage() != null)
          item.putAll(streamRecord.newImage())
        if (streamRecord.keys() != null)
          item.putAll(streamRecord.keys())
        item.put(operationTypeColumn, putMarker)
        Some(item)

      case "REMOVE" =>
        val item = new util.HashMap[String, AttributeValue]()
        if (streamRecord.keys() != null)
          item.putAll(streamRecord.keys())
        item.put(operationTypeColumn, deleteMarker)
        Some(item)

      case _ =>
        log.warn(s"Unknown event type: $eventName, skipping record")
        None
    }
  }
}
