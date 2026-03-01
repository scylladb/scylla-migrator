package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchWriteItemRequest,
  DeleteRequest,
  DynamoDbException,
  PutRequest,
  TableDescription,
  WriteRequest
}

import java.util
import scala.jdk.CollectionConverters._

/** Handles batched writes to a DynamoDB target table using BatchWriteItem.
  *
  * Extracted from `DynamoStreamReplication` to separate write concerns from stream orchestration.
  */
/** Thrown when BatchWriteItem retries are exhausted and unprocessed items remain. */
class BatchWriteExhaustedException(message: String, val unprocessedCount: Int)
    extends RuntimeException(message)

object BatchWriter {
  private val log = LogManager.getLogger("com.scylladb.migrator.writers.BatchWriter")

  /** Maximum number of items in a single DynamoDB BatchWriteItem request. */
  private[writers] val batchWriteItemLimit = 25

  /** Backoff parameters for flushAndClearBatch retries: initial delay, max delay, max attempts. */
  private val initialBackoffMs = 100L
  private val maxBackoffMs = 3000L
  private val maxRetries = 5
  private val retryableErrorCodes = CheckpointManager.retryableErrorCodes

  type DynamoItem = util.Map[String, AttributeValue]

  private[writers] val operationTypeColumn = "_dynamo_op_type"
  private[writers] val putOperation = AttributeValue.fromBool(true)
  private[writers] val deleteOperation = AttributeValue.fromBool(false)

  /** Write a batch of stream change items to the target table. Items are grouped into
    * BatchWriteItem requests of up to 25 items each. Duplicate keys within a batch trigger an early
    * flush to preserve ordering.
    */
  private[writers] def run(
    items: Iterable[DynamoItem],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription,
    client: DynamoDbClient
  ): Unit = {
    val msgs = items
    if (msgs.isEmpty) {
      log.debug("No changes to apply")
    } else {
      val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet
      var putCount = 0L
      var deleteCount = 0L
      val batch = new util.ArrayList[WriteRequest]()
      val batchKeyStrings = new util.HashSet[String]()
      val keyAttrNames = keyAttributeNames.toSeq

      msgs.foreach { item =>
        val isPut = item.get(operationTypeColumn) == putOperation

        val keyMap = new util.HashMap[String, AttributeValue](keyAttrNames.size)
        val keyStringBuilder = new StringBuilder
        for (attr <- keyAttrNames) {
          val value = item.get(attr)
          if (value != null) {
            val renamedAttr = renamesMap.getOrElse(attr, attr)
            keyMap.put(renamedAttr, value)
            if (keyStringBuilder.nonEmpty) keyStringBuilder.append('|')
            keyStringBuilder.append(renamedAttr).append('=').append(value)
          }
        }
        val keyString = keyStringBuilder.toString

        if (batchKeyStrings.contains(keyString))
          flushAndClearBatch(client, target.table, batch, batchKeyStrings, keyAttributeNames)

        if (isPut) {
          putCount += 1
          val finalItem = new util.HashMap[String, AttributeValue](item.size())
          val it = item.entrySet().iterator()
          while (it.hasNext) {
            val entry = it.next()
            val k = entry.getKey
            if (k != operationTypeColumn)
              finalItem.put(renamesMap.getOrElse(k, k), entry.getValue)
          }
          batch.add(
            WriteRequest
              .builder()
              .putRequest(PutRequest.builder().item(finalItem).build())
              .build()
          )
        } else {
          deleteCount += 1
          batch.add(
            WriteRequest
              .builder()
              .deleteRequest(DeleteRequest.builder().key(keyMap).build())
              .build()
          )
        }
        batchKeyStrings.add(keyString)

        if (batch.size() >= batchWriteItemLimit)
          flushAndClearBatch(client, target.table, batch, batchKeyStrings, keyAttributeNames)
      }

      if (!batch.isEmpty)
        flushAndClearBatch(client, target.table, batch, batchKeyStrings, keyAttributeNames)

      log.info(s"""
                  |Changes applied:
                  |  - $putCount items UPSERTED
                  |  - $deleteCount items DELETED
                  |""".stripMargin)
    }
  }

  /** Flush a batch of write requests using BatchWriteItem, retrying unprocessed items.
    *
    * Note: uses `Thread.sleep` for retry backoff, which blocks the calling thread (typically a
    * polling pool thread). The sleep durations are short (100ms-3000ms) and bounded by
    * `maxRetries`, so the impact on pool throughput is minimal.
    */
  private[writers] def flushAndClearBatch(
    client: DynamoDbClient,
    tableName: String,
    batch: util.ArrayList[WriteRequest],
    batchKeys: util.HashSet[String],
    keyAttributeNames: Set[String] = Set.empty
  ): Unit = {
    var unprocessed: util.List[WriteRequest] = batch
    var attempt = 0
    try
      while (!unprocessed.isEmpty) {
        val request = BatchWriteItemRequest
          .builder()
          .requestItems(Map(tableName -> unprocessed).asJava)
          .build()
        try {
          val response = client.batchWriteItem(request)
          val remaining = response.unprocessedItems()
          unprocessed =
            if (remaining != null && remaining.containsKey(tableName))
              remaining.get(tableName)
            else
              util.Collections.emptyList()
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            throw new RuntimeException("Interrupted during batch write retry backoff")
          case e: DynamoDbException
              if e.awsErrorDetails() != null &&
                retryableErrorCodes.contains(e.awsErrorDetails().errorCode()) =>
            // Transient error on batchWriteItem itself — treat the entire batch as
            // unprocessed so the retry loop below will back off and re-submit.
            log.warn(
              s"Transient error on batchWriteItem (${e.awsErrorDetails().errorCode()}), " +
                s"will retry ${unprocessed.size()} items"
            )
        }
        if (!unprocessed.isEmpty) {
          attempt += 1
          if (attempt > maxRetries) {
            val deadLetterKeys = unprocessed.asScala.map { wr =>
              if (wr.putRequest() != null) {
                val item = wr.putRequest().item()
                if (keyAttributeNames.nonEmpty) {
                  val keys = new util.HashMap[String, AttributeValue]()
                  keyAttributeNames.foreach { k =>
                    val v = item.get(k)
                    if (v != null) keys.put(k, v)
                  }
                  keys.toString
                } else item.keySet().toString
              } else if (wr.deleteRequest() != null) wr.deleteRequest().key().toString
              else "unknown"
            }
            val msg =
              s"Giving up on ${unprocessed.size()} unprocessed items after $attempt attempts"
            log.error(s"$msg — dead-letter items: ${deadLetterKeys.mkString("; ")}")
            // The caller (pollAndProcess) skips checkpointing on write failure,
            // so the entire poll cycle will be retried from the last checkpoint.
            throw new BatchWriteExhaustedException(msg, unprocessed.size())
          } else {
            val backoffMs =
              math.min(initialBackoffMs * (1L << attempt), maxBackoffMs)
            log.warn(
              s"${unprocessed.size()} unprocessed items, retrying after ${backoffMs}ms"
            )
            try Thread.sleep(backoffMs)
            catch {
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
                throw new RuntimeException("Interrupted during batch write retry backoff")
            }
          }
        }
      }
    finally {
      batch.clear()
      batchKeys.clear()
    }
  }
}
