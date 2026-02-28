package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.log4j.LogManager
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchWriteItemRequest,
  DeleteRequest,
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
object BatchWriter {
  private val log = LogManager.getLogger("com.scylladb.migrator.writers.BatchWriter")

  /** Maximum number of items in a single DynamoDB BatchWriteItem request. */
  private[writers] val batchWriteItemLimit = 25

  /** Backoff parameters for flushBatch retries: initial delay, max delay, max attempts. */
  private val flushBatchInitialBackoffMs = 100L
  private val flushBatchMaxBackoffMs = 3000L
  private val flushBatchMaxRetries = 5

  type DynamoItem = util.Map[String, AttributeValue]

  private[writers] val operationTypeColumn = "_dynamo_op_type"
  private[writers] val putOperation = AttributeValue.fromBool(true)
  private[writers] val deleteOperation = AttributeValue.fromBool(false)

  /** Write a batch of stream change items to the target table. Items are grouped into
    * BatchWriteItem requests of up to 25 items each. Duplicate keys within a batch trigger an early
    * flush to preserve ordering.
    */
  private[writers] def run(
    items: Iterable[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription,
    client: DynamoDbClient
  ): Unit = {
    val msgs = items.flatten
    if (msgs.isEmpty) {
      log.info("No changes to apply")
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
          flushBatch(client, target.table, batch, batchKeyStrings)

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
          flushBatch(client, target.table, batch, batchKeyStrings)
      }

      if (!batch.isEmpty)
        flushBatch(client, target.table, batch, batchKeyStrings)

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
    * `flushBatchMaxRetries`, so the impact on pool throughput is minimal.
    */
  private[writers] def flushBatch(
    client: DynamoDbClient,
    tableName: String,
    batch: util.ArrayList[WriteRequest],
    batchKeys: util.HashSet[String]
  ): Unit = {
    var unprocessed: util.List[WriteRequest] = batch
    var attempt = 0
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
        if (!unprocessed.isEmpty) {
          attempt += 1
          if (attempt > flushBatchMaxRetries) {
            val deadLetterKeys = unprocessed.asScala.map { wr =>
              val key =
                if (wr.putRequest() != null) wr.putRequest().item().toString
                else if (wr.deleteRequest() != null) wr.deleteRequest().key().toString
                else "unknown"
              key
            }
            val msg =
              s"Giving up on ${unprocessed.size()} unprocessed items after $attempt attempts"
            log.error(s"$msg — dead-letter items: ${deadLetterKeys.mkString("; ")}")
            throw new RuntimeException(msg)
          } else {
            val backoffMs =
              math.min(flushBatchInitialBackoffMs * (1L << attempt), flushBatchMaxBackoffMs)
            log.warn(
              s"${unprocessed.size()} unprocessed items, retrying after ${backoffMs}ms"
            )
            Thread.sleep(backoffMs)
          }
        }
      } catch {
        case e: Exception =>
          log.error(s"Failed to batch write to $tableName", e)
          throw e
      }
    }
    batch.clear()
    batchKeys.clear()
  }
}
