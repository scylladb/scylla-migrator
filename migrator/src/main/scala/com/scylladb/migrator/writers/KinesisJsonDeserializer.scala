package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.amazonaws.services.kinesis.model.Record
import io.circe.{ parser, Json, JsonObject }

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64
import scala.jdk.CollectionConverters._

/** Convert a raw Kinesis Data Streams for DynamoDB record into the same internal shape produced by
  * the DynamoDB Streams path in [[DynamoStreamReplication.createDStream]] — a
  * [[DynamoStreamReplication.StreamChange]] tagging the decoded attribute map with the operation
  * that produced it. The shared [[DynamoStreamReplication.run]] then applies both streams with
  * identical code.
  *
  * Design notes:
  *
  *   - `com.amazonaws.services.dynamodbv2.model.AttributeValue` (AWS SDK v1) is the target type
  *     because the existing DDB-streams path already emits that type; using SDK v2 here would force
  *     a type conversion inside `run` and change the DDB path.
  *   - `com.amazonaws.services.kinesis.model.Record` (SDK v1) is the record shape exposed by
  *     Spark's `spark-streaming-kinesis-asl`. Both types are transitively on the classpath via
  *     `spark-kinesis-dynamodb` → `dynamodb-streams-kinesis-adapter` and
  *     `spark-streaming-kinesis-asl` respectively. No new library dependency is needed.
  *   - Binary attributes (`B` and `BS`) arrive as base64 strings in this JSON format (unlike the
  *     SDK-v1 typed path where `ByteBuffer` is already decoded). We decode them here.
  *   - Errors are always swallowed into `None` so a single malformed record cannot crash the
  *     streaming application. Per-record logging is deliberately omitted — the untrusted record
  *     bytes are user data (often containing PII) and must never reach ops tooling. This matches
  *     the DDB-streams path's silent-drop behavior (`case _ => None` in its `messageHandler`).
  *     [[DynamoStreamReplication]] surfaces the total drop count via its
  *     [[org.apache.spark.util.LongAccumulator]], emitted in the per-batch summary, so operators
  *     can still distinguish "stream is quiet" from "stream is full of garbage".
  *   - The op-type tag is attached out-of-band via the [[DynamoStreamReplication.StreamChange]]
  *     wrapper. It used to be written into the item map under the reserved attribute name
  *     `_dynamo_op_type`, but that silently collided with any user attribute of the same name,
  *     flipping put↔delete on the affected items. The wrapper cannot collide with user data.
  */
object KinesisJsonDeserializer {

  /** Shape of the JSON envelope emitted by Kinesis Data Streams for DynamoDB.
    *
    * See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/kds_gettingstarted.html
    *
    * Only the three fields we consume are modeled explicitly; the rest (awsRegion, eventID,
    * tableName, userIdentity, recordFormat, eventSource, dynamodb.ApproximateCreationDateTime,
    * dynamodb.SizeBytes, dynamodb.OldImage) are ignored.
    */
  private val KnownEventNames: Set[String] = Set("INSERT", "MODIFY", "REMOVE")

  /** Parse a Kinesis record into a [[DynamoStreamReplication.StreamChange]]. Any parse failure
    * (non-JSON bytes, missing fields, unknown event name, malformed type descriptor) returns
    * `None`; the caller silently drops the record and increments the drop-count accumulator that is
    * emitted in [[DynamoStreamReplication.run]]'s per-batch summary. No payload bytes are logged
    * (see class-level docstring for the PII rationale).
    */
  def parseRecord(record: Record): Option[DynamoStreamReplication.StreamChange] = {
    val raw = extractBytes(record)
    parseJsonBytes(raw).flatMap(parseEnvelope)
  }

  private def extractBytes(record: Record): Array[Byte] = {
    val buffer = record.getData
    // ByteBuffer.array() is not guaranteed to work for direct buffers or non-backing-array slices,
    // so we make a defensive copy that always works regardless of the buffer's concrete type.
    val duplicate = buffer.duplicate()
    val bytes = new Array[Byte](duplicate.remaining())
    duplicate.get(bytes)
    bytes
  }

  private def parseJsonBytes(bytes: Array[Byte]): Option[Json] =
    parser.parse(new String(bytes, StandardCharsets.UTF_8)).toOption

  private def parseEnvelope(json: Json): Option[DynamoStreamReplication.StreamChange] = {
    val cursor = json.hcursor
    val eventName = cursor.get[String]("eventName").toOption.getOrElse("<missing>")
    if (!KnownEventNames.contains(eventName)) return None

    val dynamoField = cursor.downField("dynamodb")
    if (dynamoField.failed) return None

    val keys = dynamoField.get[JsonObject]("Keys").toOption
    val newImage = dynamoField.get[JsonObject]("NewImage").toOption
    if (keys.isEmpty) return None

    val merged = new util.HashMap[String, AttributeValueV1]()
    try {
      // Merge NewImage first, then Keys (Keys wins on conflict — which matches the DDB-streams
      // path's `newMap.putAll(newImage); newMap.putAll(keys)` ordering).
      for (imageObj <- newImage) mergeInto(merged, imageObj)
      for (keysObj <- keys) mergeInto(merged, keysObj)

      val op = eventName match {
        case "INSERT" | "MODIFY" => DynamoStreamReplication.OpType.Put
        case "REMOVE"            => DynamoStreamReplication.OpType.Delete
      }
      Some(DynamoStreamReplication.StreamChange(merged, op))
    } catch {
      case _: DeserializationException => None
    }
  }

  private def mergeInto(
    target: util.Map[String, AttributeValueV1],
    source: JsonObject
  ): Unit =
    source.toIterable.foreach { case (attrName, typedJson) =>
      target.put(attrName, jsonToAttributeValue(typedJson))
    }

  /** Convert a `{typeDescriptor: value}` JSON object — as emitted by Kinesis Data Streams for
    * DynamoDB — into the SDK-v1 `AttributeValue` the rest of the pipeline already consumes.
    *
    * Throws [[DeserializationException]] on anything malformed (unknown descriptor, wrong shape,
    * invalid base64). Callers catch this at the per-record boundary and drop the record.
    */
  def jsonToAttributeValue(json: Json): AttributeValueV1 = {
    val obj = json.asObject.getOrElse(
      throw new DeserializationException(s"Expected a {type: value} object, got: ${json.noSpaces}")
    )
    val entries = obj.toList
    if (entries.size != 1) {
      throw new DeserializationException(
        s"Expected exactly one type descriptor in ${json.noSpaces}, got ${entries.size}"
      )
    }
    val (descriptor, value) = entries.head
    descriptor match {
      case "S" =>
        val s = value.asString.getOrElse(
          throw new DeserializationException(s"S requires a JSON string, got: ${value.noSpaces}")
        )
        new AttributeValueV1().withS(s)

      case "N" =>
        // DynamoDB N is transmitted as a JSON string (precision is preserved by avoiding float).
        val n = value.asString.getOrElse(
          throw new DeserializationException(
            s"N requires a JSON string (for precision), got: ${value.noSpaces}"
          )
        )
        new AttributeValueV1().withN(n)

      case "B" =>
        val b = value.asString.getOrElse(
          throw new DeserializationException(
            s"B requires a base64 JSON string, got: ${value.noSpaces}"
          )
        )
        new AttributeValueV1().withB(decodeBase64(b))

      case "BOOL" =>
        val b = value.asBoolean.getOrElse(
          throw new DeserializationException(
            s"BOOL requires a JSON boolean, got: ${value.noSpaces}"
          )
        )
        new AttributeValueV1().withBOOL(b)

      case "NULL" =>
        // DynamoDB NULL's canonical value is `true`; `false` is not valid per the spec.
        val b = value.asBoolean.getOrElse(
          throw new DeserializationException(
            s"NULL requires a JSON boolean, got: ${value.noSpaces}"
          )
        )
        if (!b) {
          throw new DeserializationException(
            s"NULL must be true per the DynamoDB JSON spec, got: ${value.noSpaces}"
          )
        }
        new AttributeValueV1().withNULL(true)

      case "SS" =>
        val arr = value.asArray.getOrElse(
          throw new DeserializationException(s"SS requires a JSON array, got: ${value.noSpaces}")
        )
        val strings = arr.map(j =>
          j.asString.getOrElse(
            throw new DeserializationException(
              s"SS elements must be JSON strings, got: ${j.noSpaces}"
            )
          )
        )
        new AttributeValueV1().withSS(strings.asJava)

      case "NS" =>
        val arr = value.asArray.getOrElse(
          throw new DeserializationException(s"NS requires a JSON array, got: ${value.noSpaces}")
        )
        val numbers = arr.map(j =>
          j.asString.getOrElse(
            throw new DeserializationException(
              s"NS elements must be JSON strings, got: ${j.noSpaces}"
            )
          )
        )
        new AttributeValueV1().withNS(numbers.asJava)

      case "BS" =>
        val arr = value.asArray.getOrElse(
          throw new DeserializationException(s"BS requires a JSON array, got: ${value.noSpaces}")
        )
        val bytes = arr.map(j =>
          decodeBase64(
            j.asString.getOrElse(
              throw new DeserializationException(
                s"BS elements must be base64 JSON strings, got: ${j.noSpaces}"
              )
            )
          )
        )
        new AttributeValueV1().withBS(bytes.asJava)

      case "L" =>
        val arr = value.asArray.getOrElse(
          throw new DeserializationException(s"L requires a JSON array, got: ${value.noSpaces}")
        )
        val converted = arr.map(jsonToAttributeValue).toList
        new AttributeValueV1().withL(converted.asJava)

      case "M" =>
        val inner = value.asObject.getOrElse(
          throw new DeserializationException(s"M requires a JSON object, got: ${value.noSpaces}")
        )
        val javaMap = new util.HashMap[String, AttributeValueV1]()
        inner.toIterable.foreach { case (k, v) => javaMap.put(k, jsonToAttributeValue(v)) }
        new AttributeValueV1().withM(javaMap)

      case other =>
        throw new DeserializationException(
          s"Unknown DynamoDB type descriptor '$other'. " +
            "Valid descriptors: S, N, B, BOOL, NULL, SS, NS, BS, L, M."
        )
    }
  }

  private def decodeBase64(encoded: String): ByteBuffer =
    try ByteBuffer.wrap(Base64.getDecoder.decode(encoded))
    catch {
      case _: IllegalArgumentException =>
        throw new DeserializationException(s"Invalid base64 encoding: '$encoded'")
    }

  /** Internal exception used only to carry a structured error from the deep recursive descent in
    * [[jsonToAttributeValue]] up to [[parseEnvelope]] where it is caught and the record is dropped.
    * It is NOT thrown across package boundaries.
    */
  private final class DeserializationException(msg: String) extends RuntimeException(msg)
}
