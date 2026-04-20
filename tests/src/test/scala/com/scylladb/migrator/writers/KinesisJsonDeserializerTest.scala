package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.amazonaws.services.kinesis.model.Record

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters._

/** Unit tests for [[KinesisJsonDeserializer]].
  *
  * These test the conversion of the JSON envelope emitted by Kinesis Data Streams for DynamoDB into
  * [[DynamoStreamReplication.StreamChange]] — the out-of-band tagged wrapper that
  * [[DynamoStreamReplication.run]] already knows how to apply. Both successful conversions and the
  * failure path (which must return `None` rather than crash the streaming application) are
  * exercised.
  *
  * The tests build synthetic JSON envelopes with hand-crafted type descriptors rather than
  * capturing real KDS records because:
  *   1. The wire format is stable (DynamoDB published it in 2020 and it has not changed).
  *   2. Capturing real records requires LocalStack Pro or a live AWS account and a table with the
  *      streaming destination already enabled — both are out of scope for a unit suite.
  */
class KinesisJsonDeserializerTest extends munit.FunSuite {

  /** Build a `Record` with the given UTF-8 JSON payload. The rest of the `Record` fields
    * (sequenceNumber, partitionKey, etc.) are left null because the deserializer only reads
    * `getData()`.
    */
  private def record(json: String): Record =
    new Record().withData(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))

  private def base64(bytes: Array[Byte]): String =
    Base64.getEncoder.encodeToString(bytes)

  test("INSERT with all primitive types (S, N, B, BOOL, NULL) is parsed correctly") {
    val payload =
      s"""
         |{
         |  "eventName": "INSERT",
         |  "eventSource": "aws:dynamodb",
         |  "dynamodb": {
         |    "Keys": { "pk": {"S": "item-1"} },
         |    "NewImage": {
         |      "pk":     {"S":    "item-1"},
         |      "count":  {"N":    "42"},
         |      "data":   {"B":    "${base64(Array[Byte](1, 2, 3, 4))}"},
         |      "active": {"BOOL": true},
         |      "empty":  {"NULL": true}
         |    }
         |  }
         |}
         |""".stripMargin

    val parsed = KinesisJsonDeserializer.parseRecord(record(payload))
    assert(parsed.isDefined, "A well-formed INSERT must decode successfully")
    val change = parsed.get
    val item = change.item.asScala

    assertEquals(item("pk"), new AttributeValueV1().withS("item-1"))
    assertEquals(item("count"), new AttributeValueV1().withN("42"))
    assertEquals(item("active"), new AttributeValueV1().withBOOL(true))
    assertEquals(item("empty"), new AttributeValueV1().withNULL(true))
    assertEquals(
      item("data").getB.array().toSeq,
      Array[Byte](1, 2, 3, 4).toSeq
    )
    assertEquals(
      change.op,
      DynamoStreamReplication.OpType.Put: DynamoStreamReplication.OpType,
      "INSERT must tag the StreamChange as Put"
    )
    assert(
      !item.contains("_dynamo_op_type"),
      "The item map must NOT contain the legacy _dynamo_op_type sentinel (LOGIC-1 regression)"
    )
  }

  test("MODIFY with set types (SS, NS, BS) preserves element order") {
    val payload =
      s"""
         |{
         |  "eventName": "MODIFY",
         |  "dynamodb": {
         |    "Keys": { "pk": {"S": "set-keeper"} },
         |    "NewImage": {
         |      "pk":      {"S":  "set-keeper"},
         |      "strings": {"SS": ["apple", "banana", "cherry"]},
         |      "numbers": {"NS": ["1", "2", "3"]},
         |      "blobs":   {"BS": ["${base64(Array[Byte](1))}", "${base64(Array[Byte](2))}"]}
         |    }
         |  }
         |}
         |""".stripMargin

    val change = KinesisJsonDeserializer.parseRecord(record(payload)).get
    val item = change.item.asScala

    assertEquals(
      item("strings").getSS.asScala.toList,
      List("apple", "banana", "cherry")
    )
    assertEquals(item("numbers").getNS.asScala.toList, List("1", "2", "3"))
    assertEquals(
      item("blobs").getBS.asScala.map(_.array().toSeq).toList,
      List(Seq[Byte](1), Seq[Byte](2))
    )
    assertEquals(
      change.op,
      DynamoStreamReplication.OpType.Put: DynamoStreamReplication.OpType,
      "MODIFY must tag the StreamChange as Put"
    )
  }

  test("REMOVE tags the StreamChange as Delete and skips NewImage") {
    // DDB REMOVE events in Kinesis typically carry only `Keys` (no NewImage). The deserializer
    // must accept the absent NewImage rather than crashing on the missing field.
    val payload =
      """
        |{
        |  "eventName": "REMOVE",
        |  "dynamodb": {
        |    "Keys": { "pk": {"S": "to-delete"} }
        |  }
        |}
        |""".stripMargin

    val change = KinesisJsonDeserializer.parseRecord(record(payload)).get
    val item = change.item.asScala

    assertEquals(item("pk"), new AttributeValueV1().withS("to-delete"))
    assertEquals(
      change.op,
      DynamoStreamReplication.OpType.Delete: DynamoStreamReplication.OpType,
      "REMOVE must tag the StreamChange as Delete"
    )
    assert(
      !item.contains("ignored-field"),
      "REMOVE must not fabricate attributes that were not in Keys"
    )
  }

  test("nested L and M are decoded recursively") {
    // Real DynamoDB items routinely nest L/M inside each other (e.g., a list of addresses with
    // each address as a map). The deserializer must recurse all the way down.
    val payload =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys": { "pk": {"S": "nested"} },
        |    "NewImage": {
        |      "pk": {"S": "nested"},
        |      "addresses": {
        |        "L": [
        |          {"M": {
        |            "street": {"S": "100 Main"},
        |            "tags":   {"SS": ["home", "primary"]}
        |          }},
        |          {"M": {
        |            "street": {"S": "500 Office"},
        |            "floors": {"L": [{"N": "1"}, {"N": "2"}]}
        |          }}
        |        ]
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

    val item = KinesisJsonDeserializer.parseRecord(record(payload)).get.item.asScala
    val addresses = item("addresses").getL.asScala.toList
    assertEquals(addresses.size, 2)

    val addr0 = addresses.head.getM.asScala
    assertEquals(addr0("street"), new AttributeValueV1().withS("100 Main"))
    assertEquals(addr0("tags").getSS.asScala.toList, List("home", "primary"))

    val addr1 = addresses(1).getM.asScala
    assertEquals(addr1("street"), new AttributeValueV1().withS("500 Office"))
    assertEquals(
      addr1("floors").getL.asScala.map(_.getN).toList,
      List("1", "2")
    )
  }

  test("N is decoded from a JSON string (for precision) — JSON numbers are rejected") {
    // DynamoDB N values are transmitted as strings to avoid float-precision loss. A JSON number
    // would silently truncate a 30-digit N, so the deserializer refuses the record.
    val bad =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys":     { "pk": {"S": "bad-n"} },
        |    "NewImage": { "pk": {"S": "bad-n"}, "count": {"N": 42} }
        |  }
        |}
        |""".stripMargin

    assertEquals(
      KinesisJsonDeserializer.parseRecord(record(bad)),
      None,
      "An N with a JSON number (not a string) must drop the record rather than truncate"
    )
  }

  test("NULL with value `false` is rejected (spec says NULL is always true)") {
    val bad =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys":     { "pk": {"S": "bad-null"} },
        |    "NewImage": { "pk": {"S": "bad-null"}, "field": {"NULL": false} }
        |  }
        |}
        |""".stripMargin

    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("unknown type descriptor is rejected") {
    val bad =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys":     { "pk": {"S": "bad-desc"} },
        |    "NewImage": { "pk": {"S": "bad-desc"}, "x": {"ZZ": "nope"} }
        |  }
        |}
        |""".stripMargin

    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("two type descriptors in one attribute value is rejected") {
    // Protects against ambiguous encodings — DynamoDB's format defines exactly one descriptor
    // per value, and silently picking the first would quietly corrupt data.
    val bad =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys":     { "pk": {"S": "bad"} },
        |    "NewImage": { "pk": {"S": "bad"}, "x": {"S": "a", "N": "1"} }
        |  }
        |}
        |""".stripMargin

    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("unknown eventName is dropped (does not crash)") {
    // DDB could one day introduce a new eventName (e.g., 'TRUNCATE'). The deserializer must not
    // crash on it — it must drop and keep consuming.
    val bad =
      """
        |{
        |  "eventName": "TRUNCATE",
        |  "dynamodb": {
        |    "Keys": { "pk": {"S": "ignored"} }
        |  }
        |}
        |""".stripMargin

    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("missing dynamodb.Keys is dropped") {
    // Without Keys, the target row cannot be identified (especially for REMOVE). Drop safely.
    val bad =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "NewImage": { "pk": {"S": "orphan"} }
        |  }
        |}
        |""".stripMargin

    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("missing dynamodb field is dropped") {
    val bad = """{"eventName": "INSERT"}"""
    assertEquals(KinesisJsonDeserializer.parseRecord(record(bad)), None)
  }

  test("non-JSON payload is dropped") {
    // A producer could misconfigure and write plain text instead of JSON. The stream cannot
    // crash because of it; we drop and move on.
    assertEquals(KinesisJsonDeserializer.parseRecord(record("not-json-at-all")), None)
  }

  test("empty payload is dropped") {
    assertEquals(KinesisJsonDeserializer.parseRecord(record("")), None)
  }

  test("direct ByteBuffer (non-array-backed) is handled by the defensive copy") {
    // Some Kinesis clients hand us a direct buffer; ByteBuffer.array() would throw on those.
    // The deserializer must cope by duplicating and draining the buffer regardless.
    val json = """{"eventName":"INSERT","dynamodb":{"Keys":{"pk":{"S":"direct"}}}}"""
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    val direct = ByteBuffer.allocateDirect(bytes.length)
    direct.put(bytes)
    direct.flip()
    val rec = new Record().withData(direct)

    val item = KinesisJsonDeserializer.parseRecord(rec).get.item.asScala
    assertEquals(item("pk"), new AttributeValueV1().withS("direct"))
  }

  test("jsonToAttributeValue: B decodes valid base64 into a ByteBuffer with matching bytes") {
    import io.circe.parser
    val raw = Array[Byte](1, 2, 3, 4, 5)
    val json = parser.parse(s"""{"B": "${base64(raw)}"}""").toOption.get
    val av = KinesisJsonDeserializer.jsonToAttributeValue(json)
    val bb = av.getB
    val got = new Array[Byte](bb.remaining())
    bb.duplicate().get(got)
    assertEquals(got.toSeq, raw.toSeq)
  }

  test("source attribute named `_dynamo_op_type` round-trips as user data (LOGIC-1)") {
    // A Kinesis envelope whose NewImage contains an attribute literally named `_dynamo_op_type`
    // used to silently flip put->delete under the in-band marker. After the out-of-band refactor
    // the attribute must pass through as plain user data and the op-type is carried on the
    // wrapper, not inside the item.
    val payload =
      """
        |{
        |  "eventName": "INSERT",
        |  "dynamodb": {
        |    "Keys":     { "pk": {"S": "collision-row"} },
        |    "NewImage": {
        |      "pk":              {"S":    "collision-row"},
        |      "_dynamo_op_type": {"BOOL": false},
        |      "value":           {"S":    "keep-me"}
        |    }
        |  }
        |}
        |""".stripMargin

    val change = KinesisJsonDeserializer.parseRecord(record(payload)).get
    assertEquals(
      change.op,
      DynamoStreamReplication.OpType.Put: DynamoStreamReplication.OpType,
      "INSERT must remain a Put even when the item has an attribute named like the former marker"
    )
    val item = change.item.asScala
    assertEquals(
      item("_dynamo_op_type"),
      new AttributeValueV1().withBOOL(false),
      "User-controlled attribute must round-trip unchanged"
    )
    assertEquals(item("value"), new AttributeValueV1().withS("keep-me"))
  }
}
