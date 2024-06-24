package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.nio.ByteBuffer
import java.util.stream.Collectors
import java.util.{ Map => JMap }
import scala.jdk.CollectionConverters._

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def binaryValue(bytes: Array[Byte]): AttributeValue =
    binaryValue(ByteBuffer.wrap(bytes))

  // Fixme simplify
  def binaryValue(byteBuffer: ByteBuffer): AttributeValue =
    AttributeValue.fromB(SdkBytes.fromByteBuffer(byteBuffer))

  def binaryValues(byteBuffers: ByteBuffer*): AttributeValue =
    AttributeValue.fromBs(byteBuffers.map(SdkBytes.fromByteBuffer).asJava)

  def stringValue(value: String): AttributeValue =
    AttributeValue.fromS(value)

  def stringValues(values: String*): AttributeValue =
    AttributeValue.fromSs(values.asJava)

  def numericalValue(value: String): AttributeValue =
    AttributeValue.fromN(value)

  def numericalValues(values: String*): AttributeValue =
    AttributeValue.fromNs(values.asJava)

  def boolValue(value: Boolean): AttributeValue =
    AttributeValue.fromBool(value)

  def listValue(items: AttributeValue*): AttributeValue =
    AttributeValue.fromL(items.asJava)

  def mapValue(items: (String, AttributeValue)*): AttributeValue =
    AttributeValue.fromM(items.toMap.asJava)

  def mapValue(items: Map[String, AttributeValue]): AttributeValue =
    AttributeValue.fromM(items.asJava)

  val nullValue: AttributeValue =
    AttributeValue.fromNul(true)

  def fromV1(value: AttributeValueV1): AttributeValue =
    if (value.getS ne null) stringValue(value.getS)
    else if (value.getN ne null) numericalValue(value.getN)
    else if (value.getB ne null) binaryValue(value.getB)
    else if (value.getSS ne null) AttributeValue.fromSs(value.getSS)
    else if (value.getNS ne null) AttributeValue.fromNs(value.getNS)
    else if (value.getBS ne null)
      AttributeValue.fromBs(
        value.getBS.stream().map(SdkBytes.fromByteBuffer).collect(Collectors.toList[SdkBytes]))
    else if (value.getM ne null) {
      AttributeValue.fromM(
        value.getM
          .entrySet()
          .stream()
          .collect(
            Collectors.toMap(
              (entry: JMap.Entry[String, AttributeValueV1]) => entry.getKey,
              (entry: JMap.Entry[String, AttributeValueV1]) => fromV1(entry.getValue)
            )
          )
      )
    } else if (value.getL ne null)
      AttributeValue.fromL(
        value.getL.stream().map(fromV1).collect(Collectors.toList[AttributeValue]))
    else if (value.getNULL ne null) AttributeValue.fromNul(value.getNULL)
    else if (value.getBOOL ne null) boolValue(value.getBOOL)
    else sys.error("Unable to convert AttributeValue from AWS SDK V1 to V2")

}
