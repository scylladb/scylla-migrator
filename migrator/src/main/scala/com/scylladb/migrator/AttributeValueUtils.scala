package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def binaryValue(bytes: Array[Byte]): AttributeValue =
    binaryValue(ByteBuffer.wrap(bytes))

  def binaryValue(byteBuffer: ByteBuffer): AttributeValue =
    new AttributeValue().withB(byteBuffer)

  def binaryValues(byteBuffers: ByteBuffer*): AttributeValue =
    new AttributeValue().withBS(byteBuffers: _*)

  def stringValue(value: String): AttributeValue =
    new AttributeValue().withS(value)

  def stringValues(values: String*): AttributeValue =
    new AttributeValue().withSS(values: _*)

  def numericalValue(value: String): AttributeValue =
    new AttributeValue().withN(value)

  def numericalValues(values: String*): AttributeValue =
    new AttributeValue().withNS(values: _*)

  def boolValue(value: Boolean): AttributeValue =
    new AttributeValue().withBOOL(value)

  def listValue(items: AttributeValue*): AttributeValue =
    new AttributeValue().withL(items: _*)

  def mapValue(items: (String, AttributeValue)*): AttributeValue =
    new AttributeValue().withM(items.toMap.asJava)

  def mapValue(items: Map[String, AttributeValue]): AttributeValue =
    new AttributeValue().withM(items.asJava)

  val nullValue: AttributeValue =
    new AttributeValue().withNULL(true)

}
