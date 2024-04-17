package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import scala.jdk.CollectionConverters._

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def stringValue(value: String): AttributeValue =
    new AttributeValue().withS(value)

  def numericalValue(value: String): AttributeValue =
    new AttributeValue().withN(value)

  def boolValue(value: Boolean): AttributeValue =
    new AttributeValue().withBOOL(value)

  def mapValue(items: (String, AttributeValue)*): AttributeValue =
    new AttributeValue().withM(items.toMap.asJava)

}
