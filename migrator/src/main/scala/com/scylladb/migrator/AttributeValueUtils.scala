package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util.stream.Collectors
import java.util.{ Map => JMap }

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def fromV1(value: AttributeValueV1): AttributeValue =
    if (value.getS ne null) AttributeValue.fromS(value.getS)
    else if (value.getN ne null) AttributeValue.fromN(value.getN)
    else if (value.getB ne null) AttributeValue.fromB(SdkBytes.fromByteBuffer(value.getB))
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
    else if (value.getBOOL ne null) AttributeValue.fromBool(value.getBOOL)
    else sys.error("Unable to convert AttributeValue from AWS SDK V1 to V2")

}
