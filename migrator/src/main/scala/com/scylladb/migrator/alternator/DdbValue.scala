package com.scylladb.migrator.alternator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

/** A data type isomorphic to AWS’ `AttributeValue`, but effectively serializable. See
  * https://github.com/aws/aws-sdk-java-v2/issues/3143 The fact that `AttributeValue` is not
  * serializable anymore prevents us to use it in RDD operations that may perform shuffles. As a
  * workaround, we convert values of type `AttributeValue` to `DdbValue`.
  */
@SerialVersionUID(1L)
sealed trait DdbValue extends Serializable

object DdbValue {
  case class S(value: String) extends DdbValue
  case class N(value: String) extends DdbValue
  case class Bool(value: Boolean) extends DdbValue
  case class L(values: collection.Seq[DdbValue]) extends DdbValue
  case class Null(value: Boolean) extends DdbValue
  case class B(value: SdkBytes) extends DdbValue
  case class M(value: Map[String, DdbValue]) extends DdbValue
  case class Ss(values: collection.Seq[String]) extends DdbValue
  case class Ns(values: collection.Seq[String]) extends DdbValue
  case class Bs(values: collection.Seq[SdkBytes]) extends DdbValue

  def from(value: AttributeValue): DdbValue =
    if (value.s() != null) S(value.s())
    else if (value.n() != null) N(value.n())
    else if (value.bool() != null) Bool(value.bool())
    else if (value.hasL) L(value.l().asScala.map(from))
    else if (value.nul() != null) Null(value.nul())
    else if (value.b() != null) B(value.b())
    else if (value.hasM) M(value.m().asScala.view.mapValues(from).toMap)
    else if (value.hasSs) Ss(value.ss().asScala)
    else if (value.hasNs) Ns(value.ns().asScala)
    else if (value.hasBs) Bs(value.bs().asScala)
    else sys.error("Unknown AttributeValue type")
}
