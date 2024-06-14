package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class AWSCredentials(accessKey: String, secretKey: String, assumeRole: Option[AWSAssumeRole]) {
  override def toString: String = s"AWSCredentials(${accessKey.take(3)}..., <redacted>)"
}
object AWSCredentials {
  implicit val decoder: Decoder[AWSCredentials] = deriveDecoder
  implicit val encoder: Encoder[AWSCredentials] = deriveEncoder
}

case class AWSAssumeRole(arn: String, sessionName: Option[String]) {
  def getSessionName: String =
    sessionName.getOrElse(AWSAssumeRole.defaultSessionName)
}

object AWSAssumeRole {
  implicit val decoder: Decoder[AWSAssumeRole] = deriveDecoder
  implicit val encoder: Encoder[AWSAssumeRole] = deriveEncoder

  val defaultSessionName: String = "scylla-migrator"
}
