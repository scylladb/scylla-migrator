package com.scylladb.migrator.config

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class AWSCredentials(accessKey: String, secretKey: String)
    extends AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)) {
  override def toString: String = s"AWSCredentials(${accessKey.take(3)}..., <redacted>)"
}
object AWSCredentials {
  implicit val decoder: Decoder[AWSCredentials] = deriveDecoder
  implicit val encoder: Encoder[AWSCredentials] = deriveEncoder
}
