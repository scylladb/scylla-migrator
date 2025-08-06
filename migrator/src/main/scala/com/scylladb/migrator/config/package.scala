package com.scylladb.migrator

import io.circe.Decoder
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import io.circe.Encoder

package object config {
  object BillingModeCodec {
    implicit val billingModeDecoder: Decoder[BillingMode] =
      Decoder.decodeString.emap { billingModeStr =>
        Option(BillingMode.fromValue(billingModeStr))
          .toRight(s"Invalid billing mode: ${billingModeStr}")
      }
    implicit val billingModeEncoder: Encoder[BillingMode] =
      Encoder.encodeString.contramap(_.toString)
  }
}
