package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

sealed trait ParquetProcessingMode
object ParquetProcessingMode {
  case object Parallel extends ParquetProcessingMode
  case object Sequential extends ParquetProcessingMode

  implicit val encoder: Encoder[ParquetProcessingMode] = Encoder.encodeString.contramap {
    case Parallel   => "parallel"
    case Sequential => "sequential"
  }

  implicit val decoder: Decoder[ParquetProcessingMode] = Decoder.decodeString.emap {
    case "parallel"   => Right(Parallel)
    case "sequential" => Right(Sequential)
    case other =>
      Left(s"Unknown parquet processing mode: $other. Valid values: parallel, sequential")
  }
}

case class Savepoints(intervalSeconds: Int,
                      path: String,
                      parquetProcessingMode: Option[ParquetProcessingMode]) {
  def getParquetProcessingMode: ParquetProcessingMode =
    parquetProcessingMode.getOrElse(ParquetProcessingMode.Parallel)
}

object Savepoints {
  implicit val encoder: Encoder[Savepoints] = deriveEncoder[Savepoints]
  implicit val decoder: Decoder[Savepoints] = deriveDecoder[Savepoints]
}
