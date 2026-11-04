package com.scylladb.migrator.config

import io.circe.Decoder

/** Contact points for CQL sources and targets.
  *
  * The `host` field accepts either a single host, a comma-separated list (`"a,b,c"`) or a YAML list
  * (`[a, b, c]`). Lists are normalized to the comma-separated form, so `host` stays a `String` in
  * the settings case classes and savepoint files round-trip unchanged. Giving several contact
  * points lets the driver open its first session even when one of them is down (e.g. during a
  * rolling restart).
  */
object ContactPoints {

  val decoder: Decoder[String] =
    Decoder.decodeString.or(Decoder.decodeList[String].map(_.mkString(",")))

  def split(host: String): List[String] =
    host.split(',').iterator.map(_.trim).filter(_.nonEmpty).toList

  def validate(label: String, host: String): List[String] =
    split(host) match {
      case Nil   => List(s"$label host must not be empty")
      case hosts => hosts.flatMap(HostValidation.validateHostOrIp(label, _))
    }
}
