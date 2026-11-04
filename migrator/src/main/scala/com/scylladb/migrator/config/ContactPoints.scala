package com.scylladb.migrator.config

/** A CQL contact point. IPv6 addresses are kept without brackets. */
final case class ContactPoint(host: String, port: Int)

/** Parses the `host` field of a CQL source or target. Like the connector's
  * `spark.cassandra.connection.host`, it may list several contact points, comma-separated, each
  * `host` or `host:port`; entries without a port use the configured `port`. An IPv6 address with a
  * port must be bracketed (`[2001:db8::1]:9042`).
  *
  * The driver tries the contact points until one answers, so listing several private endpoint
  * addresses (one per availability zone, for example) keeps sessions opening while one of them, or
  * the node behind it, is unavailable.
  */
object ContactPoints {

  /** @return
    *   the contact points, or a description of every invalid entry
    */
  def parse(
    label: String,
    hosts: String,
    defaultPort: Int
  ): Either[List[String], List[ContactPoint]] = {
    val parsed =
      hosts.split(",", -1).toList.map(entry => parseEntry(label, entry.trim, defaultPort))
    val errors = parsed.collect { case Left(entryErrors) => entryErrors }.flatten
    if (errors.nonEmpty) Left(errors) else Right(parsed.collect { case Right(point) => point })
  }

  private def parseEntry(
    label: String,
    entry: String,
    defaultPort: Int
  ): Either[List[String], ContactPoint] = {
    val hostAndPort: (String, Either[String, Int]) =
      if (entry.startsWith("[") && entry.contains("]")) {
        val end = entry.indexOf(']')
        entry.drop(end + 1) match {
          case "" => (entry.take(end + 1), Right(defaultPort))
          case rest if rest.startsWith(":") =>
            (entry.take(end + 1), parsePort(label, entry, rest.drop(1)))
          case _ =>
            (
              entry.take(end + 1),
              Left(s"$label host '$entry': expected '[address]' or '[address]:port'")
            )
        }
      } else if (entry.count(_ == ':') == 1) {
        val separator = entry.indexOf(':')
        (entry.take(separator), parsePort(label, entry, entry.drop(separator + 1)))
      } else
        // No port, or a bare IPv6 address, which cannot carry one.
        (entry, Right(defaultPort))
    val (host, port) = hostAndPort
    val errors = HostValidation.validateHostOrIp(label, host) ++
      port.fold(List(_), HostValidation.validatePort(label, _))
    if (errors.nonEmpty) Left(errors)
    else Right(ContactPoint(host.stripPrefix("[").stripSuffix("]"), port.getOrElse(defaultPort)))
  }

  private def parsePort(label: String, entry: String, port: String): Either[String, Int] =
    port.toIntOption.toRight(s"$label host '$entry' has an invalid port '$port'")
}
