package com.scylladb.migrator.readers

import com.aerospike.client.{ AerospikeException, Host }

/** Interpret seed specifications locally using the same grammar as the Aerospike client. */
private[migrator] object AerospikeHosts {
  def parse(specifications: Seq[String], defaultPort: Int): Array[Host] = {
    require(specifications.nonEmpty, "Aerospike 'hosts' must not be empty")
    require(defaultPort > 0 && defaultPort <= 65535, s"Invalid Aerospike port: $defaultPort")
    specifications.zipWithIndex.flatMap { case (raw, index) =>
      val prefix = s"Invalid Aerospike hosts[$index] '$raw'"
      require(raw != null && raw.trim.nonEmpty, s"$prefix: host specification must not be blank")
      val parsed =
        try Host.parseHosts(raw, defaultPort)
        catch {
          case error: AerospikeException =>
            throw new IllegalArgumentException(
              s"$prefix: invalid seed syntax: ${error.getMessage}",
              error
            )
        }
      require(parsed.nonEmpty, s"$prefix: no hosts were parsed")
      parsed.foreach { host =>
        require(host.name.trim.nonEmpty, s"$prefix: parsed host name must not be blank")
        require(host.port > 0 && host.port <= 65535, s"$prefix: invalid parsed port ${host.port}")
      }
      parsed
    }.toArray
  }
}
