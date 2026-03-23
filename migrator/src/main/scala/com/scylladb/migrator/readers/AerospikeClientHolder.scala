package com.scylladb.migrator.readers

import com.aerospike.client.AerospikeClient
import org.apache.logging.log4j.LogManager
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/** Connection parameters used as cache key in AerospikeClientHolder.
  *
  * Credentials are stored as a SHA-256 hex digest rather than plaintext to avoid retaining raw
  * passwords in memory (heap dumps, hashCode/equals). The actual credentials are passed separately
  * to `buildClient`.
  */
private[migrator] case class AerospikeConnectionKey(
  hosts: List[String],
  port: Int,
  credentialHash: Option[String],
  connectTimeoutMs: Option[Int],
  socketTimeoutMs: Option[Int],
  totalTimeoutMs: Option[Int],
  tlsName: Option[String],
  maxConnsPerNode: Option[Int],
  connPoolsPerNode: Option[Int]
)

private[migrator] object AerospikeConnectionKey {

  /** Build a connection key from config and credentials, hashing credentials once. */
  def fromConfig(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): AerospikeConnectionKey =
    AerospikeConnectionKey(
      connConfig.hosts,
      connConfig.port,
      hashCredentials(credentials),
      connConfig.connectTimeoutMs,
      connConfig.socketTimeoutMs,
      connConfig.totalTimeoutMs,
      connConfig.tlsName,
      connConfig.maxConnsPerNode,
      connConfig.connPoolsPerNode
    )

  /** Hash credentials to avoid storing plaintext passwords in the cache key. */
  def hashCredentials(credentials: Option[(String, String)]): Option[String] =
    credentials.map { case (user, pass) =>
      val digest = MessageDigest.getInstance("SHA-256")
      digest.update(user.getBytes("UTF-8"))
      digest.update(0.toByte) // separator
      digest.update(pass.getBytes("UTF-8"))
      digest.digest().map("%02x".format(_)).mkString
    }
}

/** Shares AerospikeClient instances per executor, keyed by connection parameters. The client is
  * thread-safe and manages its own internal connection pool. Multiple concurrent configurations are
  * supported (e.g., different hosts or credentials).
  *
  * Marked Serializable as a safety net for cluster mode, even though it should only be accessed
  * inside RDD.compute() on executor JVMs, never serialized across the wire.
  */
private[migrator] object AerospikeClientHolder extends Serializable {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.AerospikeClientHolder")
  @transient private lazy val clients =
    new ConcurrentHashMap[AerospikeConnectionKey, AerospikeClient]()
  @volatile private var closed = false

  @transient private var shutdownHook: Thread = _

  private def ensureHookRegistered(): Unit = synchronized {
    if (closed) throw new IllegalStateException("AerospikeClientHolder has been shut down")
    if (shutdownHook == null) {
      shutdownHook = new Thread(() => closeAll())
      Runtime.getRuntime.addShutdownHook(shutdownHook)
    }
  }

  private def closeAll(): Unit = synchronized {
    closed = true
    clients.forEach { (_, c) =>
      if (c != null && c.isConnected) c.close()
    }
    clients.clear()
  }

  /** For tests only — close all clients and allow the holder to be reused. */
  private[migrator] def reset(): Unit = synchronized {
    closeAll()
    if (shutdownHook != null) {
      try Runtime.getRuntime.removeShutdownHook(shutdownHook)
      catch { case _: IllegalStateException => }
      shutdownHook = null
    }
    closed = false
  }

  /** Remove and close a cached client for the given connection config and credentials. Safe to call
    * even if no client is cached for the key. Useful for task cleanup and tests to prevent
    * unbounded growth of the client map.
    */
  def release(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): Unit = {
    val key = AerospikeConnectionKey.fromConfig(connConfig, credentials)
    val removed = clients.remove(key)
    if (removed != null) {
      try removed.close()
      catch { case e: Exception => log.debug("Error closing Aerospike client during release", e) }
    }
  }

  /** Get or create an AerospikeClient for the given connection config and credentials. */
  def get(
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): AerospikeClient =
    get(AerospikeConnectionKey.fromConfig(connConfig, credentials), connConfig, credentials)

  /** Get or create an AerospikeClient using a pre-computed connection key. Avoids re-hashing
    * credentials on every call when the key is already known.
    */
  def get(
    key: AerospikeConnectionKey,
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): AerospikeClient = {
    ensureHookRegistered()
    clients.compute(
      key,
      (_, existing) => {
        if (closed)
          throw new IllegalStateException("AerospikeClientHolder has been shut down")
        if (existing != null && existing.isConnected) existing
        else {
          if (existing != null) existing.close()
          val newClient = Aerospike.buildClient(connConfig, credentials)
          // Guard against TOCTOU race: closeAll() may have fired between
          // ensureHookRegistered() and this point, clearing all clients.
          if (closed) {
            newClient.close()
            throw new IllegalStateException(
              "AerospikeClientHolder was shut down during client creation"
            )
          }
          newClient
        }
      }
    )
  }
}
