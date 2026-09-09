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
  // Active-reference counts per key. A live reference pins the client against eviction; a client
  // with no references stays cached (see `releaseOne`) and is closed by the shutdown hook, by
  // `release`/`releaseAndClose`/`reset`, or by idle eviction when the cache grows past
  // MaxCachedClients.
  @transient private lazy val refCounts =
    new ConcurrentHashMap[AerospikeConnectionKey, Integer]()
  @volatile private var closed = false

  @transient private var shutdownHook: Thread = _

  private def ensureHookRegistered(): Unit = synchronized {
    if (closed) throw new IllegalStateException("AerospikeClientHolder has been shut down")
    if (shutdownHook == null) {
      shutdownHook = new Thread(() => closeAll())
      Runtime.getRuntime.addShutdownHook(shutdownHook)
    }
  }

  /** Maximum number of cached clients tolerated before idle ones are evicted. Keeps the cache
    * useful for the sequential tasks of one job while bounding growth across jobs that use distinct
    * connection configurations.
    */
  private val MaxCachedClients = 4

  private def closeQuietly(client: AerospikeClient, context: String): Unit =
    if (client != null)
      try client.close()
      catch { case e: Exception => log.debug(s"Error closing Aerospike client $context", e) }

  private def closeAll(): Unit = synchronized {
    closed = true
    // Close every cached client, connected or not: a client that lost its connection still owns
    // its tend thread and sockets, so skipping it would leak exactly the failed clients.
    clients.forEach((_, c) => closeQuietly(c, "during shutdown"))
    clients.clear()
    refCounts.clear()
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
    refCounts.remove(key)
    closeQuietly(clients.remove(key), "during release")
  }

  /** Acquire a client and increment its active-reference count. Every acquire MUST be paired with
    * exactly one `releaseOne` (e.g. via a TaskCompletionListener). Reaching zero references does
    * not close the client — see `releaseOne` — it only makes it eligible for eviction.
    */
  def acquire(
    key: AerospikeConnectionKey,
    connConfig: AerospikeConnectionConfig,
    credentials: Option[(String, String)]
  ): AerospikeClient = {
    refCounts.merge(key, 1, (a, b) => a + b)
    try get(key, connConfig, credentials)
    catch {
      case e: Throwable =>
        releaseOne(key)
        throw e
    }
  }

  /** Release one active reference.
    *
    * The client is deliberately left open once the count reaches zero: an executor processes many
    * partitions in sequence, and closing between tasks would repeat connection setup and cluster
    * discovery for every split. Cached clients are closed by the JVM shutdown hook, by `release` or
    * `reset`, or by idle eviction once more than `MaxCachedClients` are held.
    */
  def releaseOne(key: AerospikeConnectionKey): Unit = {
    val _ = refCounts.compute(
      key,
      (_, c) => {
        val n = (if (c == null) 0 else c.intValue) - 1
        if (n <= 0) null // drop the refcount entry; the client stays cached
        else Integer.valueOf(n)
      }
    )
    if (clients.size() > MaxCachedClients) evictIdleClients()
  }

  /** Release one reference and close the client if that was the last one.
    *
    * For callers that will not come back — notably driver-side schema discovery, which does no
    * further Aerospike I/O once the schema is known. Executors keep using `releaseOne` so their
    * client survives between the many tasks of a job.
    */
  def releaseAndClose(key: AerospikeConnectionKey): Unit = {
    releaseOne(key)
    closeIfUnreferenced(key, "after the last reference was released")
  }

  /** Close and evict cached clients that currently have no active references. */
  private def evictIdleClients(): Unit =
    clients.forEach((k, _) => closeIfUnreferenced(k, "during idle eviction"))

  /** Close and evict the client for `key` unless a reference is currently held.
    *
    * The refcount is re-checked *inside* `clients.computeIfPresent` so this serializes with the
    * `clients.compute` in `get` for the same key. A concurrent `acquire` therefore either registers
    * its reference before this runs — in which case the client is kept — or blocks and then builds
    * a fresh client. Checking `refCounts` outside the map operation would allow the acquirer to be
    * handed a client that this method closes an instant later.
    */
  private def closeIfUnreferenced(key: AerospikeConnectionKey, context: String): Unit = {
    val _ = clients.computeIfPresent(
      key,
      (_, client) =>
        if (refCounts.containsKey(key)) client
        else {
          closeQuietly(client, context)
          null // evict
        }
    )
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
