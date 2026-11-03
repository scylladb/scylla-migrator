package com.scylladb.migrator.readers

class AerospikeClientHolderTest extends munit.FunSuite {

  private val testClients = scala.collection.mutable.ArrayBuffer.empty[TestClient]

  override def afterEach(context: AfterEach): Unit =
    try AerospikeClientHolder.reset()
    finally {
      testClients.filter(_.closeCount == 0).foreach(_.close())
      testClients.clear()
    }

  test("release on uncached key is a no-op") {
    val connConfig = AerospikeConnectionConfig(
      hosts            = List("nonexistent"),
      port             = 13579,
      connectTimeoutMs = Some(100),
      socketTimeoutMs  = None,
      totalTimeoutMs   = None,
      tlsName          = None,
      maxConnsPerNode  = None,
      connPoolsPerNode = None
    )
    // Should not throw
    AerospikeClientHolder.release(connConfig, None)
  }

  test("reset allows subsequent get without IllegalStateException") {
    AerospikeClientHolder.reset()
    val connConfig = AerospikeConnectionConfig(
      hosts            = List("nonexistent"),
      port             = 13579,
      connectTimeoutMs = Some(100),
      socketTimeoutMs  = None,
      totalTimeoutMs   = None,
      tlsName          = None,
      maxConnsPerNode  = None,
      connPoolsPerNode = None
    )
    // get() will fail to connect, but should not throw IllegalStateException
    val ex = intercept[Exception] {
      AerospikeClientHolder.get(connConfig, None)
    }
    assert(!ex.isInstanceOf[IllegalStateException], s"Expected connection error, got: $ex")
  }

  private val localConfig = AerospikeConnectionConfig(
    hosts            = List("localhost"),
    port             = 3000,
    connectTimeoutMs = Some(1000),
    socketTimeoutMs  = None,
    totalTimeoutMs   = None,
    tlsName          = None,
    maxConnsPerNode  = None,
    connPoolsPerNode = None
  )

  // No seed hosts means no network I/O and a deterministically disconnected client.
  private def disconnectedPolicy: com.aerospike.client.policy.ClientPolicy = {
    val policy = new com.aerospike.client.policy.ClientPolicy()
    policy.failIfNotConnected = false
    policy
  }

  private class TestClient extends com.aerospike.client.AerospikeClient(
        disconnectedPolicy,
        Array.empty[com.aerospike.client.Host]: _*
      ) {
    var closeCount = 0
    testClients += this
    override def close(): Unit = {
      closeCount += 1
      super.close()
    }
  }

  test("an outage cannot close a shared client while tasks retain references") {
    val key = AerospikeConnectionKey.fromConfig(localConfig, None)
    val client = new TestClient
    AerospikeClientHolder.getOrCreate(key)(client)
    assert(AerospikeClientHolder.acquire(key, localConfig, None) eq client)
    assert(!client.isConnected)
    // A second task must retain this client's reconnecting cluster, not close it.
    assert(AerospikeClientHolder.acquire(key, localConfig, None) eq client)
    assertEquals(client.closeCount, 0)
    AerospikeClientHolder.releaseOne(key)
    assertEquals(client.closeCount, 0)
    AerospikeClientHolder.releaseOne(key)
    assertEquals(client.closeCount, 0)
    val replacement = new TestClient
    assert(AerospikeClientHolder.getOrCreate(key)(replacement) eq replacement)
    assertEquals(client.closeCount, 1)
  }

  test("an unreferenced disconnected client can be replaced") {
    val key = AerospikeConnectionKey.fromConfig(localConfig, None)
    val client = new TestClient
    AerospikeClientHolder.getOrCreate(key)(client)
    assert(!client.isConnected)
    val replacement = new TestClient
    assert(AerospikeClientHolder.getOrCreate(key)(replacement) eq replacement)
    assertEquals(client.closeCount, 1)
    assertEquals(replacement.closeCount, 0)
  }

  test("idle eviction spares a referenced disconnected client") {
    val key = AerospikeConnectionKey.fromConfig(localConfig, None)
    val pinned = new TestClient
    AerospikeClientHolder.getOrCreate(key)(pinned)
    AerospikeClientHolder.acquire(key, localConfig, None)
    assert(!pinned.isConnected)
    val idle = (1 to 5).map { port =>
      val idleKey = key.copy(port = port)
      val client = new TestClient
      AerospikeClientHolder.getOrCreate(idleKey)(client)
      idleKey -> client
    }
    AerospikeClientHolder.releaseOne(idle.head._1)
    assert(idle.forall(_._2.closeCount == 1))
    assertEquals(pinned.closeCount, 0)
    AerospikeClientHolder.releaseOne(key)
    val replacement = new TestClient
    AerospikeClientHolder.getOrCreate(key)(replacement)
    assertEquals(pinned.closeCount, 1)
  }

  test("reset closes cached clients and permits a fresh cache") {
    val key = AerospikeConnectionKey.fromConfig(localConfig, None)
    val client = new TestClient
    AerospikeClientHolder.getOrCreate(key)(client)
    AerospikeClientHolder.reset()
    assertEquals(client.closeCount, 1)
    val replacement = new TestClient
    assert(AerospikeClientHolder.getOrCreate(key)(replacement) eq replacement)
    assertEquals(replacement.closeCount, 0)
  }

}
