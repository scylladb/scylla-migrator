package com.scylladb.migrator.readers

class AerospikeClientHolderTest extends munit.FunSuite {

  override def afterEach(context: AfterEach): Unit =
    AerospikeClientHolder.reset()

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

  private def requireAerospike(): com.aerospike.client.AerospikeClient = {
    val client =
      try AerospikeClientHolder.get(localConfig, None)
      catch {
        case _: Exception =>
          assume(false, "Aerospike not available on localhost:3000, skipping")
          null // unreachable — assume(false) throws
      }
    client
  }

  test("get returns the same client instance for the same key") {
    val client1 = requireAerospike()
    val client2 = AerospikeClientHolder.get(localConfig, None)
    assert(client1 eq client2, "Expected same client instance for same connection key")
  }

  test("release removes cached client") {
    val client1 = requireAerospike()
    AerospikeClientHolder.release(localConfig, None)
    val client2 = requireAerospike()
    assert(!(client1 eq client2), "Expected different client instance after release")
  }
}
