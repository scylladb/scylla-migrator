package com.scylladb.migrator.aerospike

import com.scylladb.migrator.Integration
import com.scylladb.migrator.readers.{ AerospikeClientHolder, AerospikeConnectionConfig }
import org.junit.experimental.categories.Category

@Category(Array(classOf[Integration]))
class AerospikeClientHolderIntegrationTest extends munit.FunSuite {
  private val connConfig = AerospikeConnectionConfig(
    hosts            = List(sys.env.getOrElse("AEROSPIKE_HOST", "localhost")),
    port             = sys.env.getOrElse("AEROSPIKE_PORT", "3000").toInt,
    connectTimeoutMs = Some(1000),
    socketTimeoutMs  = None,
    totalTimeoutMs   = None,
    tlsName          = None,
    maxConnsPerNode  = None,
    connPoolsPerNode = None
  )

  override def beforeEach(context: BeforeEach): Unit = AerospikeClientHolder.reset()

  override def afterEach(context: AfterEach): Unit = AerospikeClientHolder.reset()

  test("get returns the same connected client instance for the same idle key") {
    val client1 = AerospikeClientHolder.get(connConfig, None)
    assert(client1.isConnected)
    val client2 = AerospikeClientHolder.get(connConfig, None)
    assert(client1 eq client2, "Expected same client instance for same connection key")
  }

  test("release removes cached client") {
    val client1 = AerospikeClientHolder.get(connConfig, None)
    assert(client1.isConnected)
    AerospikeClientHolder.release(connConfig, None)
    assert(!client1.isConnected)
    val client2 = AerospikeClientHolder.get(connConfig, None)
    assert(!(client1 eq client2), "Expected different client instance after release")
  }
}
