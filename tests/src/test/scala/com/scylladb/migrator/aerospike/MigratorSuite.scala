package com.scylladb.migrator.aerospike

import com.aerospike.client.{ AerospikeClient, AerospikeException, Bin, Key, ResultCode }
import com.aerospike.client.policy.{ ClientPolicy, InfoPolicy, ScanPolicy, WritePolicy }
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.scylladb.migrator.Integration
import org.apache.logging.log4j.LogManager
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Base class for Aerospike-to-Scylla end-to-end tests.
  *
  * It expects external services (Aerospike, Scylla, Spark) to be running. See the files
  * `CONTRIBUTING.md` and `docker-compose-tests.yml` for more information.
  */
@Category(Array(classOf[Integration]))
abstract class MigratorSuite extends munit.FunSuite {

  // Aerospike truncation is asynchronous and may take several seconds on CI.
  // The default 30s munit timeout is too tight when retryPut and migration are combined.
  override val munitTimeout: Duration = 90.seconds

  private val log = LogManager.getLogger(getClass)

  val keyspace = "test"
  val aerospikeNamespace = "test"

  private val aerospikeHost = sys.env.getOrElse("AEROSPIKE_HOST", "localhost")
  private val aerospikePort = sys.env.getOrElse("AEROSPIKE_PORT", "3000").toInt
  private val scyllaHost = sys.env.getOrElse("SCYLLA_HOST", "localhost")
  private val scyllaPort = sys.env.getOrElse("SCYLLA_PORT", "9042").toInt

  private val createKeyspaceStatement =
    SchemaBuilder
      .createKeyspace(keyspace)
      .ifNotExists()
      .withReplicationOptions(
        Map[String, AnyRef](
          "class"              -> "SimpleStrategy",
          "replication_factor" -> Integer.valueOf(1)
        ).asJava
      )
      .build()

  /** Client of a source Aerospike instance */
  val sourceAerospike: Fixture[AerospikeClient] =
    new Fixture[AerospikeClient]("sourceAerospike") {
      private var client: AerospikeClient = null
      def apply(): AerospikeClient = client
      override def beforeAll(): Unit = {
        val policy = new ClientPolicy()
        client = new AerospikeClient(policy, aerospikeHost, aerospikePort)
      }
      override def afterAll(): Unit = client.close()
    }

  /** Client of a target ScyllaDB instance */
  val targetScylla: Fixture[CqlSession] = new Fixture[CqlSession]("targetScylla") {
    var session: CqlSession = null
    def apply(): CqlSession = session
    override def beforeAll(): Unit = {
      session = CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress(scyllaHost, scyllaPort))
        .withLocalDatacenter("datacenter1")
        .withAuthCredentials("dummy", "dummy")
        .build()
      session.execute(createKeyspaceStatement)
    }
    override def afterAll(): Unit = session.close()
  }

  /** Fixture automating housekeeping when migrating an Aerospike set to a Scylla table.
    *
    * It truncates the Aerospike set and drops/recreates the target Scylla table.
    *
    * @param setName
    *   Name of the Aerospike set and corresponding Scylla table
    */
  /** Wait for an Aerospike truncate to take effect by polling until the set is empty. Truncate is
    * asynchronous in Aerospike — data may still be visible briefly after the call. Uses exponential
    * backoff to reduce flakiness on slow CI machines.
    */
  protected def waitForTruncate(setName: String, maxWaitMs: Int = 10000): Unit = {
    val deadline = System.currentTimeMillis() + maxWaitMs
    val scanPolicy = new ScanPolicy()
    scanPolicy.maxRecords = 1
    var found = true
    var sleepMs = 100L
    while (found && System.currentTimeMillis() < deadline) {
      var count = 0
      try
        sourceAerospike().scanPartitions(
          scanPolicy,
          com.aerospike.client.query.PartitionFilter.all(),
          aerospikeNamespace,
          setName,
          (_: Key, _: com.aerospike.client.Record) => count += 1
        )
      catch { case e: Exception => log.debug("waitForTruncate: scan check failed", e) }
      found = count > 0
      if (found) {
        Thread.sleep(sleepMs)
        sleepMs = math.min(sleepMs * 2, 2000L) // exponential backoff, cap at 2s
      }
    }
    assert(!found, s"Timed out waiting for truncate of set $setName after ${maxWaitMs}ms")
    // Verify the set is writable (including TTL writes) — Aerospike may briefly
    // reject writes while finalizing truncation or initializing a new set.
    // Use TTL=1 so the probe record self-expires before any scan picks it up.
    // Two consecutive successful writes confirm the set is stable — Aerospike 7.x may
    // transiently re-enter FORBIDDEN state after a single successful probe write.
    val writeDeadline = System.currentTimeMillis() + 20000
    val testKey = new Key(aerospikeNamespace, setName, "__truncate_check__")
    var consecutiveSuccesses = 0
    var writeSleepMs = 200L
    while (consecutiveSuccesses < 2 && System.currentTimeMillis() < writeDeadline)
      try {
        val ttlPolicy = new com.aerospike.client.policy.WritePolicy()
        ttlPolicy.expiration = 1 // 1 second — self-expires before migration scan
        sourceAerospike().put(ttlPolicy, testKey, new com.aerospike.client.Bin("_c", 1))
        consecutiveSuccesses += 1
        if (consecutiveSuccesses < 2) Thread.sleep(200)
      } catch {
        case e: Exception =>
          consecutiveSuccesses = 0
          log.debug("waitForTruncate: write check failed, retrying", e)
          Thread.sleep(writeSleepMs)
          writeSleepMs = math.min(writeSleepMs * 2, 3000L)
      }
  }

  def withSet(setName: String): FunFixture[String] =
    FunFixture(
      setup = { _ =>
        // Truncate the Aerospike set (ignore errors if set doesn't exist)
        try sourceAerospike().truncate(new InfoPolicy(), aerospikeNamespace, setName, null)
        catch { case e: Exception => log.debug("Setup: truncate exception (ignored)", e) }
        waitForTruncate(setName)

        // Drop the target Scylla table if it exists
        val dropStmt = SchemaBuilder.dropTable(keyspace, setName).ifExists().build()
        targetScylla().execute(dropStmt)

        setName
      },
      teardown = { _ =>
        try sourceAerospike().truncate(new InfoPolicy(), aerospikeNamespace, setName, null)
        catch { case e: Exception => log.debug("Teardown: truncate exception (ignored)", e) }

        val dropStmt = SchemaBuilder.dropTable(keyspace, setName).ifExists().build()
        try targetScylla().execute(dropStmt)
        catch { case e: Exception => log.debug("Teardown: drop table exception (ignored)", e) }
        ()
      }
    )

  /** Put with retry — Aerospike 7.x may transiently reject writes (Error 22: FORBIDDEN) after a
    * truncate, even after waitForTruncate succeeds. Uses a deadline-based approach to stay within
    * per-test timeouts while retrying for as long as possible.
    */
  protected def retryPut(policy: WritePolicy, key: Key, bins: Bin*): Unit = {
    val deadline = System.currentTimeMillis() + 30000 // 30s budget for slow CI
    var sleepMs = 200L
    while (true)
      try {
        sourceAerospike().put(policy, key, bins: _*)
        return
      } catch {
        case e: AerospikeException
            if (e.getResultCode == ResultCode.FAIL_FORBIDDEN ||
              e.getResultCode == ResultCode.ALWAYS_FORBIDDEN) =>
          if (System.currentTimeMillis() + sleepMs >= deadline) throw e
          log.debug(
            s"retryPut: FORBIDDEN (code ${e.getResultCode}), retrying in ${sleepMs}ms",
            e
          )
          Thread.sleep(sleepMs)
          sleepMs = math.min(sleepMs * 2, 2000L)
      }
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(sourceAerospike, targetScylla)
}
