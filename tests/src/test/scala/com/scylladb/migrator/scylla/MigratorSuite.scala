package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/**
  * Base class for implementing end-to-end tests.
  *
  * It expects external services (Cassandra, Scylla, Spark, etc.) to be running.
  * See the files `CONTRIBUTING.md` and `docker-compose-tests.yml` for more information.
  *
  * @param sourcePort TCP port of the source database. See docker-compose-test.yml.
  */
abstract class MigratorSuite(sourcePort: Int) extends munit.FunSuite {

  val keyspace = "test"

  private val createKeyspaceStatement =
    SchemaBuilder
      .createKeyspace(keyspace)
      .ifNotExists()
      .withReplicationOptions(Map[String, AnyRef](
        "class"              -> "SimpleStrategy",
        "replication_factor" -> Integer.valueOf(1)).asJava)
      .build()

  /** Client of a target ScyllaDB instance */
  val targetScylla: Fixture[CqlSession] = new Fixture[CqlSession]("targetScylla") {
    var session: CqlSession = null
    def apply(): CqlSession = session
    override def beforeAll(): Unit = {
      session =
        CqlSession
          .builder()
          .addContactPoint(new InetSocketAddress("localhost", 9042))
          .withLocalDatacenter("datacenter1")
          .withAuthCredentials("dummy", "dummy")
          .build()
      session.execute(createKeyspaceStatement)
    }
    override def afterAll(): Unit = session.close()
  }
}
