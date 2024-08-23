package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.scylladb.migrator.CassandraUtils.dropAndRecreateTable

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

  /** Client of a source Cassandra instance */
  val sourceCassandra: Fixture[CqlSession] = new Fixture[CqlSession]("sourceCassandra") {
    private var session: CqlSession = null
    def apply(): CqlSession = session
    override def beforeAll(): Unit = {
      session =
        CqlSession
          .builder()
          .addContactPoint(new InetSocketAddress("localhost", sourcePort))
          .withLocalDatacenter("datacenter1")
          .withAuthCredentials("dummy", "dummy")
          .build()
      session.execute(createKeyspaceStatement)
    }
    override def afterAll(): Unit = session.close()
  }

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

  /**
    * Fixture automating the house-keeping work when migrating a table.
    *
    * It deletes the table from both the source and target databases in case it was already
    * existing, and then recreates it in the source database.
    *
    * After the test is executed, it deletes the table from both the source and target
    * databases.
    *
    * @param name Name of the table
    */
  def withTable(name: String, renames: Map[String, String] = Map.empty): FunFixture[String] =
    FunFixture(
      setup = { _ =>
        // Make sure the source and target databases do not contain the table already
        try {
          dropAndRecreateTable(sourceCassandra(), keyspace, name, columnName = identity)
          dropAndRecreateTable(
            targetScylla(),
            keyspace,
            name,
            columnName = originalName => renames.getOrElse(originalName, originalName))
        } catch {
          case any: Throwable =>
            fail(s"Something did not work as expected", any)
        }
        name
      },
      teardown = { _ =>
        // Clean-up both the source and target databases
        val dropTableQuery = SchemaBuilder.dropTable(keyspace, name).build()
        targetScylla().execute(dropTableQuery)
        sourceCassandra().execute(dropTableQuery)
        ()
      }
    )

  override def munitFixtures: Seq[Fixture[_]] = Seq(sourceCassandra, targetScylla)

}
