package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

import java.net.InetSocketAddress
import scala.sys.process.Process
import scala.jdk.CollectionConverters._
import scala.util.chaining._

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

  /** Client of a source Cassandra instance */
  val sourceCassandra: CqlSession = CqlSession
    .builder()
    .addContactPoint(new InetSocketAddress("localhost", sourcePort))
    .withLocalDatacenter("datacenter1")
    .withAuthCredentials("dummy", "dummy")
    .build()

  /** Client of a target ScyllaDB instance */
  val targetScylla: CqlSession = CqlSession
    .builder()
    .addContactPoint(new InetSocketAddress("localhost", 9042))
    .withLocalDatacenter("datacenter1")
    .withAuthCredentials("dummy", "dummy")
    .build()

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
  def withTable(name: String, renames: Map[String, String] = Map.empty): FunFixture[String] = FunFixture(
    setup = { _ =>
      def dropAndRecreateTable(database: CqlSession, columnName: String => String): Unit =
        try {
          val dropTableStatement =
            SchemaBuilder
              .dropTable(keyspace, name)
              .ifExists()
              .build()
          database
            .execute(dropTableStatement)
            .ensuring(_.wasApplied())
          val createTableStatement =
            SchemaBuilder
              .createTable(keyspace, name)
              .withPartitionKey("id", DataTypes.TEXT)
              .withColumn(columnName("foo"), DataTypes.TEXT)
              .withColumn(columnName("bar"), DataTypes.INT)
              .build()
          database
            .execute(createTableStatement)
            .ensuring(_.wasApplied())
        } catch {
          case any: Throwable =>
            fail(s"Something did not work as expected", any)
        }
      // Make sure the source and target databases do not contain the table already
      dropAndRecreateTable(sourceCassandra, columnName = identity)
      dropAndRecreateTable(targetScylla, columnName = originalName => renames.getOrElse(originalName, originalName))
      name
    },
    teardown = { _ =>
      // Clean-up both the source and target databases
      val dropTableQuery = SchemaBuilder.dropTable(keyspace, name).build()
      targetScylla.execute(dropTableQuery)
      sourceCassandra.execute(dropTableQuery)
      ()
    }
  )

  /**
   * Run a migration by submitting a Spark job to the Spark cluster.
   * @param migratorConfigFile Configuration file to use. Write your
   *                           configuration files in the directory
   *                           `src/test/configurations`, which is
   *                           automatically mounted to the Spark
   *                           cluster by Docker Compose.
   */
  def submitSparkJob(migratorConfigFile: String): Unit = {
    Process(
      Seq(
        "docker",
        "compose",
        "-f", "docker-compose-tests.yml",
        "exec",
        "spark-master",
        "/spark/bin/spark-submit",
        "--class", "com.scylladb.migrator.Migrator",
        "--master", "spark://spark-master:7077",
        "--conf", "spark.driver.host=spark-master",
        "--conf", s"spark.scylla.config=/app/configurations/${migratorConfigFile}",
        // Uncomment one of the following lines to plug a remote debugger on the Spark master or worker.
        // "--conf", "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
        // "--conf", "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006",
        "/jars/scylla-migrator-assembly-0.0.1.jar"
      )
    ).run().exitValue().tap { statusCode =>
      assertEquals(statusCode, 0, "Spark job failed")
    }
    ()
  }

  override def beforeAll(): Unit = {
    val keyspaceStatement =
      SchemaBuilder
        .createKeyspace(keyspace)
        .ifNotExists()
        .withReplicationOptions(Map[String, AnyRef]("class" -> "SimpleStrategy", "replication_factor" -> new Integer(1)).asJava)
        .build()
    sourceCassandra.execute(keyspaceStatement)
    targetScylla.execute(keyspaceStatement)
  }

  override def afterAll(): Unit = {
    sourceCassandra.close()
    targetScylla.close()
  }

}
