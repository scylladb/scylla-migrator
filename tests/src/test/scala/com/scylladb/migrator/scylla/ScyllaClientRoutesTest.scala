package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import com.scylladb.migrator.config.{
  ClientRouteEndpoint,
  ClientRoutesSettings,
  Credentials,
  TargetSettings
}
import org.apache.spark.SparkConf

import java.net.{ InetSocketAddress, URI }
import java.net.http.{ HttpClient, HttpRequest, HttpResponse }
import scala.jdk.CollectionConverters._
import scala.util.chaining._

/** Client routes (PrivateLink / Private Service Connect) against the `scylla` service.
  *
  * The suite publishes a client route that maps the node to a second host port of the same
  * container (`routedPort`), standing in for the private endpoint of a cloud load balancer. A
  * session that honours the route reaches the node through that port instead of its own address.
  * Requires the REST API (port 10000) and `routedPort` to be published, see
  * `docker-compose-scylla.yml`.
  */
class ScyllaClientRoutesTest extends MigratorSuite(sourcePort = 9044) {

  private val connectionId = "migrator-it-connection"
  private val routedPort = 9049
  private val clientRoutesApi = URI.create("http://localhost:10000/v2/client-routes")
  private val http = HttpClient.newHttpClient()

  private lazy val routeEntries: String = {
    val hostId =
      targetScylla().execute("SELECT host_id FROM system.local").one().getUuid("host_id")
    s"""[{"connection_id":"$connectionId","host_id":"$hostId","address":"127.0.0.1",""" +
      s""""port":$routedPort,"tls_port":$routedPort,"alternator_port":8000,""" +
      s""""alternator_https_port":8000}]"""
  }

  private val clientRoute: Fixture[Unit] = new Fixture[Unit]("clientRoute") {
    def apply(): Unit = ()
    override def beforeAll(): Unit = {
      callClientRoutesApi("POST", routeEntries)
      awaitRoutePublished()
    }
    override def afterAll(): Unit = callClientRoutesApi("DELETE", routeEntries)
  }

  override def munitFixtures: Seq[Fixture[_]] = super.munitFixtures :+ clientRoute

  test("nodes are reached through the routed endpoint") {
    val connector =
      Connectors.targetConnector(new SparkConf(false), routedTarget(connectionId))
    val nodePorts = connector.withSessionDo { session =>
      session.execute("SELECT release_version FROM system.local")
      session.getMetadata.getNodes.values.asScala.toList.map(_.getEndPoint.resolve()).collect {
        case address: InetSocketAddress => address.getPort
      }
    }
    assertEquals(nodePorts, List(routedPort))
  }

  test("an unknown connection ID fails the preflight check") {
    val error = intercept[IllegalStateException] {
      Connectors.targetConnector(new SparkConf(false), routedTarget("migrator-it-unknown"))
    }
    assert(error.getMessage.contains("have no client route"), error.getMessage)
    assert(error.getMessage.contains("migrator-it-unknown"), error.getMessage)
  }

  withTable("ClientRoutesTest").test("migrate into a target reached through client routes") {
    tableName =>
      sourceCassandra().execute(
        QueryBuilder
          .insertInto(keyspace, tableName)
          .values(
            Map[String, Term]("id" -> literal("12345"), "foo" -> literal("bar")).asJava
          )
          .build()
      )

      successfullyPerformMigration("scylla-to-scylla-client-routes.yaml")

      targetScylla()
        .execute(QueryBuilder.selectFrom(keyspace, tableName).all().build())
        .tap { resultSet =>
          val rows = resultSet.all().asScala
          assertEquals(rows.size, 1)
          assertEquals(rows.head.getString("id"), "12345")
          assertEquals(rows.head.getString("foo"), "bar")
        }
  }

  private def routedTarget(connectionIds: String*): TargetSettings.Scylla =
    TargetSettings.Scylla(
      host                          = "localhost",
      port                          = 9042,
      localDC                       = Some("datacenter1"),
      credentials                   = Some(Credentials("dummy", "dummy")),
      sslOptions                    = None,
      keyspace                      = keyspace,
      table                         = "unused",
      connections                   = None,
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "LOCAL_QUORUM",
      clientRoutes = Some(ClientRoutesSettings(connectionIds.map(ClientRouteEndpoint(_)).toList))
    )

  private def callClientRoutesApi(method: String, body: String): Unit = {
    val request = HttpRequest
      .newBuilder(clientRoutesApi)
      .header("Content-Type", "application/json")
      .method(method, HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    assert(
      response.statusCode() / 100 == 2,
      s"$method $clientRoutesApi returned ${response.statusCode()}: ${response.body()}"
    )
  }

  private def awaitRoutePublished(): Unit = {
    val query = SimpleStatement.newInstance(
      "SELECT host_id FROM system.client_routes WHERE connection_id = ? ALLOW FILTERING",
      connectionId
    )
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    while (targetScylla().execute(query).one() == null) {
      assert(System.nanoTime() < deadline, s"route for $connectionId was not published in time")
      Thread.sleep(500)
    }
  }
}
