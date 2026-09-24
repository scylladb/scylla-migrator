package com.scylladb.migrator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.spark.connector.cql.CassandraConnector
import com.scylladb.migrator.config.ClientRoutesSettings
import org.apache.logging.log4j.{ LogManager, Logger }

import java.util.UUID
import scala.jdk.CollectionConverters._

/** Fails fast when the configured client routes do not cover the local datacenter.
  *
  * The driver still opens a session when a connection ID matches no route: the contact point keeps
  * working, and only connections to the other nodes fail later, on executors. Checking the routes
  * on the Spark driver turns a mistyped connection ID into an immediate configuration error.
  */
object ClientRoutesPreflight {
  private val log: Logger = LogManager.getLogger("com.scylladb.migrator.ClientRoutesPreflight")

  private val RoutesQuery =
    "SELECT connection_id, host_id FROM system.client_routes " +
      "WHERE connection_id IN ? ALLOW FILTERING"

  private[migrator] final case class Route(connectionId: String, hostId: UUID)

  def verify(
    connector: CassandraConnector,
    routes: ClientRoutesSettings,
    localDC: String,
    side: String
  ): Unit =
    connector.withSessionDo { session =>
      val connectionIds = routes.endpoints.map(_.connectionId)
      val published = fetchRoutes(session, connectionIds, side)
      val nodes = session.getMetadata.getNodes.values.asScala.toList
      val localHostIds =
        nodes.filter(_.getDatacenter == localDC).flatMap(node => Option(node.getHostId)).toSet
      val knownDCs = nodes.flatMap(node => Option(node.getDatacenter)).toSet
      evaluate(side, localDC, connectionIds, published, localHostIds, knownDCs) match {
        case Left(error) => throw new IllegalStateException(error)
        case Right(warnings) =>
          warnings.foreach(warning => log.warn(warning))
          log.info(
            s"$side: client routes cover all ${localHostIds.size} node(s) of datacenter '$localDC'."
          )
      }
    }

  private def fetchRoutes(
    session: CqlSession,
    connectionIds: Seq[String],
    side: String
  ): Seq[Route] =
    try
      session
        .execute(SimpleStatement.newInstance(RoutesQuery, connectionIds.asJava))
        .all()
        .asScala
        .toList
        .map(row => Route(row.getString("connection_id"), row.getUuid("host_id")))
    catch {
      case e: InvalidQueryException =>
        throw new IllegalStateException(
          s"$side: cannot read system.client_routes (${e.getMessage}). Client routes require " +
            "ScyllaDB Cloud or ScyllaDB 2026.1 and later.",
          e
        )
    }

  /** @return
    *   an error when a node of `localDC` has no route, otherwise warnings about connection IDs that
    *   match no route at all (typically a typo in a remote datacenter's connection ID).
    */
  private[migrator] def evaluate(
    side: String,
    localDC: String,
    connectionIds: Seq[String],
    routes: Seq[Route],
    localHostIds: Set[UUID],
    knownDCs: Set[String]
  ): Either[String, List[String]] = {
    val missing = localHostIds -- routes.map(_.hostId)
    if (localHostIds.isEmpty)
      Left(
        s"$side: no nodes found in localDC '$localDC'. " +
          s"Known datacenters: ${knownDCs.toList.sorted.mkString(", ")}."
      )
    else if (missing.nonEmpty)
      Left(
        s"$side: ${missing.size} of ${localHostIds.size} node(s) in localDC '$localDC' have no " +
          s"client route for connection ID(s) ${connectionIds.mkString(", ")} " +
          s"(host IDs: ${missing.toList.map(_.toString).sorted.mkString(", ")}). Check that " +
          "clientRoutes.endpoints contains the connection ID of the private endpoint that serves " +
          "this datacenter."
      )
    else {
      val idsWithRoutes = routes.map(_.connectionId).toSet
      Right(
        connectionIds
          .filterNot(idsWithRoutes)
          .map(id => s"$side: connection ID '$id' has no entries in system.client_routes.")
          .toList
      )
    }
  }
}
