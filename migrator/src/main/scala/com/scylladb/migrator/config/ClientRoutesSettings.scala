package com.scylladb.migrator.config

import com.datastax.oss.driver.api.core.config.ClientRouteProxy
import com.datastax.spark.connector.cql.{ ClientRouteEndpointConf, ClientRoutesConf }
import io.circe.{ Decoder, DecodingFailure, Encoder, JsonObject }
import io.circe.syntax._

import scala.util.Try

/** Connect to a ScyllaDB Cloud cluster through a private endpoint (AWS PrivateLink, GCP Private
  * Service Connect) using the client routes support of the Spark connector and the driver.
  *
  * The driver reads `system.client_routes` for the configured connection IDs and replaces the
  * address of every node with the private endpoint address and port published for it, so the Spark
  * driver and executors never need to reach node IPs directly. Requires ScyllaDB Cloud (or ScyllaDB
  * 2026.1+ with routes registered through the `/v2/client-routes` REST API). When the Spark driver
  * opens its first session, the connector checks that every node of the local datacenter has a
  * usable route.
  *
  * When this configuration is present, `host`/`port` are the private endpoint address(es) and port
  * provided by the user, only used as initial contact points (see [[ContactPoints]] for listing
  * several addresses), and `cloud` must not be set. `localDC` can be omitted when the listed
  * connection IDs route the nodes of a single datacenter: the connector then uses that datacenter.
  *
  * @param endpoints
  *   One entry per private endpoint connection. A cluster can expose several connection IDs per
  *   datacenter (e.g. one per availability zone) across several datacenters; list every connection
  *   ID the migrator is allowed to use. Together, the listed IDs must route every node of the local
  *   datacenter. When a node is published under several listed IDs, the driver uses one of them.
  * @param shardAwareness
  *   Enable advanced shard awareness: the driver picks the source port of each connection so that
  *   it lands on a given shard. Off by default, as in the driver. ScyllaDB Cloud private endpoints
  *   support it, so enable it there; leave it off for a proxy that does not preserve the client
  *   source port (Proxy Protocol v2). With `false`, basic shard awareness still applies: the driver
  *   sends each request to a connection of the shard that owns the data.
  */
case class ClientRoutesSettings(
  endpoints: List[ClientRouteEndpoint],
  shardAwareness: Boolean = false
) {
  def toConnectorConf: ClientRoutesConf =
    ClientRoutesConf(
      endpoints.map(endpoint =>
        ClientRouteEndpointConf(endpoint.connectionId, endpoint.connectionAddr)
      ),
      shardAwareness = shardAwareness
    )
}

/** @param connectionId
  *   Connection ID of the private endpoint, as shown in the ScyllaDB Cloud console.
  * @param connectionAddr
  *   Optional hostname or IP address (without port) that replaces the `address` published in
  *   `system.client_routes` for this connection ID, e.g. a VPC-local DNS alias of the endpoint.
  */
case class ClientRouteEndpoint(connectionId: String, connectionAddr: Option[String] = None)

object ClientRouteEndpoint {
  implicit val decoder: Decoder[ClientRouteEndpoint] = Decoder.instance { c =>
    for {
      connectionId   <- c.get[String]("connectionId")
      connectionAddr <- c.get[Option[String]]("connectionAddr")
    } yield ClientRouteEndpoint(connectionId.trim, connectionAddr.map(_.trim))
  }

  implicit val encoder: Encoder.AsObject[ClientRouteEndpoint] = Encoder.AsObject.instance { e =>
    JsonObject.fromIterable(
      ("connectionId" -> e.connectionId.asJson) ::
        e.connectionAddr.map(addr => "connectionAddr" -> addr.asJson).toList
    )
  }
}

object ClientRoutesSettings {
  implicit val decoder: Decoder[ClientRoutesSettings] = Decoder.instance { c =>
    for {
      endpoints      <- c.get[List[ClientRouteEndpoint]]("endpoints")
      shardAwareness <- c.getOrElse[Boolean]("shardAwareness")(false)
      settings = ClientRoutesSettings(endpoints, shardAwareness)
      _ <- validate(settings).left.map(DecodingFailure(_, c.history))
    } yield settings
  }

  implicit val encoder: Encoder.AsObject[ClientRoutesSettings] = Encoder.AsObject.instance { s =>
    JsonObject(
      "endpoints"      -> s.endpoints.asJson,
      "shardAwareness" -> s.shardAwareness.asJson
    )
  }

  private def validate(settings: ClientRoutesSettings): Either[String, Unit] = {
    val endpointErrors = settings.endpoints.zipWithIndex.flatMap { case (endpoint, i) =>
      val field = s"clientRoutes.endpoints[$i]"
      if (endpoint.connectionId.isEmpty)
        Some(s"$field.connectionId must not be empty.")
      else if (endpoint.connectionAddr.exists(_.isEmpty))
        Some(s"$field.connectionAddr must not be empty when set.")
      else
        Try(
          new ClientRouteProxy(endpoint.connectionId, endpoint.connectionAddr.orNull)
        ).failed.toOption
          .map(e => s"$field: ${e.getMessage}")
    }
    val duplicateIds = settings.endpoints
      .groupBy(_.connectionId)
      .collect { case (id, occurrences) if id.nonEmpty && occurrences.size > 1 => id }
      .toList
      .sorted
    val errors =
      (if (settings.endpoints.isEmpty)
         List("clientRoutes.endpoints must list at least one connection ID.")
       else Nil) ++
        endpointErrors ++
        (if (duplicateIds.nonEmpty)
           List(
             s"clientRoutes.endpoints has duplicate connectionId(s): ${duplicateIds.mkString(", ")}."
           )
         else Nil)
    Either.cond(errors.isEmpty, (), errors.mkString(" "))
  }
}
