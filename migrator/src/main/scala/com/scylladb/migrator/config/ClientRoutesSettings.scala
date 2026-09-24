package com.scylladb.migrator.config

import com.datastax.oss.driver.api.core.config.{ ClientRouteProxy, ClientRoutesConfig }
import io.circe.{ Decoder, DecodingFailure, Encoder, JsonObject }
import io.circe.syntax._

import scala.util.Try

/** Connect to a ScyllaDB Cloud cluster through a private endpoint (AWS PrivateLink, GCP Private
  * Service Connect) using the driver's client routes support.
  *
  * The driver reads `system.client_routes` for the configured connection IDs and replaces the
  * address of every node with the private endpoint address and port published for it, so the Spark
  * driver and executors never need to reach node IPs directly. Requires ScyllaDB Cloud (or ScyllaDB
  * 2026.1+ with routes registered through the `/v2/client-routes` REST API).
  *
  * When this configuration is present, `host`/`port` must point at a private endpoint address that
  * reaches one node (it is only used as the initial contact point), and `localDC` is mandatory.
  * `cloud` must not be set.
  *
  * @param endpoints
  *   One entry per private endpoint connection. A multi-datacenter cluster typically exposes one
  *   connection ID per datacenter; list every connection ID the migrator is allowed to use. The
  *   connection covering `localDC` must be present.
  * @param shardAwareness
  *   Keep shard-aware routing through the load balancer. Only enable it when the endpoint forwards
  *   the client source port (Proxy Protocol v2); otherwise connections land on random shards and
  *   the driver may open excess connections.
  */
case class ClientRoutesSettings(
  endpoints: List[ClientRouteEndpoint],
  shardAwareness: Boolean = false
) {
  def toDriverConfig: ClientRoutesConfig =
    endpoints
      .foldLeft(ClientRoutesConfig.builder()) { (builder, endpoint) =>
        builder.addEndpoint(
          new ClientRouteProxy(endpoint.connectionId, endpoint.connectionAddr.orNull)
        )
      }
      .withShardAwareness(shardAwareness)
      .build()
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
