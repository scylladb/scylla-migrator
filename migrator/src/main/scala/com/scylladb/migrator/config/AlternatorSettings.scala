package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, HCursor }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class AlternatorSettings(
  datacenter: Option[String] = None,
  rack: Option[String] = None,
  activeRefreshIntervalMs: Option[Long] = None,
  idleRefreshIntervalMs: Option[Long] = None,
  compression: Option[Boolean] = None,
  optimizeHeaders: Option[Boolean] = None,
  maxConnections: Option[Int] = None,
  connectionMaxIdleTimeMs: Option[Long] = None,
  connectionTimeToLiveMs: Option[Long] = None,
  connectionAcquisitionTimeoutMs: Option[Long] = None,
  connectionTimeoutMs: Option[Long] = None,
  maxItemsPerBatch: Option[Int] = None,
  loadBalancing: Option[Boolean] = None
) {

  /** Whether requests go through the Alternator load-balancing client, which discovers the nodes
    * through `/localnodes` and spreads requests over them. Disabled, every request goes to the
    * configured `endpoint` with the plain AWS SDK client. That is the only mode that works behind a
    * private endpoint (AWS PrivateLink, GCP Private Service Connect) until the Alternator client
    * supports client routes, because `/localnodes` returns node addresses that cannot be reached
    * through it.
    */
  def loadBalancingEnabled: Boolean = loadBalancing.getOrElse(true)
}

object AlternatorSettings {
  implicit val decoder: Decoder[AlternatorSettings] = deriveDecoder
  implicit val asObjectEncoder: Encoder.AsObject[AlternatorSettings] = deriveEncoder

  /** Field names of AlternatorSettings, used by DynamoDB decoders to reject Alternator-only fields.
    * Derived from the case class to stay in sync automatically.
    */
  val fieldNames: Set[String] = AlternatorSettings().productElementNames.toSet

  private def hasProtocolPrefix(host: String): Boolean = {
    val lower = host.toLowerCase(java.util.Locale.ROOT)
    lower.startsWith("http://") || lower.startsWith("https://")
  }

  /** Guard the `type: dynamodb` / `type: dynamo` decoder branch.
    *
    * Rejects configs that contain Alternator-specific keys (for example a nested `alternator`
    * block, `removeConsumedCapacity`, or any [[AlternatorSettings]] field) and suggests switching
    * to `type: alternator`.
    *
    * @param cursor
    *   JSON cursor positioned at the source/target object
    * @param label
    *   `"Source"` or `"Target"`, used in error messages
    * @return
    *   `Right(())` when valid, `Left(DecodingFailure)` otherwise
    */
  def guardDynamoDBType(
    cursor: HCursor,
    label: String
  ): Either[DecodingFailure, Unit] = {
    val guardErrors = List.newBuilder[String]
    if (cursor.downField("alternator").focus.isDefined)
      guardErrors +=
        s"$label type 'dynamodb' contains a nested 'alternator' key. " +
          "Please change the type to 'alternator' and promote the nested Alternator settings to top level."
    if (cursor.downField("removeConsumedCapacity").focus.isDefined)
      guardErrors +=
        s"$label type 'dynamodb' does not support 'removeConsumedCapacity'. " +
          "This setting is only applicable to type 'alternator'."
    val presentKeys = cursor.keys.map(_.toSet).getOrElse(Set.empty)
    val badKeys = presentKeys.intersect(fieldNames)
    if (badKeys.nonEmpty)
      guardErrors +=
        s"$label type 'dynamodb' does not support Alternator-only fields: ${badKeys.toSeq.sorted
            .mkString(", ")}. " +
          "Please change the type to 'alternator' if you want to use these settings."
    val allGuardErrors = guardErrors.result()
    if (allGuardErrors.nonEmpty)
      Left(DecodingFailure(allGuardErrors.mkString("; "), cursor.history))
    else
      Right(())
  }

  /** Validate common Alternator decoding concerns and return error messages if invalid. Checks:
    * endpoint required, protocol prefix required, assumeRole not supported, plus all
    * AlternatorSettings-level validations.
    */
  def validateDecoding(
    endpoint: Option[DynamoDBEndpoint],
    credentials: Option[AWSCredentials],
    altSettings: AlternatorSettings
  ): List[String] = {
    val errors = List.newBuilder[String]
    if (endpoint.isEmpty)
      errors += "requires an 'endpoint' to be set."
    if (endpoint.exists(e => !hasProtocolPrefix(e.host)))
      errors += "endpoint host must include a protocol prefix ('http://' or 'https://')."
    if (credentials.flatMap(_.assumeRole).isDefined)
      errors += "does not support 'assumeRole' in credentials."
    errors ++= validate(altSettings)
    errors.result()
  }

  /** Validate Alternator-specific fields and return error messages if invalid. */
  def validate(s: AlternatorSettings): List[String] = {
    val errors = List.newBuilder[String]
    if (s.rack.isDefined && s.datacenter.isEmpty)
      errors += "'rack' is set without 'datacenter'. Please also set 'datacenter' when using 'rack'."
    if (s.maxConnections.exists(_ <= 0))
      errors += "'maxConnections' must be a positive integer."
    if (s.activeRefreshIntervalMs.exists(_ <= 0))
      errors += "'activeRefreshIntervalMs' must be a positive value."
    if (s.idleRefreshIntervalMs.exists(_ <= 0))
      errors += "'idleRefreshIntervalMs' must be a positive value."
    if (s.connectionMaxIdleTimeMs.exists(_ < 0))
      errors += "'connectionMaxIdleTimeMs' must not be negative."
    if (s.connectionTimeToLiveMs.exists(_ < 0))
      errors += "'connectionTimeToLiveMs' must not be negative."
    if (s.connectionAcquisitionTimeoutMs.exists(_ < 0))
      errors += "'connectionAcquisitionTimeoutMs' must not be negative."
    if (s.connectionTimeoutMs.exists(_ < 0))
      errors += "'connectionTimeoutMs' must not be negative."
    if (s.maxItemsPerBatch.exists(_ <= 0))
      errors += "'maxItemsPerBatch' must be a positive integer."
    if (!s.loadBalancingEnabled) {
      val clientOnly = loadBalancingClientSettings(s)
      if (clientOnly.nonEmpty)
        errors +=
          s"${clientOnly.mkString(", ")} only apply to the Alternator load-balancing client; " +
            "remove them or remove 'loadBalancing: false'."
    }
    errors.result()
  }

  /** Names of the settings that only the Alternator load-balancing client applies. */
  private def loadBalancingClientSettings(s: AlternatorSettings): List[String] =
    List(
      "datacenter"                     -> s.datacenter,
      "rack"                           -> s.rack,
      "activeRefreshIntervalMs"        -> s.activeRefreshIntervalMs,
      "idleRefreshIntervalMs"          -> s.idleRefreshIntervalMs,
      "compression"                    -> s.compression,
      "optimizeHeaders"                -> s.optimizeHeaders,
      "maxConnections"                 -> s.maxConnections,
      "connectionMaxIdleTimeMs"        -> s.connectionMaxIdleTimeMs,
      "connectionTimeToLiveMs"         -> s.connectionTimeToLiveMs,
      "connectionAcquisitionTimeoutMs" -> s.connectionAcquisitionTimeoutMs,
      "connectionTimeoutMs"            -> s.connectionTimeoutMs
    ).collect { case (name, Some(_)) => s"'$name'" }
}
