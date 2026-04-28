package com.scylladb.migrator.config

import java.net.URI

import io.circe.{ Decoder, DecodingFailure, Encoder }
import io.circe.generic.semiauto.deriveEncoder

import scala.util.Try

/** Configuration for connecting to a Cassandra-compatible service via a Cloud Secure Connect bundle
  * (DataStax Astra DB).
  *
  * The bundle is a zip file produced by Astra that contains the contact points, TLS material
  * (certificates, keystore/truststore), metadata service URL and protocol settings required to
  * reach a managed cluster behind an SNI proxy. When this configuration is present, the migrator
  * delegates contact-point discovery, TLS and SNI handshake to the underlying connector and
  * therefore the `host`, `port`, `localDC` and `sslOptions` fields of the source/target settings
  * MUST NOT be specified — they would be ignored by the driver and conflict with the bundle.
  *
  * Authentication credentials must still be supplied via the `credentials` field of the enclosing
  * source/target settings: the bundle does not embed Astra database credentials.
  *
  * @param secureBundlePath
  *   Path to the secure-connect bundle. Must be readable from the Spark driver and from every Spark
  *   executor that will open a CQL session. Supported forms:
  *   - An absolute filesystem path that exists identically on every node (recommended for
  *     standalone clusters: bake the bundle into the worker image, or place it on a shared mount).
  *   - An `https://`, `s3://`, or `s3a://` URL accessible from every node.
  *
  * Note: relative paths are intentionally not auto-resolved via `SparkFiles.get` because the
  * resolved path is computed on the driver and would be incorrect on executors. If you want to ship
  * the bundle through `--files`, materialise it on a deterministic path on every node (e.g. via the
  * entrypoint script) and pass that absolute path here.
  */
case class CloudConfig(secureBundlePath: String)

object CloudConfig {
  private val RemoteSchemes = Set("https", "s3", "s3a")

  implicit val encoder: Encoder[CloudConfig] = deriveEncoder[CloudConfig]
  implicit val decoder: Decoder[CloudConfig] = Decoder.instance { c =>
    for {
      rawPath <- c.get[String]("secureBundlePath")
      path = rawPath.trim
      _ <-
        if (path.trim.isEmpty)
          Left(
            DecodingFailure(
              "cloud.secureBundlePath must not be empty.",
              c.history
            )
          )
        else if (path.startsWith("/")) Right(())
        else {
          val uri = Try(new URI(path)).toOption
          val scheme = uri.flatMap(u => Option(u.getScheme).map(_.toLowerCase))
          scheme match {
            case Some("http") =>
              Left(
                DecodingFailure(
                  "cloud.secureBundlePath must not use plain HTTP; use an absolute local path, https://, s3://, or s3a://.",
                  c.history
                )
              )
            case Some(s) if RemoteSchemes.contains(s) && uri.exists(_.getUserInfo != null) =>
              Left(
                DecodingFailure(
                  "cloud.secureBundlePath must not include URL user-info credentials.",
                  c.history
                )
              )
            case Some(s) if RemoteSchemes.contains(s) && uri.exists(_.getRawQuery != null) =>
              Left(
                DecodingFailure(
                  "cloud.secureBundlePath must not include query string credentials.",
                  c.history
                )
              )
            case Some(s) if RemoteSchemes.contains(s) => Right(())
            case _ =>
              Left(
                DecodingFailure(
                  "cloud.secureBundlePath must be an absolute local path, https:// URL, s3:// URL, or s3a:// URL.",
                  c.history
                )
              )
          }
        }
    } yield CloudConfig(path)
  }
}
