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
  *   - An absolute filesystem path (e.g. `/opt/migrator/bundle.zip`) that exists identically on
  *     every node. The migrator auto-converts this to a `file://` URL at runtime because the
  *     connector resolves bundle paths via `new URL(path)`, which requires a scheme.
  *   - An `https://` URL accessible from every node.
  *   - A bare filename (e.g. `bundle.zip`) for bundles distributed via Spark's `--files` mechanism.
  *     The connector resolves bare filenames through `SparkFiles.get` on each executor.
  *   - `s3://` or `s3a://` URLs. '''Note:''' these rely on Hadoop's URL stream handler being
  *     registered in the JVM (standard in most Spark environments). For maximum reliability, prefer
  *     distributing the bundle with `--files s3://bucket/bundle.zip` and setting `secureBundlePath`
  *     to the bare filename instead.
  */
case class CloudConfig(secureBundlePath: String)

object CloudConfig {
  private val RemoteSchemes = Set("https", "s3", "s3a", "file")

  implicit val encoder: Encoder[CloudConfig] = deriveEncoder[CloudConfig]
  private val BareFilenamePattern = "[a-zA-Z0-9][a-zA-Z0-9._-]*".r

  implicit val decoder: Decoder[CloudConfig] = Decoder.instance { c =>
    for {
      rawPath <- c.get[String]("secureBundlePath")
      path = rawPath.trim
      _ <-
        if (path.isEmpty)
          Left(
            DecodingFailure(
              "cloud.secureBundlePath must not be empty.",
              c.history
            )
          )
        else if (path.startsWith("/")) Right(())
        else if (BareFilenamePattern.matches(path) && !path.contains("/")) Right(())
        else {
          val uri = Try(new URI(path)).toOption
          val scheme = uri.flatMap(u => Option(u.getScheme).map(_.toLowerCase))
          scheme match {
            case Some("http") =>
              Left(
                DecodingFailure(
                  "cloud.secureBundlePath must not use plain HTTP; use an absolute local path, " +
                    "an https://, s3://, or s3a:// URL, or a bare filename for --files.",
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
                  "cloud.secureBundlePath must be an absolute local path, an https://, s3://, " +
                    "or s3a:// URL, or a bare filename (for Spark --files distribution).",
                  c.history
                )
              )
          }
        }
    } yield CloudConfig(path)
  }
}
