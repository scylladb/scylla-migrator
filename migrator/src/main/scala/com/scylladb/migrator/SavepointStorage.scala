package com.scylladb.migrator

import com.scylladb.migrator.config.{ MigratorConfig, SavepointsTarget, SparkSecretRedaction }
import org.apache.hadoop.conf.Configuration

/** Builds the Hadoop [[Configuration]] used to access the savepoints store.
  *
  * The credentials and connector settings for the savepoints bucket are derived from
  * `savepoints.target` (S3/GCS). Both [[SavepointsManager]] (which writes savepoints) and the
  * startup auto-resume resolver ([[com.scylladb.migrator.config.SavepointsResume]], which reads the
  * latest savepoint) must build an identical configuration to reach the same store, so the logic
  * lives here in one place.
  */
private[migrator] object SavepointStorage {
  private val S3ASimpleCredentialsProvider =
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  private val S3ATemporaryCredentialsProvider =
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
  private val GcsServiceAccountJsonKeyfileAuthType = "SERVICE_ACCOUNT_JSON_KEYFILE"

  def hadoopConfiguration(
    migratorConfig: MigratorConfig,
    baseConfiguration: Option[Configuration],
    redactionRegex: Option[String]
  ): Configuration = {
    val conf = baseConfiguration.map(new Configuration(_)).getOrElse(new Configuration())

    migratorConfig.savepoints.target.foreach {
      case s3: SavepointsTarget.S3 =>
        s3.region.foreach(conf.set("fs.s3a.endpoint.region", _))
        s3.credentials.foreach { configuredCredentials =>
          AwsUtils.computeFinalCredentials(Some(configuredCredentials), None, s3.region).foreach {
            credentials =>
              val credentialOptions =
                Seq(
                  "fs.s3a.access.key" -> credentials.accessKey,
                  "fs.s3a.secret.key" -> credentials.secretKey
                ) ++ credentials.maybeSessionToken.toSeq.map { sessionToken =>
                  "fs.s3a.session.token" -> sessionToken
                }

              ensureHadoopKeysRedacted(redactionRegex, credentialOptions.map(_._1))
              credentialOptions.foreach { case (key, value) => conf.set(key, value) }
              credentials.maybeSessionToken match {
                case Some(_) =>
                  conf.set("fs.s3a.aws.credentials.provider", S3ATemporaryCredentialsProvider)
                case None =>
                  conf.set("fs.s3a.aws.credentials.provider", S3ASimpleCredentialsProvider)
                  conf.unset("fs.s3a.session.token")
              }
          }
        }
      case gcs: SavepointsTarget.GCS =>
        gcs.projectId.foreach(conf.set("fs.gs.project.id", _))
        gcs.credentials.foreach { credentials =>
          ensureHadoopKeysRedacted(redactionRegex, Seq("fs.gs.auth.service.account.json.keyfile"))
          conf.set("fs.gs.auth.type", GcsServiceAccountJsonKeyfileAuthType)
          conf.set(
            "fs.gs.auth.service.account.json.keyfile",
            credentials.serviceAccountJsonKeyfile
          )
        }
      case _ => ()
    }

    conf
  }

  private def ensureHadoopKeysRedacted(
    redactionRegex: Option[String],
    keys: Iterable[String]
  ): Unit =
    SparkSecretRedaction.ensureKeysRedacted(
      redactionRegex,
      keys,
      "savepoint Hadoop configuration"
    )
}
