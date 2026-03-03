package com.scylladb.migrator

import com.scylladb.migrator.config.DynamoDBEndpoint
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.net.URI

/** An [[AwsCredentialsProvider]] that also closes the underlying [[StsClient]] when it is closed.
  * This prevents resource leaks in long-running jobs.
  */
class CloseableStsCredentialsProvider(
  stsClient: StsClient,
  delegate: StsAssumeRoleCredentialsProvider
) extends AwsCredentialsProvider with AutoCloseable {
  override def resolveCredentials() = delegate.resolveCredentials()
  override def identityType() = delegate.identityType()
  override def resolveIdentity() = delegate.resolveIdentity()
  override def close(): Unit = {
    delegate.close()
    stsClient.close()
  }
}

object AwsUtils {

  /** Configure an AWS SDK builder to use the provided endpoint, region and credentials provider */
  def configureClientBuilder[Builder <: AwsClientBuilder[Builder, Client], Client](
    builder: AwsClientBuilder[Builder, Client],
    maybeEndpoint: Option[DynamoDBEndpoint],
    maybeRegion: Option[String],
    maybeCredentialsProvider: Option[AwsCredentialsProvider]
  ): builder.type = {

    for (endpoint <- maybeEndpoint)
      builder.endpointOverride(new URI(endpoint.renderEndpoint))
    maybeCredentialsProvider.foreach(builder.credentialsProvider)
    maybeRegion.map(Region.of).foreach(builder.region)

    builder
  }

  /** Compute the final AWS credentials provider to use to call any AWS API.
    *
    * The credentials provided in the configuration may require acquiring temporary credentials by
    * delegation (“AssumeRole” in AWS jargon). In that case, an [[StsAssumeRoleCredentialsProvider]]
    * is returned, which automatically refreshes the temporary credentials before they expire. This
    * is critical for long-running stream replication jobs that run continuously for days/weeks.
    */
  def computeFinalCredentials(
    maybeConfiguredCredentials: Option[config.AWSCredentials],
    endpoint: Option[DynamoDBEndpoint],
    region: Option[String]
  ): Option[AwsCredentialsProvider] =
    maybeConfiguredCredentials.map { configuredCredentials =>
      val baseProvider =
        StaticCredentialsProvider.create(
          AwsBasicCredentials
            .create(configuredCredentials.accessKey, configuredCredentials.secretKey)
        )
      configuredCredentials.assumeRole match {
        case None => baseProvider
        case Some(role) =>
          val stsClient =
            configureClientBuilder(
              StsClient.builder(),
              endpoint,
              region,
              Some(baseProvider)
            ).build()
          val assumeRoleProvider = StsAssumeRoleCredentialsProvider
            .builder()
            .stsClient(stsClient)
            .refreshRequest(
              AssumeRoleRequest
                .builder()
                .roleArn(role.arn)
                .roleSessionName(role.getSessionName)
                .build()
            )
            .build()
          new CloseableStsCredentialsProvider(stsClient, assumeRoleProvider)
      }
    }

}
