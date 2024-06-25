package com.scylladb.migrator

import com.scylladb.migrator.config.DynamoDBEndpoint
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.net.URI

object AwsUtils {

  /** Configure an AWS SDK builder to use the provided endpoint, region and credentials provider */
  def configureClientBuilder[Builder <: AwsClientBuilder[Builder, Client], Client](
    builder: AwsClientBuilder[Builder, Client],
    maybeEndpoint: Option[DynamoDBEndpoint],
    maybeRegion: Option[String],
    maybeCredentialsProvider: Option[AwsCredentialsProvider]): builder.type = {

    for (endpoint <- maybeEndpoint) {
      builder.endpointOverride(new URI(endpoint.renderEndpoint))
    }
    maybeCredentialsProvider.foreach(builder.credentialsProvider)
    maybeRegion.map(Region.of).foreach(builder.region)

    builder
  }

  /**
    * Compute the final AWS credentials to use to call any AWS API.
    *
    * The credentials provided in the configuration may require acquiring temporary credentials by
    * delegation (“AssumeRole” in AWS jargon).
    */
  def computeFinalCredentials(maybeConfiguredCredentials: Option[config.AWSCredentials],
                              endpoint: Option[DynamoDBEndpoint],
                              region: Option[String]): Option[AWSCredentials] =
    maybeConfiguredCredentials.map { configuredCredentials =>
      val baseCredentials =
        AWSCredentials(configuredCredentials.accessKey, configuredCredentials.secretKey, None)
      configuredCredentials.assumeRole match {
        case None => baseCredentials
        case Some(role) =>
          val stsClient =
            configureClientBuilder(
              StsClient.builder(),
              endpoint,
              region,
              Some(
                StaticCredentialsProvider.create(AwsBasicCredentials
                  .create(configuredCredentials.accessKey, configuredCredentials.secretKey)))
            ).build()
          val response =
            stsClient.assumeRole(
              AssumeRoleRequest
                .builder()
                .roleArn(role.arn)
                .roleSessionName(role.getSessionName)
                .build()
            )
          val assumedCredentials = response.credentials
          AWSCredentials(
            assumedCredentials.accessKeyId,
            assumedCredentials.secretAccessKey,
            Some(assumedCredentials.sessionToken)
          )
      }
    }

}

/** Bare AWS credentials */
case class AWSCredentials(accessKey: String, secretKey: String, maybeSessionToken: Option[String]) {

  /** Convenient method to use our credentials with the AWS SDK */
  def toProvider: AwsCredentialsProvider = {
    val staticCredentials =
      maybeSessionToken match {
        case Some(sessionToken) => AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
        case None               => AwsBasicCredentials.create(accessKey, secretKey)
      }
    StaticCredentialsProvider.create(staticCredentials)
  }

}
