package com.scylladb.migrator

import com.amazonaws.auth.{
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  BasicSessionCredentials
}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import com.scylladb.migrator.config.{ AWSAssumeRole, DynamoDBEndpoint }
object AwsUtils {

  /** Configure an AWS SDK builder to use the provided endpoint, region and credentials provider */
  def configureClientBuilder[Builder <: AwsClientBuilder[Builder, Client], Client](
    builder: AwsClientBuilder[Builder, Client],
    maybeEndpoint: Option[DynamoDBEndpoint],
    maybeRegion: Option[String],
    maybeCredentialsProvider: Option[AWSCredentialsProvider]): builder.type = {

    for (endpoint <- maybeEndpoint) {
      builder.setEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          endpoint.renderEndpoint,
          maybeRegion.getOrElse("empty")))
    }
    maybeCredentialsProvider.foreach(builder.setCredentials)
    maybeRegion.foreach(builder.setRegion)

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
        case Some(AWSAssumeRole(roleArn, roleSessionName)) =>
          val stsClient =
            configureClientBuilder(
              AWSSecurityTokenServiceClientBuilder.standard(),
              endpoint,
              region,
              Some(
                new AWSStaticCredentialsProvider(
                  new BasicAWSCredentials(
                    configuredCredentials.accessKey,
                    configuredCredentials.secretKey)))
            ).build()
          val response =
            stsClient.assumeRole(
              new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName(roleSessionName)
            )
          val assumedCredentials = response.getCredentials
          AWSCredentials(
            assumedCredentials.getAccessKeyId,
            assumedCredentials.getSecretAccessKey,
            Some(assumedCredentials.getSessionToken)
          )
      }
    }

}

/** Bare AWS credentials */
case class AWSCredentials(accessKey: String, secretKey: String, maybeSessionToken: Option[String]) {
  /** Convenient method to use our credentials with the AWS SDK */
  def toProvider: AWSCredentialsProvider = {
    val staticCredentials =
      maybeSessionToken match {
        case Some(sessionToken) => new BasicSessionCredentials(accessKey, secretKey, sessionToken)
        case None               => new BasicAWSCredentials(accessKey, secretKey)
      }
    new AWSStaticCredentialsProvider(staticCredentials)
  }

}
