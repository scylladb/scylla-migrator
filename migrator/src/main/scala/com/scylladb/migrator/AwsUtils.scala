package com.scylladb.migrator

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.scylladb.migrator.config.DynamoDBEndpoint

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

}
