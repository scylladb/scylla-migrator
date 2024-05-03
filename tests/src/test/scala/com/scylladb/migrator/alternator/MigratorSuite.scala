package com.scylladb.migrator.alternator

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }

import scala.util.chaining._

/**
  * Base class for implementing end-to-end tests.
  *
  * It expects external services (DynamoDB, Scylla, Spark, etc.) to be running.
  * See the files `CONTRIBUTING.md` and `docker-compose-tests.yml` for more information.
  */
trait MigratorSuite extends munit.FunSuite {

  /** Client of a source DynamoDB instance */
  val sourceDDb: AmazonDynamoDB = AmazonDynamoDBClientBuilder
    .standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8001", "eu-central-1"))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
    .build()

  /** Client of a target Alternator instance */
  val targetAlternator: AmazonDynamoDB = AmazonDynamoDBClientBuilder
    .standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", "eu-central-1"))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
    .build()

  /**
    * Fixture automating the house-keeping work when migrating a table.
    *
    * It deletes the table from both the source and target databases in case it was already
    * existing, and then recreates it in the source database.
    *
    * After the test is executed, it deletes the table from both the source and target
    * databases.
    *
    * @param name Name of the table
    */
  def withTable(name: String): FunFixture[String] = FunFixture(
    setup = { _ =>
      def deleteTableIfExists(database: AmazonDynamoDB): Unit =
        try {
          database.deleteTable(name).ensuring { result =>
            result.getSdkHttpMetadata.getHttpStatusCode == 200
          }
          // Wait for the table to be effectively deleted
          // TODO After we upgrade the AWS SDK version, we could use “waiters”
          // https://aws.amazon.com/fr/blogs/developer/using-waiters-in-the-aws-sdk-for-java-2-x/
          Thread.sleep(5000)
        } catch {
          case _: ResourceNotFoundException =>
            // OK, the table was not existing
            ()
          case any: Throwable =>
            fail(s"Something did not work as expected: ${any}")
        }
      // Make sure the target database does not contain the table already
      deleteTableIfExists(sourceDDb)
      deleteTableIfExists(targetAlternator)
      try {
        // Create the table in the source database
        val createTableRequest =
          new CreateTableRequest()
            .withTableName(name)
            .withKeySchema(new KeySchemaElement("id", "HASH"))
            .withAttributeDefinitions(new AttributeDefinition("id", "S"))
            .withProvisionedThroughput(new ProvisionedThroughput(25L, 25L))
        sourceDDb.createTable(createTableRequest)
        // TODO Replace with “waiters” after we upgrade the AWS SDK
        Thread.sleep(5000)
        sourceDDb.describeTable(name).tap { result =>
          println(s"Table status is ${result.getTable.getTableStatus}")
        }
      } catch {
        case any: Throwable =>
          fail(s"Failed to created table ${name} in database ${sourceDDb}", any)
      }
      name
    },
    teardown = { _ =>
      // Clean-up both the source and target databases because we assume the test did replicate the table
      // to the target database
      targetAlternator.deleteTable(name)
      sourceDDb.deleteTable(name)
      ()
    }
  )

}
