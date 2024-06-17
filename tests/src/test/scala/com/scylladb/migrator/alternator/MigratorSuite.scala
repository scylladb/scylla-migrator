package com.scylladb.migrator.alternator

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }

import scala.util.chaining._
import scala.jdk.CollectionConverters._

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
      // Make sure the target database does not contain the table already
      deleteTableIfExists(sourceDDb, name)
      deleteTableIfExists(targetAlternator, name)
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
          assertEquals(result.getTable.getTableStatus, TableStatus.ACTIVE.toString)
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

  /** Delete the table from the provided database instance */
  def deleteTableIfExists(database: AmazonDynamoDB, name: String): Unit =
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

  /** Check that the table schema in the target database is the same as in the source database */
  def checkSchemaWasMigrated(tableName: String): Unit = {
    val sourceTableDesc = sourceDDb.describeTable(tableName).getTable
    checkSchemaWasMigrated(
      tableName,
      sourceTableDesc.getKeySchema,
      sourceTableDesc.getAttributeDefinitions
    )
  }

  /** Check that the table schema in the target database is equal to the provided schema */
  def checkSchemaWasMigrated(tableName: String, keySchema: java.util.List[KeySchemaElement], attributeDefinitions: java.util.List[AttributeDefinition]): Unit = {
    targetAlternator
      .describeTable(tableName)
      .getTable
      .tap { targetTableDesc =>
        assertEquals(targetTableDesc.getKeySchema, keySchema)
        assertEquals(targetTableDesc.getAttributeDefinitions, attributeDefinitions)
      }
  }

  /** Check that the target database contains the provided item description */
  def checkItemWasMigrated(tableName: String, itemKey: Map[String, AttributeValue], itemData: Map[String, AttributeValue]): Unit = {
    targetAlternator
      .getItem(new GetItemRequest(tableName, itemKey.asJava))
      .tap { itemResult =>
        assertEquals(itemResult.getItem.asScala.toMap, itemData)
      }
  }

}
