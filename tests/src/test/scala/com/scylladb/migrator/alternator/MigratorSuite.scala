package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ AWS, Integration }
import org.junit.experimental.categories.Category
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  GetItemRequest,
  GlobalSecondaryIndexDescription,
  KeySchemaElement,
  KeyType,
  LocalSecondaryIndexDescription,
  ProvisionedThroughput,
  ResourceNotFoundException,
  ScalarAttributeType
}

import java.net.URI
import java.nio.file.Paths
import scala.util.chaining._
import scala.jdk.CollectionConverters._
import scala.sys.process.stringToProcess

/** Base class for implementing end-to-end tests.
  *
  * It expects external services (DynamoDB, Scylla, Spark, etc.) to be running. See the files
  * `CONTRIBUTING.md` and `docker-compose-tests.yml` for more information.
  */
trait MigratorSuite extends munit.FunSuite {
  import com.scylladb.migrator.DynamoUtils
  import com.scylladb.migrator.config.DynamoDBEndpoint

  /** Client of a source DynamoDB instance */
  def sourceDDb: Fixture[DynamoDbClient]

  /** Client of a target Alternator instance */
  val targetAlternator: Fixture[DynamoDbClient] = new Fixture[DynamoDbClient]("targetAlternator") {
    private var client: DynamoDbClient = null
    def apply(): DynamoDbClient = client
    override def beforeAll(): Unit =
      client = DynamoUtils.buildDynamoClient(
        endpoint = Some(DynamoDBEndpoint("http://localhost", 8000)),
        creds =
          Some(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))),
        region       = Some("dummy"),
        interceptors = Seq(new DynamoUtils.RemoveConsumedCapacityInterceptor)
      )
    override def afterAll(): Unit = client.close()
  }

  /** Fixture automating the house-keeping work when migrating a table.
    *
    * It deletes the table from both the source and target databases in case it was already
    * existing, and then recreates it in the source database.
    *
    * After the test is executed, it deletes the table from both the source and target databases.
    *
    * @param name
    *   Name of the table
    */
  def withTable(name: String): FunFixture[String] = FunFixture(
    setup = { _ =>
      // Make sure the target database does not contain the table already
      deleteTableIfExists(sourceDDb(), name)
      deleteTableIfExists(targetAlternator(), name)
      try {
        // Create the table in the source database
        val createTableRequest =
          CreateTableRequest
            .builder()
            .tableName(name)
            .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
            .attributeDefinitions(
              AttributeDefinition
                .builder()
                .attributeName("id")
                .attributeType(ScalarAttributeType.S)
                .build()
            )
            .provisionedThroughput(
              ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
            )
            .build()
        sourceDDb().createTable(createTableRequest)
        val waiterResponse =
          sourceDDb()
            .waiter()
            .waitUntilTableExists(describeTableRequest(name))
        assert(
          waiterResponse.matched().response().isPresent,
          s"Failed to create table ${name}: ${waiterResponse.matched().exception().get()}"
        )
      } catch {
        case any: Throwable =>
          fail(s"Failed to create table ${name} in database ${sourceDDb()}", any)
      }
      name
    },
    teardown = { _ =>
      // Clean-up both the source and target databases because we assume the test did replicate the table
      // to the target database
      targetAlternator().deleteTable(DeleteTableRequest.builder().tableName(name).build())
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(name).build())
      ()
    }
  )

  def describeTableRequest(name: String): DescribeTableRequest =
    DescribeTableRequest.builder().tableName(name).build()

  /** Delete the table from the provided database instance */
  def deleteTableIfExists(database: DynamoDbClient, name: String): Unit =
    try {
      database.deleteTable(DeleteTableRequest.builder().tableName(name).build()).ensuring { result =>
        result.sdkHttpResponse().isSuccessful
      }
      val maybeFailure =
        database
          .waiter()
          .waitUntilTableNotExists(describeTableRequest(name))
          .matched()
          .exception()
      if (maybeFailure.isPresent) {
        throw maybeFailure.get()
      }
    } catch {
      case _: ResourceNotFoundException =>
        // OK, the table was not existing or the waiter completed with the ResourceNotFoundException
        ()
      case any: Throwable =>
        fail(s"Failed to delete table ${name}", any)
    }

  /** Check that the table schema in the target database is the same as in the source database */
  def checkSchemaWasMigrated(tableName: String): Unit = {
    val sourceTableDesc = sourceDDb().describeTable(describeTableRequest(tableName)).table
    checkSchemaWasMigrated(
      tableName,
      sourceTableDesc.keySchema,
      sourceTableDesc.attributeDefinitions,
      sourceTableDesc.localSecondaryIndexes,
      sourceTableDesc.globalSecondaryIndexes
    )
  }

  /** Check that the table schema in the target database is equal to the provided schema */
  def checkSchemaWasMigrated(
    tableName: String,
    keySchema: java.util.List[KeySchemaElement],
    attributeDefinitions: java.util.List[AttributeDefinition],
    localSecondaryIndexes: java.util.List[LocalSecondaryIndexDescription],
    globalSecondaryIndexes: java.util.List[GlobalSecondaryIndexDescription]
  ): Unit =
    targetAlternator()
      .describeTable(describeTableRequest(tableName))
      .table
      .tap { targetTableDesc =>
        // Partition key
        assertEquals(targetTableDesc.keySchema, keySchema)

        // Attribute definitions
        assertEquals(
          targetTableDesc.attributeDefinitions.asScala.toSet,
          attributeDefinitions.asScala.toSet
        )

        // Local secondary indexes: do not compare their ARN, which always unique
        def localIndexRelevantProperties(index: LocalSecondaryIndexDescription) =
          (index.indexName, index.keySchema, index.projection)
        assertEquals(
          targetTableDesc.localSecondaryIndexes.asScala.map(localIndexRelevantProperties),
          localSecondaryIndexes.asScala.map(localIndexRelevantProperties)
        )

        // Global secondary indexes: do not compare ARN and provisioned throughput (see https://github.com/scylladb/scylladb/issues/19718)
        def globalIndexRelevantProperties(index: GlobalSecondaryIndexDescription) =
          (index.indexName, index.keySchema, index.projection /*, index.provisionedThroughput*/ )
        assertEquals(
          targetTableDesc.globalSecondaryIndexes.asScala.map(globalIndexRelevantProperties),
          globalSecondaryIndexes.asScala.map(globalIndexRelevantProperties)
        )
      }

  /** Check that the target database contains the provided item description */
  def checkItemWasMigrated(
    tableName: String,
    itemKey: Map[String, AttributeValue],
    itemData: Map[String, AttributeValue]
  ): Unit =
    targetAlternator()
      .getItem(GetItemRequest.builder.tableName(tableName).key(itemKey.asJava).build())
      .tap { itemResult =>
        assertEquals(itemResult.item.asScala.toMap, itemData)
      }

  override def munitFixtures: Seq[Fixture[_]] = Seq(sourceDDb, targetAlternator)
}

@Category(Array(classOf[Integration]))
abstract class MigratorSuiteWithDynamoDBLocal extends MigratorSuite {

  lazy val sourceDDb: Fixture[DynamoDbClient] = new Fixture[DynamoDbClient]("sourceDDb") {
    private var client: DynamoDbClient = null
    def apply(): DynamoDbClient = client
    override def beforeAll(): Unit =
      client = DynamoDbClient
        .builder()
        .region(Region.of("dummy"))
        .endpointOverride(new URI("http://localhost:8001"))
        .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
        )
        .build()
    override def afterAll(): Unit = client.close()
  }

}

@Category(Array(classOf[Integration], classOf[AWS]))
abstract class MigratorSuiteWithAWS extends MigratorSuite {

  lazy val sourceDDb: Fixture[DynamoDbClient] = new Fixture[DynamoDbClient]("sourceDDb") {
    private var client: DynamoDbClient = null
    def apply(): DynamoDbClient = client
    override def beforeAll(): Unit = {
      // Provision the AWS credentials on the Spark nodes via a Docker volume
      val localAwsCredentials =
        Paths.get(sys.props("user.home"), ".aws", "credentials").toAbsolutePath
      (s"cp ${localAwsCredentials} docker/aws-profile/credentials").!!

      val region = Region.of(sys.env("AWS_REGION"))
      client = DynamoDbClient
        .builder()
        .region(region)
        .build()
    }
    override def afterAll(): Unit = client.close()
  }

}
