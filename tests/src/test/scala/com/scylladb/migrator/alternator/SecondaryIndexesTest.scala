package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  CreateTableRequest,
  DeleteTableRequest,
  GlobalSecondaryIndex,
  KeySchemaElement,
  KeyType,
  LocalSecondaryIndex,
  Projection,
  ProjectionType,
  ProvisionedThroughput,
  ScalarAttributeType
}

class SecondaryIndexesTest extends MigratorSuiteWithDynamoDBLocal {

  val tableName = "TableWithSecondaryIndexes"
  val withResources: FunFixture[Unit] = FunFixture(
    setup = _ => {
      deleteTableIfExists(sourceDDb(), tableName)
      deleteTableIfExists(targetAlternator(), tableName)
      try {
        val createTableRequest =
          CreateTableRequest
            .builder()
            .tableName(tableName)
            .keySchema(
              KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName("z").keyType(KeyType.RANGE).build()
            )
            .attributeDefinitions(
              AttributeDefinition
                .builder()
                .attributeName("id")
                .attributeType(ScalarAttributeType.S)
                .build(),
              AttributeDefinition
                .builder()
                .attributeName("x")
                .attributeType(ScalarAttributeType.N)
                .build(),
              AttributeDefinition
                .builder()
                .attributeName("y")
                .attributeType(ScalarAttributeType.N)
                .build(),
              AttributeDefinition
                .builder()
                .attributeName("z")
                .attributeType(ScalarAttributeType.N)
                .build()
            )
            .provisionedThroughput(
              ProvisionedThroughput.builder().readCapacityUnits(25L).writeCapacityUnits(25L).build()
            )
            .localSecondaryIndexes(
              LocalSecondaryIndex
                .builder()
                .indexName("local")
                .keySchema(
                  KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
                  KeySchemaElement.builder().attributeName("y").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
            )
            .globalSecondaryIndexes(
              GlobalSecondaryIndex
                .builder()
                .indexName("global")
                .provisionedThroughput(
                  ProvisionedThroughput
                    .builder()
                    .readCapacityUnits(1L)
                    .writeCapacityUnits(1L)
                    .build()
                )
                .keySchema(
                  KeySchemaElement.builder().attributeName("x").keyType(KeyType.HASH).build(),
                  KeySchemaElement.builder().attributeName("z").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
            )
            .build()
        sourceDDb().createTable(createTableRequest)
        val waiterResponse =
          sourceDDb()
            .waiter()
            .waitUntilTableExists(describeTableRequest(tableName))
        assert(
          waiterResponse.matched().response().isPresent,
          s"Failed to create table ${tableName}: ${waiterResponse.matched().exception().get()}"
        )
      } catch {
        case any: Throwable =>
          fail(s"Failed to create table ${tableName} in database ${sourceDDb()}", any)
      }
      ()
    },
    teardown = _ => {
      targetAlternator().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      sourceDDb().deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      ()
    }
  )

  withResources.test("The secondary indexes of a table are correctly replicated") { _ =>
    successfullyPerformMigration("dynamodb-to-alternator-secondary-indexes.yaml")
    checkSchemaWasMigrated(tableName)
  }

}
