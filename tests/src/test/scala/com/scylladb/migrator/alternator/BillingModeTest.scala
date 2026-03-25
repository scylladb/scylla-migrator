package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }

import scala.jdk.CollectionConverters._

/** Tests DynamoDB→Alternator migration across the full matrix of Scylla versions, keyspace types
  * (vnodes vs tablets) and billing modes (PAY_PER_REQUEST vs PROVISIONED).
  *
  * The matrix covers:
  *   - Old Scylla (< 2025.1.4): does not reject INDEXES consumed capacity
  *   - New Scylla (> 2025.1.4) with vnodes keyspace + PROVISIONED billing
  *   - New Scylla (> 2025.1.4) with tablets keyspace + PAY_PER_REQUEST billing
  *   - New Scylla (> 2025.1.4) with tablets keyspace + PROVISIONED billing
  *
  * Note: new Scylla with vnodes + PAY_PER_REQUEST is already covered by [[BasicMigrationTest]].
  *
  * See https://github.com/scylladb/scylla-migrator/issues/256 for background.
  */
abstract class BillingModeTest(val scyllaTarget: ScyllaAlternatorTarget, configFile: String)
    extends MigratorSuiteWithDynamoDBLocal {

  override def targetAlternatorPort: Int = scyllaTarget.alternatorPort

  withTable("BillingModeTest").test(
    s"${scyllaTarget.label}: migrate items to Alternator"
  ) { tableName =>
    val keys = Map("id" -> AttributeValue.fromS("billing-mode-test-id"))
    val attrs = Map("foo" -> AttributeValue.fromS("bar"))
    val itemData = keys ++ attrs

    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(itemData.asJava).build())

    successfullyPerformMigration(configFile)

    checkSchemaWasMigrated(tableName)
    checkItemWasMigrated(tableName, keys, itemData)
  }

}

/** Old Scylla (< 2025.1.4) with PAY_PER_REQUEST billing. */
class OldScyllaTest extends BillingModeTest(
      ScyllaAlternatorTarget.OldScylla,
      "dynamodb-to-alternator-old-scylla.yaml"
    )

/** New Scylla (> 2025.1.4) with vnodes keyspace and PROVISIONED billing. */
class NewScyllaVnodesProvisionedTest extends BillingModeTest(
      ScyllaAlternatorTarget.VnodesNewScylla,
      "dynamodb-to-alternator-vnodes-provisioned.yaml"
    )

/** New Scylla (> 2025.1.4) with tablets keyspace and PAY_PER_REQUEST billing. */
class NewScyllaTabletsPayPerRequestTest extends BillingModeTest(
      ScyllaAlternatorTarget.TabletsNewScylla,
      "dynamodb-to-alternator-tablets-pay-per-request.yaml"
    )

/** New Scylla (> 2025.1.4) with tablets keyspace and PROVISIONED billing. */
class NewScyllaTabletsProvisionedTest extends BillingModeTest(
      ScyllaAlternatorTarget.TabletsNewScylla,
      "dynamodb-to-alternator-tablets-provisioned.yaml"
    )
