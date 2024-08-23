package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.submitSparkJobProcess
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.chaining.scalaUtilChainingOps

class StreamedItemsTest extends MigratorSuiteWithAWS {

  withTable("StreamedItemsTest").test("Stream changes") { tableName =>
    val configFileName = "dynamodb-to-alternator-streaming.yaml"
    setupConfigurationFile(configFileName)

    // Populate the source table
    val keys1 = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
    val item1Data = keys1 ++ attrs1
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build())

    // Perform the migration
    val sparkLogs = new StringBuilder()
    val sparkJob =
      submitSparkJobProcess(configFileName, "com.scylladb.migrator.Migrator")
        .run(ProcessLogger { log =>
          sparkLogs ++= log
//          println(log) // Uncomment to see the logs
        })

    awaitAtMost(60.seconds) {
      assert(sparkLogs.toString().contains(s"Table ${tableName} created."))
    }
    // Check that the table initial snapshot has been successfully migrated
    awaitAtMost(60.seconds) {
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys1.asJava).build())
        .tap { itemResult =>
          assert(itemResult.hasItem, "First item not found in the target database")
          assertEquals(itemResult.item.asScala.toMap, item1Data)
        }
    }

    // Insert one more item
    val keys2 = Map("id" -> AttributeValue.fromS("67890"))
    val attrs2 = Map(
      "foo" -> AttributeValue.fromS("baz"),
      "baz" -> AttributeValue.fromBool(false)
    )
    val item2Data = keys2 ++ attrs2
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build())

    // Check that the added item has also been migrated
    awaitAtMost(60.seconds) {
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build())
        .tap { itemResult =>
          assert(itemResult.hasItem, "Second item not found in the target database")
          assertEquals(
            itemResult.item.asScala.toMap,
            item2Data + ("_dynamo_op_type" -> AttributeValue.fromBool(true)))
        }
    }

    // Stop the migration job
    stopSparkJob(configFileName)
    assertEquals(sparkJob.exitValue(), 143) // 143 = SIGTERM

    deleteStreamTable(tableName)
  }

  withTable("StreamedItemsSkipSnapshotTest").test("Stream changes but skip initial snapshot") { tableName =>
    val configFileName = "dynamodb-to-alternator-streaming-skip-snapshot.yaml"
    setupConfigurationFile(configFileName)

    // Populate the source table
    val keys1 = Map("id"   -> AttributeValue.fromS("12345"))
    val attrs1 = Map("foo" -> AttributeValue.fromS("bar"))
    val item1Data = keys1 ++ attrs1
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item1Data.asJava).build())

    // Perform the migration
    val sparkLogs = new StringBuilder()
    val sparkJob =
      submitSparkJobProcess(configFileName, "com.scylladb.migrator.Migrator")
        .run(ProcessLogger { (log: String) =>
          sparkLogs ++= log
//          println(log) // Uncomment to see the logs
        })

    // Wait for the changes to start being streamed
    awaitAtMost(60.seconds) {
      assert(sparkLogs.toString().contains("alternator: Starting to transfer changes"))
    }

    // Insert one more item
    val keys2 = Map("id" -> AttributeValue.fromS("67890"))
    val attrs2 = Map(
      "foo" -> AttributeValue.fromS("baz"),
      "baz" -> AttributeValue.fromBool(false)
    )
    val item2Data = keys2 ++ attrs2
    sourceDDb().putItem(PutItemRequest.builder().tableName(tableName).item(item2Data.asJava).build())

    // Check that only the second item has been migrated
    awaitAtMost(60.seconds) {
      targetAlternator()
        .getItem(GetItemRequest.builder().tableName(tableName).key(keys2.asJava).build())
        .tap { itemResult =>
          assert(itemResult.hasItem, "Second item not found in the target database")
          assertEquals(
            itemResult.item.asScala.toMap,
            item2Data + ("_dynamo_op_type" -> AttributeValue.fromBool(true)))
        }
    }
    targetAlternator()
      .getItem(GetItemRequest.builder().tableName(tableName).key(keys1.asJava).build())
      .tap { itemResult =>
        assert(!itemResult.hasItem, "First item found in the target database")
      }

    // Stop the migration job
    stopSparkJob(configFileName)
    assertEquals(sparkJob.exitValue(), 143)

    deleteStreamTable(tableName)
  }

  /**
   * Continuously evaluate the provided `assertion` until it terminates
   * (MUnit models failures with exceptions of type `AssertionError`),
   * or until the provided `delay` passed.
   */
  def awaitAtMost(delay: FiniteDuration)(assertion: => Unit): Unit = {
    val deadline = delay.fromNow
    var maybeFailure: Option[AssertionError] = Some(new AssertionError("Assertion has not been tested yet"))
    while (maybeFailure.isDefined && deadline.hasTimeLeft()) {
      try {
        assertion
        maybeFailure = None
      } catch {
        case failure: AssertionError =>
          maybeFailure = Some(failure)
          Thread.sleep(1000)
      }
    }
    for (failure <- maybeFailure) {
      fail(s"Assertion was not true after ${delay}", failure)
    }
  }

  /**
   * Delete the DynamoDB table automatically created by the Kinesis library
   * @param sourceTableName Source table that had streams enabled
   */
  private def deleteStreamTable(sourceTableName: String): Unit = {
    sourceDDb()
      .listTablesPaginator()
      .tableNames()
      .stream()
      .filter(name => name.startsWith(s"migrator_${sourceTableName}_"))
      .forEach { streamTableName =>
        deleteTableIfExists(sourceDDb(), streamTableName)
      }
  }

  // This looks more complicated than it should, but this is what it takes to stop a
  // process started via “docker compose exec ...”.
  // Indeed, stopping the host process does not stop the container process, see:
  // https://github.com/moby/moby/issues/9098
  // So, we look into the container for the PID of the migration process and kill it.
  private def stopSparkJob(migrationConfigFile: String): Unit = {
    val commandPrefix = Seq("docker", "compose", "-f", "../docker-compose-tests.yml", "exec", "spark-master")
    val pid =
      Process(commandPrefix ++ Seq("ps", "-ef"))
        .lazyLines
        // Find the process that contains the arguments we passed when we submitted the Spark job
        .filter(_.contains(s"--conf spark.scylla.config=/app/configurations/${migrationConfigFile}"))
        .head
        .split("\\s+")
        .apply(1) // The 2nd column contains the process ID
    Process(commandPrefix ++ Seq("kill", pid))
      .run()
      .exitValue()
      .ensuring(_ == 0)
  }

}
