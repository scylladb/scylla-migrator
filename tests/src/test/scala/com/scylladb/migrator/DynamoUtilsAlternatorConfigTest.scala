package com.scylladb.migrator

import com.scylladb.migrator.config.{ AlternatorSettings, DynamoDBEndpoint }
import org.apache.hadoop.dynamodb.DynamoDBConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

class DynamoUtilsAlternatorConfigTest extends munit.FunSuite {

  val spark: SparkSession =
    SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  test("setDynamoDBJobConf writes alternator settings to JobConf") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    val settings = AlternatorSettings(
      datacenter              = Some("dc1"),
      rack                    = Some("rack1"),
      activeRefreshIntervalMs = Some(500L),
      idleRefreshIntervalMs   = Some(30000L),
      compression             = Some(true),
      optimizeHeaders         = Some(true)
    )

    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false,
      alternatorSettings     = Some(settings)
    )

    assertEquals(jobConf.get("scylla.migrator.alternator.datacenter"), "dc1")
    assertEquals(jobConf.get("scylla.migrator.alternator.rack"), "rack1")
    assertEquals(jobConf.get("scylla.migrator.alternator.active_refresh_interval_ms"), "500")
    assertEquals(jobConf.get("scylla.migrator.alternator.idle_refresh_interval_ms"), "30000")
    assertEquals(jobConf.get("scylla.migrator.alternator.compression"), "true")
    assertEquals(jobConf.get("scylla.migrator.alternator.optimize_headers"), "true")
  }

  test("setDynamoDBJobConf without alternator settings leaves keys unset") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false
    )

    assertEquals(jobConf.get("scylla.migrator.alternator.datacenter"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.rack"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.active_refresh_interval_ms"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.idle_refresh_interval_ms"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.compression"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.optimize_headers"), null)
  }

  test("setDynamoDBJobConf with partial alternator settings") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    val settings = AlternatorSettings(
      datacenter              = Some("dc1"),
      rack                    = None,
      activeRefreshIntervalMs = None,
      idleRefreshIntervalMs   = Some(60000L),
      compression             = None,
      optimizeHeaders         = Some(false)
    )

    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false,
      alternatorSettings     = Some(settings)
    )

    assertEquals(jobConf.get("scylla.migrator.alternator.datacenter"), "dc1")
    assertEquals(jobConf.get("scylla.migrator.alternator.rack"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.active_refresh_interval_ms"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.idle_refresh_interval_ms"), "60000")
    assertEquals(jobConf.get("scylla.migrator.alternator.compression"), null)
    assertEquals(jobConf.get("scylla.migrator.alternator.optimize_headers"), "false")
  }

  test("Hadoop config round-trip preserves alternator settings") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    val originalSettings = AlternatorSettings(
      datacenter              = Some("dc1"),
      rack                    = Some("rack1"),
      activeRefreshIntervalMs = Some(500L),
      idleRefreshIntervalMs   = Some(30000L),
      compression             = Some(true),
      optimizeHeaders         = Some(true)
    )

    // Write settings to JobConf via setDynamoDBJobConf
    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false,
      alternatorSettings     = Some(originalSettings)
    )

    // Read settings back from JobConf (simulating what AlternatorLoadBalancingEnabler does)
    val reconstructed = AlternatorSettings(
      datacenter = Option(jobConf.get("scylla.migrator.alternator.datacenter")),
      rack       = Option(jobConf.get("scylla.migrator.alternator.rack")),
      activeRefreshIntervalMs =
        Option(jobConf.get("scylla.migrator.alternator.active_refresh_interval_ms")).map(_.toLong),
      idleRefreshIntervalMs =
        Option(jobConf.get("scylla.migrator.alternator.idle_refresh_interval_ms")).map(_.toLong),
      compression = Option(jobConf.get("scylla.migrator.alternator.compression")).map(_.toBoolean),
      optimizeHeaders =
        Option(jobConf.get("scylla.migrator.alternator.optimize_headers")).map(_.toBoolean)
    )

    assertEquals(reconstructed, originalSettings)
  }

  test("Hadoop config round-trip with no alternator settings yields all None") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false
    )

    val reconstructed = AlternatorSettings(
      datacenter = Option(jobConf.get("scylla.migrator.alternator.datacenter")),
      rack       = Option(jobConf.get("scylla.migrator.alternator.rack")),
      activeRefreshIntervalMs =
        Option(jobConf.get("scylla.migrator.alternator.active_refresh_interval_ms")).map(_.toLong),
      idleRefreshIntervalMs =
        Option(jobConf.get("scylla.migrator.alternator.idle_refresh_interval_ms")).map(_.toLong),
      compression = Option(jobConf.get("scylla.migrator.alternator.compression")).map(_.toBoolean),
      optimizeHeaders =
        Option(jobConf.get("scylla.migrator.alternator.optimize_headers")).map(_.toBoolean)
    )

    assertEquals(reconstructed, AlternatorSettings())
  }

  test("AlternatorLoadBalancingEnabler reads settings from Hadoop Configuration") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    val settings = AlternatorSettings(
      datacenter              = Some("dc1"),
      rack                    = Some("rack1"),
      activeRefreshIntervalMs = Some(500L),
      idleRefreshIntervalMs   = Some(30000L),
      compression             = Some(true),
      optimizeHeaders         = Some(true)
    )

    // Write all settings including endpoint and credentials
    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = Some(AWSCredentials("myAccessKey", "mySecretKey", None)),
      removeConsumedCapacity = true,
      alternatorSettings     = Some(settings)
    )

    // Instantiate the transformer and set its configuration
    val transformer = new DynamoUtils.AlternatorLoadBalancingEnabler
    transformer.setConf(jobConf)

    // Verify getConf round-trip
    assertEquals(transformer.getConf, jobConf)

    // Verify that the endpoint, region, and credentials are in the conf
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.ENDPOINT),
      "http://localhost:8000"
    )
    assertEquals(transformer.getConf.get(DynamoDBConstants.REGION), "us-east-1")
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF),
      "myAccessKey"
    )
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF),
      "mySecretKey"
    )
    assertEquals(
      transformer.getConf.get("scylla.migrator.remove_consumed_capacity"),
      "true"
    )

    // Verify alternator settings are readable from the conf
    assertEquals(transformer.getConf.get("scylla.migrator.alternator.datacenter"), "dc1")
    assertEquals(transformer.getConf.get("scylla.migrator.alternator.rack"), "rack1")
    assertEquals(
      transformer.getConf.get("scylla.migrator.alternator.active_refresh_interval_ms"),
      "500"
    )
    assertEquals(
      transformer.getConf.get("scylla.migrator.alternator.idle_refresh_interval_ms"),
      "30000"
    )
    assertEquals(transformer.getConf.get("scylla.migrator.alternator.compression"), "true")
    assertEquals(
      transformer.getConf.get("scylla.migrator.alternator.optimize_headers"),
      "true"
    )
  }

  test("AlternatorLoadBalancingEnabler with no endpoint returns the original builder") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = None,
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false
    )

    val transformer = new DynamoUtils.AlternatorLoadBalancingEnabler
    transformer.setConf(jobConf)

    // When no endpoint is set, apply() should return a builder (not throw)
    // We can't introspect the builder, but we can verify it doesn't error
    import software.amazon.awssdk.services.dynamodb.DynamoDbClient
    val inputBuilder = DynamoDbClient.builder()
    val result = transformer.apply(inputBuilder)
    assert(result != null)
  }

  test("AlternatorLoadBalancingEnabler with anonymous credentials (no accessKey/secretKey)") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    // Set endpoint but no credentials - should use anonymous access
    DynamoUtils.setDynamoDBJobConf(
      jobConf                = jobConf,
      maybeRegion            = Some("us-east-1"),
      maybeEndpoint          = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments      = None,
      maybeMaxMapTasks       = None,
      maybeAwsCredentials    = None,
      removeConsumedCapacity = false
    )

    val transformer = new DynamoUtils.AlternatorLoadBalancingEnabler
    transformer.setConf(jobConf)

    // Verify no credentials are set
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF),
      null
    )
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF),
      null
    )
  }

  test("AlternatorLoadBalancingEnabler with session token credentials") {
    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    DynamoUtils.setDynamoDBJobConf(
      jobConf           = jobConf,
      maybeRegion       = Some("us-east-1"),
      maybeEndpoint     = Some(DynamoDBEndpoint("http://localhost", 8000)),
      maybeScanSegments = None,
      maybeMaxMapTasks  = None,
      maybeAwsCredentials =
        Some(AWSCredentials("myAccessKey", "mySecretKey", Some("mySessionToken"))),
      removeConsumedCapacity = false
    )

    val transformer = new DynamoUtils.AlternatorLoadBalancingEnabler
    transformer.setConf(jobConf)

    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF),
      "myAccessKey"
    )
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF),
      "mySecretKey"
    )
    assertEquals(
      transformer.getConf.get(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF),
      "mySessionToken"
    )
  }

  override def afterAll(): Unit =
    spark.stop()

}
