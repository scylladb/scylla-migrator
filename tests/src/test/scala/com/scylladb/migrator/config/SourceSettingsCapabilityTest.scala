package com.scylladb.migrator.config

import com.scylladb.migrator.config.SourceSettings.DynamoDBS3Export.{
  AttributeDefinition,
  AttributeType,
  KeySchema,
  KeyType,
  TableDescription
}

/** Backend-neutral checks for `SourceSettings.supportsSavepoints`.
  *
  * Acts as the single source of truth that the rest of the migrator (config decoder/encoder,
  * Migrator dispatcher, readers, alternator RDD path) trusts. If a new source is added without
  * declaring its capability, the trait's abstract `def` would not compile -- these tests pin the
  * intent of every existing subtype so a silent override drift fails fast.
  */
class SourceSettingsCapabilityTest extends munit.FunSuite {

  test("Cassandra supports savepoints (CQL token-range resume)") {
    val source = SourceSettings.Cassandra(
      host               = "cass.example.com",
      port               = 9042,
      localDC            = Some("dc1"),
      credentials        = None,
      sslOptions         = None,
      keyspace           = "ks",
      table              = "t",
      splitCount         = None,
      connections        = None,
      fetchSize          = 1000,
      preserveTimestamps = false,
      where              = None,
      consistencyLevel   = "LOCAL_QUORUM"
    )
    assert(source.supportsSavepoints)
  }

  test("DynamoDB supports savepoints (scan-segment resume)") {
    val source = SourceSettings.DynamoDB(
      endpoint              = None,
      region                = Some("us-east-1"),
      credentials           = None,
      table                 = "t",
      scanSegments          = None,
      readThroughput        = None,
      throughputReadPercent = None,
      maxMapTasks           = None
    )
    assert(source.supportsSavepoints)
  }

  test("Alternator supports savepoints (scan-segment resume)") {
    val source = SourceSettings.Alternator(
      alternatorEndpoint    = DynamoDBEndpoint("http://10.0.0.1", 8000),
      region                = None,
      credentials           = None,
      table                 = "t",
      scanSegments          = None,
      readThroughput        = None,
      throughputReadPercent = None,
      maxMapTasks           = None
    )
    assert(source.supportsSavepoints)
  }

  test("Parquet supports savepoints (file-level resume capability is available)") {
    // The runtime decision between savepoint-tracked and untracked Parquet reads is the
    // user-facing `savepoints.enableParquetFileTracking` toggle, NOT the source capability.
    val source = SourceSettings.Parquet(
      path        = "s3a://bucket/data/",
      credentials = None,
      endpoint    = None,
      region      = None
    )
    assert(source.supportsSavepoints)
  }

  test("MySQL does not support savepoints (JDBC reads have no durable per-range progress)") {
    val source = SourceSettings.MySQL(
      host                 = "mysql.example.com",
      port                 = 3306,
      database             = "db",
      table                = "t",
      credentials          = Credentials("u", "p"),
      primaryKey           = None,
      partitionColumn      = None,
      numPartitions        = None,
      lowerBound           = None,
      upperBound           = None,
      zeroDateTimeBehavior = SourceSettings.MySQL.ZeroDateTimeBehavior.Exception,
      fetchSize            = SourceSettings.MySQL.DefaultFetchSize,
      where                = None,
      connectionProperties = None
    )
    assert(!source.supportsSavepoints)
  }

  test(
    "DynamoDBS3Export does not support savepoints (RDD partition shape lacks segment tracking)"
  ) {
    val source = SourceSettings.DynamoDBS3Export(
      bucket      = "exports",
      manifestKey = "manifest.json",
      tableDescription = TableDescription(
        attributeDefinitions = Seq(AttributeDefinition("pk", AttributeType.S)),
        keySchema            = Seq(KeySchema("pk", KeyType.Hash))
      ),
      endpoint           = None,
      region             = Some("us-east-1"),
      credentials        = None,
      usePathStyleAccess = None
    )
    assert(!source.supportsSavepoints)
  }

  test(
    "Aerospike does not support savepoints (custom partition-range RDD lacks durable progress)"
  ) {
    val source = SourceSettings.Aerospike(
      hosts                   = Seq("aerospike.example.com"),
      port                    = None,
      namespace               = "test",
      set                     = "users",
      bins                    = None,
      splitCount              = None,
      schemaSampleSize        = None,
      queueSize               = None,
      credentials             = None,
      connectTimeoutMs        = None,
      socketTimeoutMs         = None,
      tlsName                 = None,
      schema                  = None,
      pollTimeoutSeconds      = None,
      preserveTTL             = None,
      preserveGeneration      = None,
      totalTimeoutMs          = None,
      maxScanRetries          = None,
      schemaDiscoveryStrategy = None,
      maxPollRetries          = None,
      maxConnsPerNode         = None,
      connPoolsPerNode        = None
    )
    assert(!source.supportsSavepoints)
  }
}
