package com.scylladb.migrator

import com.scylladb.migrator.config.{
  AWSCredentials => ConfigAWSCredentials,
  AlternatorSettings,
  Credentials,
  DynamoDBEndpoint,
  MigratorConfig,
  SSLOptions,
  Savepoints,
  SourceSettings,
  StreamChangesSetting,
  TargetSettings
}

class ValidatorLoggingTest extends munit.FunSuite {
  private val syntheticTargetPassword = "TEST_ONLY_TARGET_PASSWORD"
  private val syntheticTruststorePassword = "TEST_ONLY_TRUSTSTORE_PASSWORD"

  private val syntheticCassandraToScyllaConfig = MigratorConfig(
    source = SourceSettings.Cassandra(
      host               = "source.cassandra.test.invalid",
      port               = 9042,
      localDC            = None,
      credentials        = None,
      sslOptions         = None,
      keyspace           = "keyspace1",
      table              = "standard1",
      splitCount         = Some(96),
      connections        = Some(16),
      fetchSize          = 1000,
      preserveTimestamps = true,
      where              = None,
      consistencyLevel   = "QUORUM"
    ),
    target = TargetSettings.Scylla(
      host        = "target.scylla.test.invalid",
      port        = 19142,
      localDC     = None,
      credentials = Some(Credentials("scylla", syntheticTargetPassword)),
      sslOptions = Some(
        SSLOptions(
          clientAuthEnabled  = false,
          enabled            = true,
          enabledAlgorithms  = None,
          keyStorePassword   = None,
          keyStorePath       = None,
          keyStoreType       = None,
          protocol           = None,
          trustStorePassword = Some(syntheticTruststorePassword),
          trustStorePath     = Some("truststore.jks"),
          trustStoreType     = None
        )
      ),
      keyspace                      = "keyspace1",
      table                         = "standard1",
      connections                   = Some(16),
      stripTrailingZerosForDecimals = false,
      writeTTLInS                   = None,
      writeWritetimestampInuS       = None,
      consistencyLevel              = "QUORUM"
    ),
    renames          = Some(List()),
    savepoints       = Savepoints(300, "gs://example-redaction-fixture/savepoints"),
    skipTokenRanges  = Some(Set()),
    skipSegments     = None,
    skipParquetFiles = None,
    validation       = None
  )

  private def assertSecretsAbsent(rendered: String, secrets: Seq[String]): Unit =
    secrets.zipWithIndex.foreach { case (secret, index) =>
      assert(
        !rendered.contains(secret),
        s"rendered config must not contain secret at index $index"
      )
    }

  test("loadedConfigLogMessage redacts source and target secrets") {
    val config = MigratorConfig(
      source = SourceSettings.MySQL(
        host                 = "mysql.example.com",
        port                 = 3306,
        database             = "app",
        table                = "users",
        credentials          = Credentials("mysql-user", "mysql-secret"),
        primaryKey           = Some(List("id")),
        partitionColumn      = None,
        numPartitions        = None,
        lowerBound           = None,
        upperBound           = None,
        fetchSize            = 1000,
        where                = Some("email = 'user@example.com'"),
        connectionProperties = Some(Map("trustStorePassword" -> "tls-secret"))
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = Some(Credentials("scylla-user", "scylla-secret")),
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "users",
        connections                   = Some(4),
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(300, "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )

    val logMessage = Validator.loadedConfigLogMessage(config)

    assert(logMessage.startsWith("Loaded config:\n"))
    assert(logMessage.contains("<redacted>"))
    assert(!logMessage.contains("mysql-secret"))
    assert(!logMessage.contains("scylla-secret"))
    assert(!logMessage.contains("tls-secret"))
    assert(!logMessage.contains("user@example.com"))
  }

  test("loadedConfigLogMessage covers reported Cassandra to Scylla credential leak") {
    val logMessage = Validator.loadedConfigLogMessage(syntheticCassandraToScyllaConfig)

    assert(logMessage.startsWith("Loaded config:\n"))
    assert(logMessage.contains("<redacted>"))
    assertSecretsAbsent(logMessage, Seq(syntheticTargetPassword, syntheticTruststorePassword))
  }

  test("MigratorConfig toString is redacted as a fallback for accidental case-class logging") {
    val rendered = syntheticCassandraToScyllaConfig.toString

    assert(rendered.contains("<redacted>"))
    assertSecretsAbsent(rendered, Seq(syntheticTargetPassword, syntheticTruststorePassword))
  }

  test("renderRedacted redacts secrets for every source and target type") {
    val s3ExportDescription = SourceSettings.DynamoDBS3Export.TableDescription(
      attributeDefinitions = Seq(
        SourceSettings.DynamoDBS3Export.AttributeDefinition(
          "id",
          SourceSettings.DynamoDBS3Export.AttributeType.S
        )
      ),
      keySchema = Seq(
        SourceSettings.DynamoDBS3Export.KeySchema(
          "id",
          SourceSettings.DynamoDBS3Export.KeyType.Hash
        )
      )
    )

    val configsAndSecrets = Seq(
      syntheticCassandraToScyllaConfig -> Seq(syntheticTargetPassword, syntheticTruststorePassword),
      MigratorConfig(
        source = SourceSettings.Cassandra(
          host        = "cassandra",
          port        = 9042,
          localDC     = Some("dc1"),
          credentials = Some(Credentials("src-user", "SRC_CQL_PASS_123")),
          sslOptions = Some(
            SSLOptions(
              clientAuthEnabled  = true,
              enabled            = true,
              enabledAlgorithms  = None,
              keyStorePassword   = Some("SRC_CQL_KEYSTORE_PASS_123"),
              keyStorePath       = Some("source-keystore.jks"),
              keyStoreType       = None,
              protocol           = None,
              trustStorePassword = Some("SRC_CQL_TRUSTSTORE_PASS_123"),
              trustStorePath     = Some("source-truststore.jks"),
              trustStoreType     = None
            )
          ),
          keyspace           = "ks",
          table              = "tbl",
          splitCount         = Some(16),
          connections        = Some(4),
          fetchSize          = 1000,
          preserveTimestamps = false,
          where              = Some("email = 'cql-user@example.com'"),
          consistencyLevel   = "LOCAL_QUORUM"
        ),
        target = TargetSettings.Scylla(
          host        = "scylla",
          port        = 9042,
          localDC     = Some("dc1"),
          credentials = Some(Credentials("target-user", "TGT_SCYLLA_PASS_123")),
          sslOptions = Some(
            SSLOptions(
              clientAuthEnabled  = false,
              enabled            = true,
              enabledAlgorithms  = None,
              keyStorePassword   = Some("TGT_SCYLLA_KEYSTORE_PASS_123"),
              keyStorePath       = Some("target-keystore.jks"),
              keyStoreType       = None,
              protocol           = None,
              trustStorePassword = Some("TGT_SCYLLA_TRUSTSTORE_PASS_123"),
              trustStorePath     = Some("target-truststore.jks"),
              trustStoreType     = None
            )
          ),
          keyspace                      = "ks",
          table                         = "tbl",
          connections                   = Some(4),
          stripTrailingZerosForDecimals = false,
          writeTTLInS                   = None,
          writeWritetimestampInuS       = None,
          consistencyLevel              = "LOCAL_QUORUM"
        ),
        renames          = None,
        savepoints       = Savepoints(300, "/tmp/savepoints"),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      ) -> Seq(
        "SRC_CQL_PASS_123",
        "SRC_CQL_KEYSTORE_PASS_123",
        "SRC_CQL_TRUSTSTORE_PASS_123",
        "TGT_SCYLLA_PASS_123",
        "TGT_SCYLLA_KEYSTORE_PASS_123",
        "TGT_SCYLLA_TRUSTSTORE_PASS_123",
        "cql-user@example.com"
      ),
      MigratorConfig(
        source = SourceSettings.Parquet(
          path = "s3a://bucket/path",
          credentials =
            Some(ConfigAWSCredentials("PARQUET_ACCESS_123", "PARQUET_SECRET_123", None)),
          endpoint = Some(DynamoDBEndpoint("s3", 9000)),
          region   = Some("us-east-1")
        ),
        target           = TargetSettings.Parquet(path = "s3a://target/path"),
        renames          = None,
        savepoints       = Savepoints(300, "/tmp/savepoints"),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      ) -> Seq("PARQUET_ACCESS_123", "PARQUET_SECRET_123"),
      MigratorConfig(
        source = SourceSettings.DynamoDB(
          endpoint = Some(DynamoDBEndpoint("dynamodb", 8000)),
          region   = Some("us-east-1"),
          credentials =
            Some(ConfigAWSCredentials("DDB_SRC_ACCESS_123", "DDB_SRC_SECRET_123", None)),
          table                 = "src",
          scanSegments          = Some(4),
          readThroughput        = Some(100),
          throughputReadPercent = Some(0.5f),
          maxMapTasks           = Some(4)
        ),
        target = TargetSettings.DynamoDB(
          endpoint = Some(DynamoDBEndpoint("dynamodb-target", 8000)),
          region   = Some("us-east-1"),
          credentials =
            Some(ConfigAWSCredentials("DDB_TGT_ACCESS_123", "DDB_TGT_SECRET_123", None)),
          table                       = "dst",
          writeThroughput             = Some(100),
          throughputWritePercent      = Some(0.5f),
          streamChanges               = StreamChangesSetting.Disabled,
          skipInitialSnapshotTransfer = None
        ),
        renames          = None,
        savepoints       = Savepoints(300, "/tmp/savepoints"),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      ) -> Seq(
        "DDB_SRC_ACCESS_123",
        "DDB_SRC_SECRET_123",
        "DDB_TGT_ACCESS_123",
        "DDB_TGT_SECRET_123"
      ),
      MigratorConfig(
        source = SourceSettings.Alternator(
          alternatorEndpoint = DynamoDBEndpoint("http://alternator-source", 8000),
          region             = Some("us-east-1"),
          credentials =
            Some(ConfigAWSCredentials("ALT_SRC_ACCESS_123", "ALT_SRC_SECRET_123", None)),
          table                  = "src",
          scanSegments           = Some(4),
          readThroughput         = Some(100),
          throughputReadPercent  = Some(0.5f),
          maxMapTasks            = Some(4),
          removeConsumedCapacity = true,
          alternatorConfig       = AlternatorSettings(datacenter = Some("dc1"))
        ),
        target = TargetSettings.Alternator(
          alternatorEndpoint = DynamoDBEndpoint("http://alternator-target", 8000),
          region             = Some("us-east-1"),
          credentials =
            Some(ConfigAWSCredentials("ALT_TGT_ACCESS_123", "ALT_TGT_SECRET_123", None)),
          table                       = "dst",
          writeThroughput             = Some(100),
          throughputWritePercent      = Some(0.5f),
          streamChanges               = StreamChangesSetting.Disabled,
          skipInitialSnapshotTransfer = None,
          removeConsumedCapacity      = true,
          billingMode                 = None,
          alternatorConfig            = AlternatorSettings(datacenter = Some("dc1"))
        ),
        renames          = None,
        savepoints       = Savepoints(300, "/tmp/savepoints"),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      ) -> Seq(
        "ALT_SRC_ACCESS_123",
        "ALT_SRC_SECRET_123",
        "ALT_TGT_ACCESS_123",
        "ALT_TGT_SECRET_123"
      ),
      MigratorConfig(
        source = SourceSettings.DynamoDBS3Export(
          bucket           = "bucket",
          manifestKey      = "manifest.json",
          tableDescription = s3ExportDescription,
          endpoint         = Some(DynamoDBEndpoint("s3", 9000)),
          region           = Some("us-east-1"),
          credentials =
            Some(ConfigAWSCredentials("S3EXPORT_SRC_ACCESS_123", "S3EXPORT_SRC_SECRET_123", None)),
          usePathStyleAccess = Some(true)
        ),
        target           = TargetSettings.DynamoDBS3Export(path = "s3a://bucket/export"),
        renames          = None,
        savepoints       = Savepoints(300, "/tmp/savepoints"),
        skipTokenRanges  = None,
        skipSegments     = None,
        skipParquetFiles = None,
        validation       = None
      ) -> Seq("S3EXPORT_SRC_ACCESS_123", "S3EXPORT_SRC_SECRET_123")
    )

    configsAndSecrets.foreach { case (config, secrets) =>
      val rendered = config.renderRedacted
      assert(rendered.contains("<redacted>"), "expected redaction marker in rendered config")
      assertSecretsAbsent(rendered, secrets)
    }
  }
}
