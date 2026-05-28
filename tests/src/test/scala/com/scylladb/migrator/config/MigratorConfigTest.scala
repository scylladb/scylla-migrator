package com.scylladb.migrator.config

import com.datastax.spark.connector.rdd.partitioner.dht.{ BigIntToken, LongToken }
import com.scylladb.migrator.SavepointStore
import io.circe.yaml

class MigratorConfigTest extends munit.FunSuite {
  private def parseConfig(config: String): MigratorConfig =
    yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])
      .fold(error => fail(s"Expected config to parse, got: ${error}"), identity)

  private def parseConfigFailure(config: String): Throwable =
    yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])
      .fold(identity, parsed => fail(s"Expected config parsing to fail, got: ${parsed.render}"))

  private val parquetSource: String =
    """source:
      |  type: parquet
      |  path: s3a://source-bucket/input
      |""".stripMargin

  private val scyllaTarget: String =
    """target:
      |  type: scylla
      |  host: localhost
      |  port: 9042
      |  keyspace: ks
      |  table: tbl
      |  stripTrailingZerosForDecimals: false
      |  consistencyLevel: LOCAL_QUORUM
      |""".stripMargin

  private val parquetTarget: String =
    """target:
      |  type: parquet
      |  path: /tmp/output
      |""".stripMargin

  private def parquetConfig(target: String, savepoints: String): String =
    s"""${parquetSource}${target}${savepoints}"""

  test("existing file savepoints config decodes as filesystem target") {
    val config =
      """source:
        |  type: parquet
        |  path: s3a://bucket/data
        |target:
        |  type: scylla
        |  host: scylla.example.com
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val savepoints = result.toOption.get.savepoints
    assertEquals(savepoints.resolvedTarget, SavepointTarget.Filesystem("/tmp/savepoints"))
    assertEquals(savepoints.path, "/tmp/savepoints")
    assertEquals(savepoints.intervalSeconds, 300)
    assertEquals(savepoints.resumeFromLatest, false)
  }

  test("filesystem savepoints target decodes") {
    val config =
      """source:
        |  type: parquet
        |  path: s3a://bucket/data
        |target:
        |  type: scylla
        |  host: scylla.example.com
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  intervalSeconds: 60
        |  target:
        |    type: filesystem
        |    path: /nested/savepoints
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val savepoints = result.toOption.get.savepoints
    assertEquals(savepoints.resolvedTarget, SavepointTarget.Filesystem("/nested/savepoints"))
    assertEquals(savepoints.filesystemPath, "/nested/savepoints")
    assertEquals(savepoints.intervalSeconds, 60)
  }

  test("filesystem savepoints target inherits top-level path when target path is omitted") {
    val config =
      """source:
        |  type: parquet
        |  path: s3a://bucket/data
        |target:
        |  type: scylla
        |  host: scylla.example.com
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  path: /custom/savepoints
        |  intervalSeconds: 60
        |  target:
        |    type: filesystem
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val savepoints = result.toOption.get.savepoints
    assertEquals(savepoints.resolvedTarget, SavepointTarget.Filesystem("/custom/savepoints"))
    assertEquals(savepoints.filesystemPath, "/custom/savepoints")
  }

  test("target-table savepoints config decodes with defaults") {
    val config =
      """source:
        |  type: parquet
        |  path: s3a://bucket/data
        |target:
        |  type: scylla
        |  host: scylla.example.com
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  intervalSeconds: 60
        |  target:
        |    type: target-table
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val savepoints = result.toOption.get.savepoints
    assertEquals(savepoints.path, Savepoints.Default.path)
    assertEquals(
      savepoints.resolvedTarget,
      SavepointTarget.TargetTable(
        keyspace = None,
        table    = "scylla_migrator_savepoints",
        jobId    = None
      )
    )
  }

  test("target-table savepoints config decodes explicit keyspace and table") {
    val config =
      """source:
        |  type: parquet
        |  path: s3a://bucket/data
        |target:
        |  type: scylla
        |  host: scylla.example.com
        |  port: 9042
        |  keyspace: ks
        |  table: tbl
        |  stripTrailingZerosForDecimals: false
        |  consistencyLevel: LOCAL_QUORUM
        |savepoints:
        |  intervalSeconds: 300
        |  target:
        |    type: target-table
        |    keyspace: my-keyspace
        |    table: my-table
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    assertEquals(
      result.toOption.get.savepoints.resolvedTarget,
      SavepointTarget.TargetTable(
        keyspace = Some("my-keyspace"),
        table    = "my-table",
        jobId    = None
      )
    )
  }

  test("target-table savepoints config rejects non-Scylla targets") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: dynamodb
        |  table: Dst
        |  streamChanges: false
        |savepoints:
        |  intervalSeconds: 300
        |  target:
        |    type: target-table
        |""".stripMargin

    val result = yaml.parser.parse(config).flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("target-table")),
      s"Expected Scylla-only error, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("derived target savepoint job id ignores progress fields and tracks source/target identity") {
    val base = parquetToScyllaConfig(
      sourcePath  = "s3a://bucket/source-a",
      targetTable = "table_a"
    )
    val withProgress = base.copy(skipParquetFiles = Some(Set("file-a.parquet")))
    val differentTarget = parquetToScyllaConfig(
      sourcePath  = "s3a://bucket/source-a",
      targetTable = "table_b"
    )
    val differentSource = parquetToScyllaConfig(
      sourcePath  = "s3a://bucket/source-b",
      targetTable = "table_a"
    )

    val jobId = SavepointStore.targetJobId(base)

    assert(jobId.startsWith("auto-"), s"Expected auto-derived job id, got: ${jobId}")
    assertEquals(jobId.length, "auto-".length + 32)
    assertEquals(SavepointStore.targetJobId(withProgress), jobId)
    assertNotEquals(SavepointStore.targetJobId(differentTarget), jobId)
    assertNotEquals(SavepointStore.targetJobId(differentSource), jobId)
  }

  test("full MigratorConfig with Alternator types round-trips through YAML") {
    val config = MigratorConfig(
      source = SourceSettings.Alternator(
        alternatorEndpoint     = DynamoDBEndpoint("http://10.0.0.1", 8000),
        region                 = Some("us-east-1"),
        credentials            = None,
        table                  = "SrcTable",
        scanSegments           = Some(4),
        readThroughput         = Some(100),
        throughputReadPercent  = Some(0.5f),
        maxMapTasks            = Some(2),
        removeConsumedCapacity = true,
        alternatorConfig = AlternatorSettings(
          datacenter              = Some("dc1"),
          rack                    = Some("rack1"),
          activeRefreshIntervalMs = Some(5000L)
        )
      ),
      target = TargetSettings.Alternator(
        alternatorEndpoint          = DynamoDBEndpoint("http://10.0.0.2", 8000),
        region                      = None,
        credentials                 = None,
        table                       = "DstTable",
        writeThroughput             = Some(200),
        throughputWritePercent      = Some(0.8f),
        streamChanges               = false,
        skipInitialSnapshotTransfer = None,
        removeConsumedCapacity      = true,
        billingMode                 = None,
        alternatorConfig            = AlternatorSettings(datacenter = Some("dc2"))
      ),
      renames          = Some(List(Rename("oldCol", "newCol"))),
      savepoints       = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.source, config.source)
    assertEquals(roundTripped.target, config.target)
    assertEquals(roundTripped.renames, config.renames)
    assertEquals(roundTripped.savepoints, config.savepoints)
  }

  test("savepoints path preserves Hadoop URI storage path") {
    val config = parseConfig(
      parquetConfig(
        scyllaTarget,
        """savepoints:
          |  intervalSeconds: 300
          |  path: gs://savepoint-bucket/jobs/with spaces
          |""".stripMargin
      )
    )

    assertEquals(config.savepoints.path, "gs://savepoint-bucket/jobs/with spaces")
    assertEquals(config.savepoints.storagePath, "gs://savepoint-bucket/jobs/with spaces")
    assertEquals(
      config.savepoints.effectiveTarget,
      SavepointsTarget.Filesystem("gs://savepoint-bucket/jobs/with spaces")
    )
  }

  Seq(
    "gs://savepoint-bucket/jobs",
    "gcp://savepoint-bucket/jobs",
    "s3://savepoint-bucket/jobs",
    "s3a://savepoint-bucket/jobs"
  )
    .foreach { remotePath =>
      test(s"savepoints target filesystem rejects Hadoop URI storage path ${remotePath}") {
        val error = parseConfigFailure(
          parquetConfig(
            scyllaTarget,
            s"""savepoints:
               |  intervalSeconds: 300
               |  target:
               |    type: filesystem
               |    path: ${remotePath}
               |""".stripMargin
          )
        )

        assert(
          error.getMessage.contains("filesystem") &&
            error.getMessage.contains("local filesystem paths"),
          s"Expected filesystem URI validation error, got: ${error.getMessage}"
        )
      }
    }

  test("savepoints target s3 builds a Hadoop s3a storage path") {
    val config = parseConfig(
      parquetConfig(
        scyllaTarget,
        """savepoints:
          |  intervalSeconds: 120
          |  target:
          |    type: s3
          |    bucket: savepoint-bucket
          |    prefix: scylla-migrator/job-a
          |""".stripMargin
      )
    )

    assertEquals(config.savepoints.path, "s3a://savepoint-bucket/scylla-migrator/job-a")
    assertEquals(config.savepoints.storagePath, "s3a://savepoint-bucket/scylla-migrator/job-a")
    assertEquals(
      config.savepoints.effectiveTarget,
      SavepointsTarget.S3("savepoint-bucket", Some("scylla-migrator/job-a"))
    )
  }

  test("savepoints target gcs builds a Hadoop gs storage path") {
    val config = parseConfig(
      parquetConfig(
        scyllaTarget,
        """savepoints:
          |  intervalSeconds: 120
          |  target:
          |    type: gcs
          |    bucket: savepoint-bucket
          |    prefix: scylla-migrator/job-a
          |    projectId: migrator-project
          |    credentials:
          |      serviceAccountJsonKeyfile: /etc/gcp/key.json
          |""".stripMargin
      )
    )

    assertEquals(config.savepoints.path, "gs://savepoint-bucket/scylla-migrator/job-a")
    assertEquals(config.savepoints.storagePath, "gs://savepoint-bucket/scylla-migrator/job-a")
    assertEquals(
      config.savepoints.effectiveTarget,
      SavepointsTarget.GCS(
        "savepoint-bucket",
        Some("scylla-migrator/job-a"),
        Some("migrator-project"),
        Some(SavepointsTarget.GCSCredentials("/etc/gcp/key.json"))
      )
    )
  }

  test("savepoints target table is rejected until a table-backed writer exists") {
    val error = parseConfigFailure(
      parquetConfig(
        scyllaTarget,
        """savepoints:
          |  intervalSeconds: 300
          |  target:
          |    type: target-table
          |    keyspace: ops
          |    table: migrator_savepoints
          |""".stripMargin
      )
    )

    assert(
      error.getMessage.contains("target-table") &&
        error.getMessage.contains("not supported"),
      s"Expected unsupported target-table validation error, got: ${error.getMessage}"
    )
  }

  test("savepoints target table is rejected before migration-target validation") {
    val error = parseConfigFailure(
      parquetConfig(
        parquetTarget,
        """savepoints:
          |  intervalSeconds: 300
          |  target:
          |    type: target-table
          |    table: migrator_savepoints
          |""".stripMargin
      )
    )

    assert(
      error.getMessage.contains("target-table") &&
        error.getMessage.contains("not supported"),
      s"Expected unsupported target-table validation error, got: ${error.getMessage}"
    )
  }

  test("savepoints path and target are mutually exclusive") {
    val error = parseConfigFailure(
      parquetConfig(
        scyllaTarget,
        """savepoints:
          |  intervalSeconds: 300
          |  path: /tmp/savepoints
          |  target:
          |    type: filesystem
          |    path: /tmp/target-savepoints
          |""".stripMargin
      )
    )

    assert(
      error.getMessage.contains("only one of savepoints.path or savepoints.target"),
      s"Expected path/target validation error, got: ${error.getMessage}"
    )
  }

  test("alternator source with streamChanges true is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: true
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with alternator target and streamChanges true is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.2
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb-s3-export source with streamChanges true is rejected") {
    val config =
      """source:
        |  type: dynamodb-s3-export
        |  bucket: foobar
        |  manifestKey: manifest.json
        |  tableDescription:
        |    attributeDefinitions:
        |      - name: id
        |        type: S
        |    keySchema:
        |      - name: id
        |        type: HASH
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("streamChanges")),
      s"Expected error about streamChanges, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("alternator source with streamChanges false is accepted") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
  }

  test("dynamodb source to alternator target parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
  }

  test("alternator source to alternator target parses successfully") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.2
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.Alternator])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
  }

  test("alternator source to dynamodb-s3-export target is rejected") {
    val config =
      """source:
        |  type: alternator
        |  table: Src
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |target:
        |  type: dynamodb-s3-export
        |  path: /tmp/export
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isLeft, s"Expected a decoding failure but got: ${result}")
    assert(
      result.left.exists(_.getMessage.contains("dynamodb-s3-export")),
      s"Expected error about unsupported alternator -> dynamodb-s3-export, got: ${result.left.map(_.getMessage)}"
    )
  }

  test("dynamodb source to alternator target with streaming parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: true
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.Alternator])
    assert(cfg.target.asInstanceOf[TargetSettings.Alternator].streamChanges)
  }

  test("dynamodb-s3-export source with streamChanges false is accepted") {
    val config =
      """source:
        |  type: dynamodb-s3-export
        |  bucket: foobar
        |  manifestKey: manifest.json
        |  tableDescription:
        |    attributeDefinitions:
        |      - name: id
        |        type: S
        |    keySchema:
        |      - name: id
        |        type: HASH
        |target:
        |  type: alternator
        |  table: Dest
        |  streamChanges: false
        |  endpoint:
        |    host: http://10.0.0.1
        |    port: 8000
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
  }

  test("dynamodb source to dynamodb target parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: false
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.DynamoDB])
  }

  test("dynamodb source to dynamodb target with streaming parses successfully") {
    val config =
      """source:
        |  type: dynamodb
        |  table: Src
        |target:
        |  type: dynamodb
        |  table: Dest
        |  streamChanges: true
        |savepoints:
        |  path: /tmp/savepoints
        |  intervalSeconds: 300
        |""".stripMargin

    val result = yaml.parser
      .parse(config)
      .flatMap(_.as[MigratorConfig])

    assert(result.isRight, s"Expected valid config but got: ${result}")
    val cfg = result.toOption.get
    assert(cfg.source.isInstanceOf[SourceSettings.DynamoDB])
    assert(cfg.target.isInstanceOf[TargetSettings.DynamoDB])
    assert(cfg.target.asInstanceOf[TargetSettings.DynamoDB].streamChanges)
  }

  test("MySQL config round-trips without savepoints block (supportsSavepoints = false)") {
    val config = MigratorConfig(
      source = SourceSettings.MySQL(
        host                 = "mysql.example.com",
        port                 = 3306,
        database             = "testdb",
        table                = "t",
        credentials          = Credentials("user", "pass"),
        primaryKey           = None,
        partitionColumn      = None,
        numPartitions        = None,
        lowerBound           = None,
        upperBound           = None,
        zeroDateTimeBehavior = SourceSettings.MySQL.ZeroDateTimeBehavior.Exception,
        fetchSize            = SourceSettings.MySQL.DefaultFetchSize,
        where                = None,
        connectionProperties = None
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "t",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    assert(!rendered.contains("savepoints"), "Encoder should omit savepoints for MySQL")
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.source, config.source)
    assertEquals(roundTripped.target, config.target)
  }

  test(
    "DynamoDBS3Export config round-trips without savepoints block (supportsSavepoints = false)"
  ) {
    val config = MigratorConfig(
      source = SourceSettings.DynamoDBS3Export(
        bucket      = "exports",
        manifestKey = "manifest.json",
        tableDescription = SourceSettings.DynamoDBS3Export.TableDescription(
          attributeDefinitions = Seq(
            SourceSettings.DynamoDBS3Export
              .AttributeDefinition("pk", SourceSettings.DynamoDBS3Export.AttributeType.S)
          ),
          keySchema = Seq(
            SourceSettings.DynamoDBS3Export
              .KeySchema("pk", SourceSettings.DynamoDBS3Export.KeyType.Hash)
          )
        ),
        endpoint           = None,
        region             = Some("us-east-1"),
        credentials        = None,
        usePathStyleAccess = None
      ),
      target = TargetSettings.DynamoDB(
        endpoint                    = None,
        region                      = None,
        credentials                 = None,
        table                       = "DstTable",
        writeThroughput             = None,
        throughputWritePercent      = None,
        streamChanges               = false,
        skipInitialSnapshotTransfer = None
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    assert(!rendered.contains("savepoints"), "Encoder should omit savepoints for DynamoDBS3Export")
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.source, config.source)
    assertEquals(roundTripped.target, config.target)
  }

  test("MigratorConfig roundtrip with skipTokenRanges containing LongToken and BigIntToken") {
    val config = MigratorConfig(
      source = SourceSettings.DynamoDB(
        endpoint              = None,
        region                = Some("us-east-1"),
        credentials           = None,
        table                 = "SrcTable",
        scanSegments          = None,
        readThroughput        = None,
        throughputReadPercent = None,
        maxMapTasks           = None
      ),
      target = TargetSettings.DynamoDB(
        endpoint                    = None,
        region                      = None,
        credentials                 = None,
        table                       = "DstTable",
        writeThroughput             = None,
        throughputWritePercent      = None,
        streamChanges               = false,
        skipInitialSnapshotTransfer = None
      ),
      renames    = None,
      savepoints = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints"),
      skipTokenRanges = Some(
        Set(
          (LongToken(0L), LongToken(100L)),
          (
            BigIntToken(BigInt("123456789012345678901234567890")),
            BigIntToken(BigInt("987654321098765432109876543210"))
          )
        )
      ),
      skipSegments     = Some(Set(1, 3, 5)),
      skipParquetFiles = None,
      validation       = None
    )
    val rendered = config.render
    val parsed = yaml.parser.parse(rendered).flatMap(_.as[MigratorConfig])
    assert(parsed.isRight, s"Round-trip failed: ${parsed}")
    val roundTripped = parsed.toOption.get
    assertEquals(roundTripped.skipTokenRanges, config.skipTokenRanges)
    assertEquals(roundTripped.skipSegments, config.skipSegments)
  }

  private def parquetToScyllaConfig(
    sourcePath: String,
    targetTable: String
  ): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = sourcePath,
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = targetTable,
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames = None,
      savepoints = Savepoints(
        intervalSeconds = 300,
        target          = Some(SavepointTarget.TargetTable())
      ),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )
}
